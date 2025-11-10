use godot::global::PropertyUsageFlags;
use godot::prelude::*;
use half::f16;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

#[derive(GodotClass)]
/// Streaming helper around `PackedByteArray` for binary serialization from Godot.
///
/// The stream keeps data in a growable `Vec<u8>` and tracks a read/write cursor.
/// Godot callers can push primitive values, seek, and fetch the accumulated bytes
/// as a `PackedByteArray`, or load existing bytes and iterate through them.
#[class(init)]
pub struct BinaryStream {
    base: Base<RefCounted>,
    buffer: Vec<u8>,
    cursor: usize,
    last_error: String,
}

// A helper struct to hold processed property information.
// We derive Ord to enable sorting by name.
#[derive(Debug, Clone, Eq, PartialEq)]
struct StorableProperty {
    name: GString,
    type_: VariantType,
}

// Manual implementation of Ord to sort by the string representation of the name.
impl Ord for StorableProperty {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.to_string().cmp(&other.name.to_string())
    }
}

impl PartialOrd for StorableProperty {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl BinaryStream {
    fn set_error(&mut self, msg: impl Into<String>) {
        let text = msg.into();
        self.last_error = text.clone();
        godot_error!("{}", text);
    }

    fn clear_error(&mut self) {
        self.last_error.clear();
    }

    fn write_raw(&mut self, caller: &str, bytes: &[u8]) -> bool {
        match self.cursor.checked_add(bytes.len()) {
            Some(end) => {
                if end > self.buffer.len() {
                    self.buffer.resize(end, 0);
                }
                self.buffer[self.cursor..end].copy_from_slice(bytes);
                self.cursor = end;
                self.clear_error();
                true
            }
            None => {
                self.set_error(format!("{caller} would overflow stream length"));
                false
            }
        }
    }

    fn write_fixed<const N: usize>(&mut self, caller: &str, bytes: [u8; N]) -> bool {
        self.write_raw(caller, &bytes)
    }

    fn read_array<const N: usize>(&mut self, caller: &str) -> Option<[u8; N]> {
        let len = N;
        match self.cursor.checked_add(len) {
            Some(end) if end <= self.buffer.len() => {
                let mut out = [0u8; N];
                out.copy_from_slice(&self.buffer[self.cursor..end]);
                self.cursor = end;
                self.clear_error();
                Some(out)
            }
            Some(_) => {
                let available = self.buffer.len().saturating_sub(self.cursor);
                self.set_error(format!(
                    "{caller} requires {len} bytes but only {available} remain"
                ));
                None
            }
            None => {
                self.set_error(format!("{caller} overflowed stream position"));
                None
            }
        }
    }

    fn read_vec(&mut self, len: usize, caller: &str) -> Option<Vec<u8>> {
        match self.cursor.checked_add(len) {
            Some(end) if end <= self.buffer.len() => {
                let out = self.buffer[self.cursor..end].to_vec();
                self.cursor = end;
                self.clear_error();
                Some(out)
            }
            Some(_) => {
                let available = self.buffer.len().saturating_sub(self.cursor);
                self.set_error(format!(
                    "{caller} requires {len} bytes but only {available} remain"
                ));
                None
            }
            None => {
                self.set_error(format!("{caller} overflowed stream position"));
                None
            }
        }
    }

    fn write_len_prefixed(&mut self, len: usize, caller: &str) -> bool {
        match u32::try_from(len) {
            Ok(value) => self.write_fixed(caller, value.to_le_bytes()),
            Err(_) => {
                self.set_error(format!(
                    "{caller} length {len} exceeds maximum storable (u32::MAX)"
                ));
                false
            }
        }
    }

    fn read_len_prefixed(&mut self, caller: &str) -> Option<usize> {
        self.read_array::<4>(caller)
            .map(|bytes| u32::from_le_bytes(bytes) as usize)
    }

    fn expect_non_negative_index(&mut self, value: i64, caller: &str) -> Option<usize> {
        if value < 0 {
            self.set_error(format!(
                "{caller} expects a non-negative value, got {value}"
            ));
            return None;
        }
        match usize::try_from(value) {
            Ok(v) => Some(v),
            Err(e) => {
                self.set_error(format!("{caller} could not convert {value}: {e}"));
                None
            }
        }
    }

    fn cursor_as_i64(&mut self, caller: &str) -> Option<i64> {
        match i64::try_from(self.cursor) {
            Ok(v) => Some(v),
            Err(e) => {
                self.set_error(format!("{caller} exceeds i64 range: {e}"));
                None
            }
        }
    }

    fn try_from_i64<T>(&mut self, value: i64, caller: &str) -> Option<T>
    where
        T: TryFrom<i64>,
        T::Error: Display,
    {
        match T::try_from(value) {
            Ok(v) => Some(v),
            Err(e) => {
                self.set_error(format!("{caller} out of range for value {value}: {e}"));
                None
            }
        }
    }

    fn write_f32_inner(&mut self, value: f32, caller: &str) -> bool {
        self.write_fixed(caller, value.to_le_bytes())
    }

    fn write_f64_inner(&mut self, value: f64, caller: &str) -> bool {
        self.write_fixed(caller, value.to_le_bytes())
    }

    fn write_f16_inner(&mut self, value: f16, caller: &str) -> bool {
        self.write_fixed(caller, value.to_le_bytes())
    }

    fn read_f32_inner(&mut self, caller: &str) -> Option<f32> {
        self.read_array::<4>(caller)
            .map(|bytes| f32::from_le_bytes(bytes))
    }

    fn read_f64_inner(&mut self, caller: &str) -> Option<f64> {
        self.read_array::<8>(caller)
            .map(|bytes| f64::from_le_bytes(bytes))
    }

    fn read_f16_inner(&mut self, caller: &str) -> Option<f16> {
        self.read_array::<2>(caller)
            .map(|bytes| f16::from_le_bytes(bytes))
    }

    fn read_u64_inner(&mut self, caller: &str) -> Option<u64> {
        self.read_array::<8>(caller)
            .map(|bytes| u64::from_le_bytes(bytes))
    }

    fn read_i64_inner(&mut self, caller: &str) -> Option<i64> {
        self.read_array::<8>(caller)
            .map(|bytes| i64::from_le_bytes(bytes))
    }

    fn read_vector2_inner(&mut self, caller: &str) -> Option<Vector2> {
        let x = self.read_f32_inner(&format!("{caller}.x"))?;
        let y = self.read_f32_inner(&format!("{caller}.y"))?;
        Some(Vector2 { x, y })
    }

    fn read_vector3_inner(&mut self, caller: &str) -> Option<Vector3> {
        let x = self.read_f32_inner(&format!("{caller}.x"))?;
        let y = self.read_f32_inner(&format!("{caller}.y"))?;
        let z = self.read_f32_inner(&format!("{caller}.z"))?;
        Some(Vector3 { x, y, z })
    }

    fn read_basis_inner(&mut self, caller: &str) -> Option<Basis> {
        let row_0 = self.read_vector3_inner(&format!("{caller}.row0"))?;
        let row_1 = self.read_vector3_inner(&format!("{caller}.row1"))?;
        let row_2 = self.read_vector3_inner(&format!("{caller}.row2"))?;
        Some(Basis {
            rows: [row_0, row_1, row_2],
        })
    }

    fn read_transform2d_inner(&mut self, caller: &str) -> Option<Transform2D> {
        let a = self.read_vector2_inner(&format!("{caller}.a"))?;
        let b = self.read_vector2_inner(&format!("{caller}.b"))?;
        let origin = self.read_vector2_inner(&format!("{caller}.origin"))?;
        Some(Transform2D { a, b, origin })
    }

    fn read_transform3d_inner(&mut self, caller: &str) -> Option<Transform3D> {
        let basis = self.read_basis_inner(&format!("{caller}.basis"))?;
        let origin = self.read_vector3_inner(&format!("{caller}.origin"))?;
        Some(Transform3D { basis, origin })
    }

    fn write_vector2_inner(&mut self, value: Vector2, caller: &str) -> bool {
        if !self.write_f32_inner(value.x, &format!("{caller}.x")) {
            return false;
        }
        self.write_f32_inner(value.y, &format!("{caller}.y"))
    }

    fn write_vector3_inner(&mut self, value: Vector3, caller: &str) -> bool {
        if !self.write_f32_inner(value.x, &format!("{caller}.x")) {
            return false;
        }
        if !self.write_f32_inner(value.y, &format!("{caller}.y")) {
            return false;
        }
        self.write_f32_inner(value.z, &format!("{caller}.z"))
    }

    fn write_basis_inner(&mut self, value: Basis, caller: &str) -> bool {
        if !self.write_vector3_inner(value.rows[0], &format!("{caller}.row0")) {
            return false;
        }
        if !self.write_vector3_inner(value.rows[1], &format!("{caller}.row1")) {
            return false;
        }
        self.write_vector3_inner(value.rows[2], &format!("{caller}.row2"))
    }

    fn write_transform2d_inner(&mut self, value: Transform2D, caller: &str) -> bool {
        if !self.write_vector2_inner(value.a, &format!("{caller}.a")) {
            return false;
        }
        if !self.write_vector2_inner(value.b, &format!("{caller}.b")) {
            return false;
        }
        self.write_vector2_inner(value.origin, &format!("{caller}.origin"))
    }

    fn write_transform3d_inner(&mut self, value: Transform3D, caller: &str) -> bool {
        if !self.write_basis_inner(value.basis, &format!("{caller}.basis")) {
            return false;
        }
        self.write_vector3_inner(value.origin, &format!("{caller}.origin"))
    }

    fn write_string_inner(&mut self, value: &str, caller: &str) -> bool {
        if !self.write_len_prefixed(value.len(), &format!("{caller}.len")) {
            return false;
        }
        self.write_raw(&format!("{caller}.data"), value.as_bytes())
    }

    fn read_string_inner(&mut self, caller: &str) -> Option<String> {
        let len = self.read_len_prefixed(&format!("{caller}.len"))?;
        let bytes = self.read_vec(len, &format!("{caller}.data"))?;
        match String::from_utf8(bytes) {
            Ok(s) => Some(s),
            Err(e) => {
                self.set_error(format!("{caller} contained invalid UTF-8: {e}"));
                None
            }
        }
    }

    fn read_vector2i_inner(&mut self, caller: &str) -> Option<Vector2i> {
        let x = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.x"))?);
        let y = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.y"))?);
        Some(Vector2i { x, y })
    }

    fn write_vector2i_inner(&mut self, value: Vector2i, caller: &str) -> bool {
        if !self.write_fixed(&format!("{caller}.x"), value.x.to_le_bytes()) {
            return false;
        }
        self.write_fixed(&format!("{caller}.y"), value.y.to_le_bytes())
    }

    fn read_rect2_inner(&mut self, caller: &str) -> Option<Rect2> {
        let position = self.read_vector2_inner(&format!("{caller}.position"))?;
        let size = self.read_vector2_inner(&format!("{caller}.size"))?;
        Some(Rect2 { position, size })
    }

    fn write_rect2_inner(&mut self, value: Rect2, caller: &str) -> bool {
        if !self.write_vector2_inner(value.position, &format!("{caller}.position")) {
            return false;
        }
        self.write_vector2_inner(value.size, &format!("{caller}.size"))
    }

    fn read_rect2i_inner(&mut self, caller: &str) -> Option<Rect2i> {
        let position = self.read_vector2i_inner(&format!("{caller}.position"))?;
        let size = self.read_vector2i_inner(&format!("{caller}.size"))?;
        Some(Rect2i { position, size })
    }

    fn write_rect2i_inner(&mut self, value: Rect2i, caller: &str) -> bool {
        if !self.write_vector2i_inner(value.position, &format!("{caller}.position")) {
            return false;
        }
        self.write_vector2i_inner(value.size, &format!("{caller}.size"))
    }

    fn read_vector3i_inner(&mut self, caller: &str) -> Option<Vector3i> {
        let x = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.x"))?);
        let y = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.y"))?);
        let z = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.z"))?);
        Some(Vector3i { x, y, z })
    }

    fn write_vector3i_inner(&mut self, value: Vector3i, caller: &str) -> bool {
        if !self.write_fixed(&format!("{caller}.x"), value.x.to_le_bytes()) {
            return false;
        }
        if !self.write_fixed(&format!("{caller}.y"), value.y.to_le_bytes()) {
            return false;
        }
        self.write_fixed(&format!("{caller}.z"), value.z.to_le_bytes())
    }

    fn read_vector4_inner(&mut self, caller: &str) -> Option<Vector4> {
        let x = self.read_f32_inner(&format!("{caller}.x"))?;
        let y = self.read_f32_inner(&format!("{caller}.y"))?;
        let z = self.read_f32_inner(&format!("{caller}.z"))?;
        let w = self.read_f32_inner(&format!("{caller}.w"))?;
        Some(Vector4 { x, y, z, w })
    }

    fn write_vector4_inner(&mut self, value: Vector4, caller: &str) -> bool {
        if !self.write_f32_inner(value.x, &format!("{caller}.x")) {
            return false;
        }
        if !self.write_f32_inner(value.y, &format!("{caller}.y")) {
            return false;
        }
        if !self.write_f32_inner(value.z, &format!("{caller}.z")) {
            return false;
        }
        self.write_f32_inner(value.w, &format!("{caller}.w"))
    }

    fn read_vector4i_inner(&mut self, caller: &str) -> Option<Vector4i> {
        let x = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.x"))?);
        let y = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.y"))?);
        let z = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.z"))?);
        let w = i32::from_le_bytes(self.read_array::<4>(&format!("{caller}.w"))?);
        Some(Vector4i { x, y, z, w })
    }

    fn write_vector4i_inner(&mut self, value: Vector4i, caller: &str) -> bool {
        if !self.write_fixed(&format!("{caller}.x"), value.x.to_le_bytes()) {
            return false;
        }
        if !self.write_fixed(&format!("{caller}.y"), value.y.to_le_bytes()) {
            return false;
        }
        if !self.write_fixed(&format!("{caller}.z"), value.z.to_le_bytes()) {
            return false;
        }
        self.write_fixed(&format!("{caller}.w"), value.w.to_le_bytes())
    }

    fn read_plane_inner(&mut self, caller: &str) -> Option<Plane> {
        let normal = self.read_vector3_inner(&format!("{caller}.normal"))?;
        let d = self.read_f32_inner(&format!("{caller}.d"))?;
        Some(Plane { normal, d })
    }

    fn write_plane_inner(&mut self, value: Plane, caller: &str) -> bool {
        if !self.write_vector3_inner(value.normal, &format!("{caller}.normal")) {
            return false;
        }
        self.write_f32_inner(value.d, &format!("{caller}.d"))
    }

    fn read_quaternion_inner(&mut self, caller: &str) -> Option<Quaternion> {
        let x = self.read_f32_inner(&format!("{caller}.x"))?;
        let y = self.read_f32_inner(&format!("{caller}.y"))?;
        let z = self.read_f32_inner(&format!("{caller}.z"))?;
        let w = self.read_f32_inner(&format!("{caller}.w"))?;
        Some(Quaternion { x, y, z, w })
    }

    fn write_quaternion_inner(&mut self, value: Quaternion, caller: &str) -> bool {
        if !self.write_f32_inner(value.x, &format!("{caller}.x")) {
            return false;
        }
        if !self.write_f32_inner(value.y, &format!("{caller}.y")) {
            return false;
        }
        if !self.write_f32_inner(value.z, &format!("{caller}.z")) {
            return false;
        }
        self.write_f32_inner(value.w, &format!("{caller}.w"))
    }

    fn read_aabb_inner(&mut self, caller: &str) -> Option<Aabb> {
        let position = self.read_vector3_inner(&format!("{caller}.position"))?;
        let size = self.read_vector3_inner(&format!("{caller}.size"))?;
        Some(Aabb { position, size })
    }

    fn write_aabb_inner(&mut self, value: Aabb, caller: &str) -> bool {
        if !self.write_vector3_inner(value.position, &format!("{caller}.position")) {
            return false;
        }
        self.write_vector3_inner(value.size, &format!("{caller}.size"))
    }

    fn read_projection_inner(&mut self, caller: &str) -> Option<Projection> {
        let mut cols = [Vector4::default(); 4];
        for (idx, column) in cols.iter_mut().enumerate() {
            *column = self.read_vector4_inner(&format!("{caller}.cols[{idx}]"))?;
        }
        Some(Projection { cols })
    }

    fn write_projection_inner(&mut self, value: Projection, caller: &str) -> bool {
        for (idx, column) in value.cols.iter().enumerate() {
            if !self.write_vector4_inner(*column, &format!("{caller}.cols[{idx}]")) {
                return false;
            }
        }
        true
    }

    fn read_color_inner(&mut self, caller: &str) -> Option<Color> {
        let r = self.read_f32_inner(&format!("{caller}.r"))?;
        let g = self.read_f32_inner(&format!("{caller}.g"))?;
        let b = self.read_f32_inner(&format!("{caller}.b"))?;
        let a = self.read_f32_inner(&format!("{caller}.a"))?;
        Some(Color { r, g, b, a })
    }

    fn write_color_inner(&mut self, value: Color, caller: &str) -> bool {
        if !self.write_f32_inner(value.r, &format!("{caller}.r")) {
            return false;
        }
        if !self.write_f32_inner(value.g, &format!("{caller}.g")) {
            return false;
        }
        if !self.write_f32_inner(value.b, &format!("{caller}.b")) {
            return false;
        }
        self.write_f32_inner(value.a, &format!("{caller}.a"))
    }

    fn write_packed_array_inner<T, F>(
        &mut self,
        data: &[T],
        caller: &str,
        mut write_elem: F,
    ) -> bool
    where
        T: Copy,
        F: FnMut(&mut Self, T, &str) -> bool,
    {
        if !self.write_len_prefixed(data.len(), &format!("{caller}.len")) {
            return false;
        }
        for (idx, &elem) in data.iter().enumerate() {
            if !write_elem(self, elem, &format!("{caller}[{idx}]")) {
                return false;
            }
        }
        true
    }

    fn read_packed_array_inner<T, F>(&mut self, caller: &str, mut read_elem: F) -> Option<Vec<T>>
    where
        F: FnMut(&mut Self, &str) -> Option<T>,
    {
        let len = self.read_len_prefixed(&format!("{caller}.len"))?;
        let mut data = Vec::with_capacity(len);
        for idx in 0..len {
            let value = read_elem(self, &format!("{caller}[{idx}]"))?;
            data.push(value);
        }
        Some(data)
    }

    /// Checks if a variant type is supported for serialization by `write_variant`.
    fn is_type_supported(&self, type_: VariantType) -> bool {
        matches!(
            type_,
            VariantType::BOOL
                | VariantType::INT
                | VariantType::FLOAT
                | VariantType::STRING
                | VariantType::VECTOR2
                | VariantType::VECTOR2I
                | VariantType::RECT2
                | VariantType::RECT2I
                | VariantType::VECTOR3
                | VariantType::VECTOR3I
                | VariantType::TRANSFORM2D
                | VariantType::VECTOR4
                | VariantType::VECTOR4I
                | VariantType::PLANE
                | VariantType::QUATERNION
                | VariantType::AABB
                | VariantType::BASIS
                | VariantType::TRANSFORM3D
                | VariantType::PROJECTION
                | VariantType::COLOR
                | VariantType::STRING_NAME
                | VariantType::NODE_PATH
                | VariantType::RID
                | VariantType::PACKED_BYTE_ARRAY
                | VariantType::PACKED_INT32_ARRAY
                | VariantType::PACKED_INT64_ARRAY
                | VariantType::PACKED_FLOAT32_ARRAY
                | VariantType::PACKED_FLOAT64_ARRAY
                | VariantType::PACKED_STRING_ARRAY
                | VariantType::PACKED_VECTOR2_ARRAY
                | VariantType::PACKED_VECTOR3_ARRAY
                | VariantType::PACKED_COLOR_ARRAY
                | VariantType::PACKED_VECTOR4_ARRAY
        )
    }

    /// Fetches, filters, and sorts the properties of an object that are marked for storage.
    fn get_storable_properties(
        &mut self,
        object: &Gd<Object>,
        caller: &str,
    ) -> Option<Vec<StorableProperty>> {
        let prop_list = object.get_property_list();
        let mut storable_props = Vec::new();

        for prop_dict in prop_list.iter_shared() {
            let Some(name_var) = prop_dict.get("name") else {
                self.set_error(format!("{caller}: property dictionary missing 'name'"));
                return None;
            };
            let Some(name) = name_var.try_to_relaxed::<GString>().ok() else {
                self.set_error(format!("{caller}: property 'name' was not a StringName"));
                return None;
            };

            let Some(type_var) = prop_dict.get("type") else {
                self.set_error(format!("{caller}: property '{name}' missing 'type'"));
                return None;
            };
            let Some(type_int) = type_var.try_to_relaxed::<i32>().ok() else {
                self.set_error(format!("{caller}: property '{name}' type was not an int"));
                return None;
            };
            let type_ = VariantType { ord: type_int };

            let Some(usage_var) = prop_dict.get("usage") else {
                self.set_error(format!("{caller}: property '{name}' missing 'usage'"));
                return None;
            };
            let Some(usage_int) = usage_var.try_to::<u64>().ok() else {
                self.set_error(format!("{caller}: property '{name}' usage was not an int"));
                return None;
            };
            let Some(usage) = PropertyUsageFlags::try_from_ord(usage_int) else {
                self.set_error(format!(
                    "{caller}: property '{name}' usage had invalid flags {usage_int}"
                ));
                return None;
            };

            if usage.is_set(PropertyUsageFlags::STORAGE) && self.is_type_supported(type_) {
                storable_props.push(StorableProperty { name, type_ });
            }
        }

        storable_props.sort(); // Sort alphabetically by property name
        Some(storable_props)
    }

    /// Computes a stable hash for a sorted list of storable properties.
    fn compute_property_hash(properties: &[StorableProperty]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for prop in properties {
            prop.name.to_string().hash(&mut hasher);
            (prop.type_).hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Reads a variant from the stream based on an expected type.
    fn read_variant_by_type(&mut self, type_: VariantType) -> Option<Variant> {
        let value = match type_ {
            VariantType::BOOL => self.read_bool().to_variant(),
            VariantType::INT => self.read_i64().to_variant(),
            VariantType::FLOAT => self.read_f64().to_variant(),
            VariantType::STRING => self.read_string().to_variant(),
            VariantType::VECTOR2 => self.read_vector2().to_variant(),
            VariantType::VECTOR2I => self.read_vector2i().to_variant(),
            VariantType::RECT2 => self.read_rect2().to_variant(),
            VariantType::RECT2I => self.read_rect2i().to_variant(),
            VariantType::VECTOR3 => self.read_vector3().to_variant(),
            VariantType::VECTOR3I => self.read_vector3i().to_variant(),
            VariantType::TRANSFORM2D => self.read_transform2d().to_variant(),
            VariantType::VECTOR4 => self.read_vector4().to_variant(),
            VariantType::VECTOR4I => self.read_vector4i().to_variant(),
            VariantType::PLANE => self.read_plane().to_variant(),
            VariantType::QUATERNION => self.read_quaternion().to_variant(),
            VariantType::AABB => self.read_aabb().to_variant(),
            VariantType::BASIS => self.read_basis().to_variant(),
            VariantType::TRANSFORM3D => self.read_transform3d().to_variant(),
            VariantType::PROJECTION => self.read_projection().to_variant(),
            VariantType::COLOR => self.read_color().to_variant(),
            VariantType::STRING_NAME => self.read_string_name().to_variant(),
            VariantType::NODE_PATH => self.read_node_path().to_variant(),
            VariantType::RID => self.read_rid().to_variant(),
            VariantType::PACKED_BYTE_ARRAY => self.read_packed_byte_array().to_variant(),
            VariantType::PACKED_INT32_ARRAY => self.read_packed_int32_array().to_variant(),
            VariantType::PACKED_INT64_ARRAY => self.read_packed_int64_array().to_variant(),
            VariantType::PACKED_FLOAT32_ARRAY => self.read_packed_float32_array().to_variant(),
            VariantType::PACKED_FLOAT64_ARRAY => self.read_packed_float64_array().to_variant(),
            VariantType::PACKED_STRING_ARRAY => self.read_packed_string_array().to_variant(),
            VariantType::PACKED_VECTOR2_ARRAY => self.read_packed_vector2_array().to_variant(),
            VariantType::PACKED_VECTOR3_ARRAY => self.read_packed_vector3_array().to_variant(),
            VariantType::PACKED_COLOR_ARRAY => self.read_packed_color_array().to_variant(),
            VariantType::PACKED_VECTOR4_ARRAY => self.read_packed_vector4_array().to_variant(),
            _ => {
                self.set_error(format!(
                    "read_variant_by_type: cannot read unsupported type '{:?}'",
                    type_
                ));
                return None;
            }
        };
        // Check if any read operations failed and set an error.
        if !self.last_error.is_empty() {
            None
        } else {
            Some(value)
        }
    }
}

#[godot_api]
impl BinaryStream {
    /// Clears all stored bytes and resets the cursor to the start.
    #[func]
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.cursor = 0;
        self.clear_error();
    }

    /// Replaces the current contents with `data` and rewinds the cursor.
    #[func]
    pub fn load_bytes(&mut self, data: PackedByteArray) {
        self.buffer = data.to_vec();
        self.cursor = 0;
        self.clear_error();
    }

    /// Appends the given bytes at the current cursor position.
    #[func]
    pub fn write_bytes(&mut self, data: PackedByteArray) -> bool {
        self.write_raw("write_bytes", data.as_slice())
    }

    /// Reads `count` bytes from the cursor and advances it; returns empty on failure.
    #[func]
    pub fn read_bytes(&mut self, count: i64) -> PackedByteArray {
        match self.expect_non_negative_index(count, "read_bytes.count") {
            Some(len) => self
                .read_vec(len, "read_bytes")
                .map(PackedByteArray::from)
                .unwrap_or_else(|| PackedByteArray::new()),
            None => PackedByteArray::new(),
        }
    }

    /// Returns a `PackedByteArray` copy of the current buffer contents.
    #[func]
    pub fn to_packed_byte_array(&self) -> PackedByteArray {
        PackedByteArray::from(self.buffer.clone())
    }

    /// Returns the total number of bytes currently stored.
    #[func]
    pub fn len(&self) -> i64 {
        self.buffer.len().try_into().unwrap_or(i64::MAX)
    }

    /// Returns `true` when the buffer holds no bytes.
    #[func]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the cursor position as a byte offset from the start.
    #[func]
    pub fn position(&mut self) -> i64 {
        self.cursor_as_i64("position").unwrap_or(0)
    }

    /// Moves the cursor to an absolute byte position.
    #[func]
    pub fn seek(&mut self, position: i64) -> bool {
        match self.expect_non_negative_index(position, "seek") {
            Some(pos) => {
                self.cursor = pos;
                self.clear_error();
                true
            }
            None => false,
        }
    }

    /// Adds `delta` to the cursor position, allowing negative seeks when valid.
    #[func]
    pub fn skip(&mut self, delta: i64) -> bool {
        let current = match self.cursor_as_i64("skip") {
            Some(v) => v,
            None => return false,
        };
        match current.checked_add(delta) {
            Some(next) => match self.expect_non_negative_index(next, "skip.next") {
                Some(pos) => {
                    self.cursor = pos;
                    self.clear_error();
                    true
                }
                None => false,
            },
            None => {
                self.set_error("skip would overflow position");
                false
            }
        }
    }

    /// Returns the number of unread bytes from the current cursor to the end.
    #[func]
    pub fn remaining(&mut self) -> i64 {
        match self.cursor_as_i64("remaining") {
            Some(pos) => {
                let len = self.len();
                len.saturating_sub(pos)
            }
            None => 0,
        }
    }

    /// Resets the cursor to zero without altering data.
    #[func]
    pub fn rewind(&mut self) {
        self.cursor = 0;
        self.clear_error();
    }

    /// Returns `true` when the cursor is at or beyond the buffer length.
    #[func]
    pub fn is_eof(&self) -> bool {
        self.cursor >= self.buffer.len()
    }

    /// Returns the last error message produced by stream operations.
    #[func]
    pub fn get_last_error(&self) -> GString {
        GString::from(self.last_error.as_str())
    }

    /// Clears the stored error string.
    #[func]
    pub fn clear_last_error(&mut self) {
        self.clear_error();
    }

    /// Writes an unsigned 8-bit integer.
    #[func]
    pub fn write_u8(&mut self, value: i64) -> bool {
        match self.try_from_i64::<u8>(value, "write_u8") {
            Some(v) => self.write_fixed("write_u8", [v]),
            None => false,
        }
    }

    /// Writes an unsigned 16-bit integer in little-endian order.
    #[func]
    pub fn write_u16(&mut self, value: i64) -> bool {
        match self.try_from_i64::<u16>(value, "write_u16") {
            Some(v) => self.write_fixed("write_u16", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes an unsigned 32-bit integer in little-endian order.
    #[func]
    pub fn write_u32(&mut self, value: i64) -> bool {
        match self.try_from_i64::<u32>(value, "write_u32") {
            Some(v) => self.write_fixed("write_u32", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes an unsigned 64-bit integer in little-endian order.
    #[func]
    pub fn write_u64(&mut self, value: i64) -> bool {
        match self.try_from_i64::<u64>(value, "write_u64") {
            Some(v) => self.write_fixed("write_u64", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes a signed 8-bit integer.
    #[func]
    pub fn write_i8(&mut self, value: i64) -> bool {
        match self.try_from_i64::<i8>(value, "write_i8") {
            Some(v) => self.write_fixed("write_i8", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes a signed 16-bit integer in little-endian order.
    #[func]
    pub fn write_i16(&mut self, value: i64) -> bool {
        match self.try_from_i64::<i16>(value, "write_i16") {
            Some(v) => self.write_fixed("write_i16", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes a signed 32-bit integer in little-endian order.
    #[func]
    pub fn write_i32(&mut self, value: i64) -> bool {
        match self.try_from_i64::<i32>(value, "write_i32") {
            Some(v) => self.write_fixed("write_i32", v.to_le_bytes()),
            None => false,
        }
    }

    /// Writes a signed 64-bit integer in little-endian order.
    #[func]
    pub fn write_i64(&mut self, value: i64) -> bool {
        self.write_fixed("write_i64", value.to_le_bytes())
    }

    /// Writes an `f32` value (little-endian).
    #[func]
    pub fn write_f32(&mut self, value: f64) -> bool {
        self.write_f32_inner(value as f32, "write_f32")
    }

    /// Writes an `f64` value (little-endian).
    #[func]
    pub fn write_f64(&mut self, value: f64) -> bool {
        self.write_f64_inner(value, "write_f64")
    }

    /// Writes a 16-bit floating point value, truncating from `f64`.
    #[func]
    pub fn write_half(&mut self, value: f64) -> bool {
        let f = f16::from_f32(value as f32);
        self.write_f16_inner(f, "write_half")
    }

    /// Writes a boolean as a single byte (`0` or `1`).
    #[func]
    pub fn write_bool(&mut self, value: bool) -> bool {
        self.write_fixed("write_bool", [u8::from(value)])
    }

    /// Writes a UTF-8 string prefixed with a 32-bit length (no padding).
    #[func]
    pub fn write_string(&mut self, value: GString) -> bool {
        let owned = value.to_string();
        self.write_string_inner(&owned, "write_string")
    }

    /// Writes a `StringName` using the UTF-8 length-prefixed format.
    #[func]
    pub fn write_string_name(&mut self, value: StringName) -> bool {
        let owned = value.to_string();
        self.write_string_inner(&owned, "write_string_name")
    }

    /// Writes a `NodePath` using the UTF-8 length-prefixed format.
    #[func]
    pub fn write_node_path(&mut self, value: NodePath) -> bool {
        let owned = value.to_string();
        self.write_string_inner(&owned, "write_node_path")
    }

    /// Writes an `RID` as an unsigned 64-bit integer.
    #[func]
    pub fn write_rid(&mut self, value: Rid) -> bool {
        let id = value.to_u64();
        self.write_u64(id as i64)
    }

    /// Reads an unsigned 8-bit integer and advances the cursor.
    #[func]
    pub fn read_u8(&mut self) -> i64 {
        self.read_array::<1>("read_u8")
            .map(|[b]| b as i64)
            .unwrap_or(0)
    }

    /// Reads an unsigned 16-bit integer and advances the cursor.
    #[func]
    pub fn read_u16(&mut self) -> i64 {
        self.read_array::<2>("read_u16")
            .map(|bytes| u16::from_le_bytes(bytes) as i64)
            .unwrap_or(0)
    }

    /// Reads an unsigned 32-bit integer and advances the cursor.
    #[func]
    pub fn read_u32(&mut self) -> i64 {
        self.read_array::<4>("read_u32")
            .map(|bytes| u32::from_le_bytes(bytes) as i64)
            .unwrap_or(0)
    }

    /// Reads an unsigned 64-bit integer and advances the cursor.
    #[func]
    pub fn read_u64(&mut self) -> i64 {
        match self.read_u64_inner("read_u64") {
            Some(v) if v <= i64::MAX as u64 => v as i64,
            Some(v) => {
                self.set_error(format!("read_u64 value {v} exceeds Godot int range"));
                0
            }
            None => 0,
        }
    }

    /// Reads a signed 8-bit integer and advances the cursor.
    #[func]
    pub fn read_i8(&mut self) -> i64 {
        self.read_array::<1>("read_i8")
            .map(|[b]| (b as i8) as i64)
            .unwrap_or(0)
    }

    /// Reads a signed 16-bit integer and advances the cursor.
    #[func]
    pub fn read_i16(&mut self) -> i64 {
        self.read_array::<2>("read_i16")
            .map(|bytes| i16::from_le_bytes(bytes) as i64)
            .unwrap_or(0)
    }

    /// Reads a signed 32-bit integer and advances the cursor.
    #[func]
    pub fn read_i32(&mut self) -> i64 {
        self.read_array::<4>("read_i32")
            .map(|bytes| i32::from_le_bytes(bytes) as i64)
            .unwrap_or(0)
    }

    /// Reads a signed 64-bit integer and advances the cursor.
    #[func]
    pub fn read_i64(&mut self) -> i64 {
        self.read_i64_inner("read_i64").unwrap_or(0)
    }

    /// Reads an `f32` value and advances the cursor.
    #[func]
    pub fn read_f32(&mut self) -> f64 {
        self.read_f32_inner("read_f32")
            .map(|v| v as f64)
            .unwrap_or(0.0)
    }

    /// Reads an `f64` value and advances the cursor.
    #[func]
    pub fn read_f64(&mut self) -> f64 {
        self.read_f64_inner("read_f64").unwrap_or(0.0)
    }

    /// Reads a 16-bit float and returns it as `f64`.
    #[func]
    pub fn read_half(&mut self) -> f64 {
        self.read_f16_inner("read_half")
            .map(|v| f32::from(v) as f64)
            .unwrap_or(0.0)
    }

    /// Reads a boolean from a single byte (`0` is false, non-zero is true).
    #[func]
    pub fn read_bool(&mut self) -> bool {
        self.read_array::<1>("read_bool")
            .map(|[b]| b != 0)
            .unwrap_or(false)
    }

    /// Reads a UTF-8 string that was stored with a 32-bit length prefix.
    #[func]
    pub fn read_string(&mut self) -> GString {
        match self.read_string_inner("read_string") {
            Some(s) => GString::from(s.as_str()),
            None => GString::new(),
        }
    }

    /// Reads a `StringName` from the UTF-8 length-prefixed format.
    #[func]
    pub fn read_string_name(&mut self) -> StringName {
        match self.read_string_inner("read_string_name") {
            Some(s) => StringName::from(s.as_str()),
            None => StringName::from(""),
        }
    }

    /// Reads a `NodePath` from the UTF-8 length-prefixed format.
    #[func]
    pub fn read_node_path(&mut self) -> NodePath {
        match self.read_string_inner("read_node_path") {
            Some(s) => NodePath::from(s.as_str()),
            None => NodePath::from(""),
        }
    }

    /// Reads an `RID` stored as an unsigned 64-bit integer.
    #[func]
    pub fn read_rid(&mut self) -> Rid {
        match self.read_u64_inner("read_rid") {
            Some(v) => Rid::new(v),
            None => Rid::new(0),
        }
    }

    /// Writes the two components of a `Vector2`.
    #[func]
    pub fn write_vector2(&mut self, value: Vector2) -> bool {
        self.write_vector2_inner(value, "write_vector2")
    }

    /// Writes the three components of a `Vector3`.
    #[func]
    pub fn write_vector3(&mut self, value: Vector3) -> bool {
        self.write_vector3_inner(value, "write_vector3")
    }

    /// Writes the two components of a `Vector2i`.
    #[func]
    pub fn write_vector2i(&mut self, value: Vector2i) -> bool {
        self.write_vector2i_inner(value, "write_vector2i")
    }

    /// Writes the three components of a `Vector3i`.
    #[func]
    pub fn write_vector3i(&mut self, value: Vector3i) -> bool {
        self.write_vector3i_inner(value, "write_vector3i")
    }

    /// Writes the four components of a `Vector4`.
    #[func]
    pub fn write_vector4(&mut self, value: Vector4) -> bool {
        self.write_vector4_inner(value, "write_vector4")
    }

    /// Writes the four components of a `Vector4i`.
    #[func]
    pub fn write_vector4i(&mut self, value: Vector4i) -> bool {
        self.write_vector4i_inner(value, "write_vector4i")
    }

    /// Writes the position and size of a `Rect2`.
    #[func]
    pub fn write_rect2(&mut self, value: Rect2) -> bool {
        self.write_rect2_inner(value, "write_rect2")
    }

    /// Writes the integer position and size of a `Rect2i`.
    #[func]
    pub fn write_rect2i(&mut self, value: Rect2i) -> bool {
        self.write_rect2i_inner(value, "write_rect2i")
    }

    /// Writes the basis and origin of a `Transform2D`.
    #[func]
    pub fn write_transform2d(&mut self, value: Transform2D) -> bool {
        self.write_transform2d_inner(value, "write_transform2d")
    }

    /// Writes the rows of a `Basis`.
    #[func]
    pub fn write_basis(&mut self, value: Basis) -> bool {
        self.write_basis_inner(value, "write_basis")
    }

    /// Writes the basis and origin of a `Transform3D`.
    #[func]
    pub fn write_transform3d(&mut self, value: Transform3D) -> bool {
        self.write_transform3d_inner(value, "write_transform3d")
    }

    /// Writes the components of a `Plane` (normal + distance).
    #[func]
    pub fn write_plane(&mut self, value: Plane) -> bool {
        self.write_plane_inner(value, "write_plane")
    }

    /// Writes the components of a `Quaternion`.
    #[func]
    pub fn write_quaternion(&mut self, value: Quaternion) -> bool {
        self.write_quaternion_inner(value, "write_quaternion")
    }

    /// Writes the position and size of an `Aabb`.
    #[func]
    pub fn write_aabb(&mut self, value: Aabb) -> bool {
        self.write_aabb_inner(value, "write_aabb")
    }

    /// Writes the four column vectors of a `Projection`.
    #[func]
    pub fn write_projection(&mut self, value: Projection) -> bool {
        self.write_projection_inner(value, "write_projection")
    }

    /// Writes the four components of a `Color` (r, g, b, a).
    #[func]
    pub fn write_color(&mut self, value: Color) -> bool {
        self.write_color_inner(value, "write_color")
    }

    /// Reads a `Vector2`; returns zero vector on failure.
    #[func]
    pub fn read_vector2(&mut self) -> Vector2 {
        self.read_vector2_inner("read_vector2")
            .unwrap_or(Vector2 { x: 0.0, y: 0.0 })
    }

    /// Reads a `Vector3`; returns zero vector on failure.
    #[func]
    pub fn read_vector3(&mut self) -> Vector3 {
        self.read_vector3_inner("read_vector3").unwrap_or(Vector3 {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        })
    }

    /// Reads a `Vector2i`; returns zero vector on failure.
    #[func]
    pub fn read_vector2i(&mut self) -> Vector2i {
        self.read_vector2i_inner("read_vector2i")
            .unwrap_or(Vector2i { x: 0, y: 0 })
    }

    /// Reads a `Vector3i`; returns zero vector on failure.
    #[func]
    pub fn read_vector3i(&mut self) -> Vector3i {
        self.read_vector3i_inner("read_vector3i")
            .unwrap_or(Vector3i { x: 0, y: 0, z: 0 })
    }

    /// Reads a `Vector4`; returns zero vector on failure.
    #[func]
    pub fn read_vector4(&mut self) -> Vector4 {
        self.read_vector4_inner("read_vector4").unwrap_or(Vector4 {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            w: 0.0,
        })
    }

    /// Reads a `Vector4i`; returns zero vector on failure.
    #[func]
    pub fn read_vector4i(&mut self) -> Vector4i {
        self.read_vector4i_inner("read_vector4i")
            .unwrap_or(Vector4i {
                x: 0,
                y: 0,
                z: 0,
                w: 0,
            })
    }

    /// Reads a `Rect2`; returns zero rectangle on failure.
    #[func]
    pub fn read_rect2(&mut self) -> Rect2 {
        self.read_rect2_inner("read_rect2").unwrap_or(Rect2 {
            position: Vector2 { x: 0.0, y: 0.0 },
            size: Vector2 { x: 0.0, y: 0.0 },
        })
    }

    /// Reads a `Rect2i`; returns zero rectangle on failure.
    #[func]
    pub fn read_rect2i(&mut self) -> Rect2i {
        self.read_rect2i_inner("read_rect2i").unwrap_or(Rect2i {
            position: Vector2i { x: 0, y: 0 },
            size: Vector2i { x: 0, y: 0 },
        })
    }

    /// Reads a `Transform2D`; returns identity on failure.
    #[func]
    pub fn read_transform2d(&mut self) -> Transform2D {
        self.read_transform2d_inner("read_transform2d")
            .unwrap_or(Transform2D {
                a: Vector2 { x: 1.0, y: 0.0 },
                b: Vector2 { x: 0.0, y: 1.0 },
                origin: Vector2 { x: 0.0, y: 0.0 },
            })
    }

    /// Reads a `Basis`; returns identity basis on failure.
    #[func]
    pub fn read_basis(&mut self) -> Basis {
        self.read_basis_inner("read_basis").unwrap_or(Basis {
            rows: [
                Vector3 {
                    x: 1.0,
                    y: 0.0,
                    z: 0.0,
                },
                Vector3 {
                    x: 0.0,
                    y: 1.0,
                    z: 0.0,
                },
                Vector3 {
                    x: 0.0,
                    y: 0.0,
                    z: 1.0,
                },
            ],
        })
    }

    /// Reads a `Transform3D`; returns identity on failure.
    #[func]
    pub fn read_transform3d(&mut self) -> Transform3D {
        self.read_transform3d_inner("read_transform3d")
            .unwrap_or(Transform3D {
                basis: Basis {
                    rows: [
                        Vector3 {
                            x: 1.0,
                            y: 0.0,
                            z: 0.0,
                        },
                        Vector3 {
                            x: 0.0,
                            y: 1.0,
                            z: 0.0,
                        },
                        Vector3 {
                            x: 0.0,
                            y: 0.0,
                            z: 1.0,
                        },
                    ],
                },
                origin: Vector3 {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            })
    }

    /// Reads a `Plane`; returns the XY plane on failure.
    #[func]
    pub fn read_plane(&mut self) -> Plane {
        self.read_plane_inner("read_plane").unwrap_or(Plane {
            normal: Vector3 {
                x: 0.0,
                y: 0.0,
                z: 1.0,
            },
            d: 0.0,
        })
    }

    /// Reads a `Quaternion`; returns identity on failure.
    #[func]
    pub fn read_quaternion(&mut self) -> Quaternion {
        self.read_quaternion_inner("read_quaternion")
            .unwrap_or(Quaternion {
                x: 0.0,
                y: 0.0,
                z: 0.0,
                w: 1.0,
            })
    }

    /// Reads an `Aabb`; returns zero box on failure.
    #[func]
    pub fn read_aabb(&mut self) -> Aabb {
        self.read_aabb_inner("read_aabb").unwrap_or(Aabb {
            position: Vector3 {
                x: 0.0,
                y: 0.0,
                z: 0.0,
            },
            size: Vector3 {
                x: 0.0,
                y: 0.0,
                z: 0.0,
            },
        })
    }

    /// Reads a `Projection`; returns identity on failure.
    #[func]
    pub fn read_projection(&mut self) -> Projection {
        self.read_projection_inner("read_projection")
            .unwrap_or(Projection {
                cols: [
                    Vector4 {
                        x: 1.0,
                        y: 0.0,
                        z: 0.0,
                        w: 0.0,
                    },
                    Vector4 {
                        x: 0.0,
                        y: 1.0,
                        z: 0.0,
                        w: 0.0,
                    },
                    Vector4 {
                        x: 0.0,
                        y: 0.0,
                        z: 1.0,
                        w: 0.0,
                    },
                    Vector4 {
                        x: 0.0,
                        y: 0.0,
                        z: 0.0,
                        w: 1.0,
                    },
                ],
            })
    }

    /// Reads a `Color`; returns transparent black on failure.
    #[func]
    pub fn read_color(&mut self) -> Color {
        self.read_color_inner("read_color").unwrap_or(Color {
            r: 0.0,
            g: 0.0,
            b: 0.0,
            a: 0.0,
        })
    }

    // Writes a length-prefixed `PackedByteArray` (count then raw bytes).
    #[func]
    pub fn write_packed_byte_array(&mut self, value: PackedByteArray) -> bool {
        let data = value.as_slice();
        match self.write_len_prefixed(data.len(), "write_packed_byte_array.len") {
            true => self.write_raw("write_packed_byte_array.data", data),
            false => false,
        }
    }

    /// Writes a length-prefixed `PackedByteArray` (count then raw bytes).
    #[func]
    pub fn read_packed_byte_array(&mut self) -> PackedByteArray {
        match self.read_len_prefixed("read_packed_byte_array.len") {
            Some(len) => self
                .read_vec(len, "read_packed_byte_array.data")
                .map(PackedByteArray::from)
                .unwrap_or_else(PackedByteArray::new),
            None => PackedByteArray::new(),
        }
    }

    /// Writes a length-prefixed `PackedInt32Array` using little-endian elements.
    #[func]
    pub fn write_packed_int32_array(&mut self, value: PackedInt32Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(&data, "write_packed_int32_array", |s, v, c| {
            s.write_fixed(c, v.to_le_bytes())
        })
    }

    /// Reads a length-prefixed `PackedInt32Array`.
    #[func]
    pub fn read_packed_int32_array(&mut self) -> PackedInt32Array {
        self.read_packed_array_inner("read_packed_int32_array", |s, c| {
            s.read_array::<4>(c).map(i32::from_le_bytes)
        })
        .map(PackedInt32Array::from)
        .unwrap_or_else(PackedInt32Array::new)
    }

    /// Writes a length-prefixed `PackedInt64Array` using little-endian elements.
    #[func]
    pub fn write_packed_int64_array(&mut self, value: PackedInt64Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(&data, "write_packed_int64_array", |s, v, c| {
            s.write_fixed(c, v.to_le_bytes())
        })
    }

    /// Reads a length-prefixed `PackedInt64Array`.
    #[func]
    pub fn read_packed_int64_array(&mut self) -> PackedInt64Array {
        self.read_packed_array_inner("read_packed_int64_array", |s, c| s.read_i64_inner(c))
            .map(PackedInt64Array::from)
            .unwrap_or_else(PackedInt64Array::new)
    }

    /// Writes a length-prefixed `PackedFloat32Array`.
    #[func]
    pub fn write_packed_float32_array(&mut self, value: PackedFloat32Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(&data, "write_packed_float32_array", Self::write_f32_inner)
    }

    /// Reads a length-prefixed `PackedFloat32Array`.
    #[func]
    pub fn read_packed_float32_array(&mut self) -> PackedFloat32Array {
        self.read_packed_array_inner("read_packed_float32_array", Self::read_f32_inner)
            .map(PackedFloat32Array::from)
            .unwrap_or_else(PackedFloat32Array::new)
    }

    /// Writes a length-prefixed `PackedFloat64Array`.
    #[func]
    pub fn write_packed_float64_array(&mut self, value: PackedFloat64Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(&data, "write_packed_float64_array", Self::write_f64_inner)
    }

    /// Reads a length-prefixed `PackedFloat64Array`.
    #[func]
    pub fn read_packed_float64_array(&mut self) -> PackedFloat64Array {
        self.read_packed_array_inner("read_packed_float64_array", Self::read_f64_inner)
            .map(PackedFloat64Array::from)
            .unwrap_or_else(PackedFloat64Array::new)
    }

    /// Writes a length-prefixed `PackedStringArray` (each entry UTF-8 length prefixed).
    #[func]
    pub fn write_packed_string_array(&mut self, value: PackedStringArray) -> bool {
        let data = value.to_vec();
        if !self.write_len_prefixed(data.len(), "write_packed_string_array.len") {
            return false;
        }
        for (idx, elem) in data.iter().enumerate() {
            let owned = elem.to_string();
            if !self.write_string_inner(&owned, &format!("write_packed_string_array[{idx}]")) {
                return false;
            }
        }
        true
    }

    /// Reads a length-prefixed `PackedStringArray`.
    #[func]
    pub fn read_packed_string_array(&mut self) -> PackedStringArray {
        self.read_packed_array_inner("read_packed_string_array", Self::read_string_inner)
            .map(|v| v.into_iter().map(|x| GString::from(x.as_str())).collect())
            .unwrap_or_else(PackedStringArray::new)
    }

    /// Writes a length-prefixed `PackedVector2Array`.
    #[func]
    pub fn write_packed_vector2_array(&mut self, value: PackedVector2Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(
            &data,
            "write_packed_vector2_array",
            Self::write_vector2_inner,
        )
    }

    /// Reads a length-prefixed `PackedVector2Array`.
    #[func]
    pub fn read_packed_vector2_array(&mut self) -> PackedVector2Array {
        self.read_packed_array_inner("read_packed_vector2_array", Self::read_vector2_inner)
            .map(PackedVector2Array::from)
            .unwrap_or_else(PackedVector2Array::new)
    }

    /// Writes a length-prefixed `PackedVector3Array`.
    #[func]
    pub fn write_packed_vector3_array(&mut self, value: PackedVector3Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(
            &data,
            "write_packed_vector3_array",
            Self::write_vector3_inner,
        )
    }

    /// Reads a length-prefixed `PackedVector3Array`.
    #[func]
    pub fn read_packed_vector3_array(&mut self) -> PackedVector3Array {
        self.read_packed_array_inner("read_packed_vector3_array", Self::read_vector3_inner)
            .map(PackedVector3Array::from)
            .unwrap_or_else(PackedVector3Array::new)
    }

    /// Writes a length-prefixed `PackedColorArray`.
    #[func]
    pub fn write_packed_color_array(&mut self, value: PackedColorArray) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(&data, "write_packed_color_array", Self::write_color_inner)
    }

    /// Reads a length-prefixed `PackedColorArray`.
    #[func]
    pub fn read_packed_color_array(&mut self) -> PackedColorArray {
        self.read_packed_array_inner("read_packed_color_array", Self::read_color_inner)
            .map(PackedColorArray::from)
            .unwrap_or_else(PackedColorArray::new)
    }

    /// Writes a length-prefixed `PackedVector4Array`.
    #[func]
    pub fn write_packed_vector4_array(&mut self, value: PackedVector4Array) -> bool {
        let data = value.to_vec();
        self.write_packed_array_inner(
            &data,
            "write_packed_vector4_array",
            Self::write_vector4_inner,
        )
    }

    /// Reads a length-prefixed `PackedVector4Array`.
    #[func]
    pub fn read_packed_vector4_array(&mut self) -> PackedVector4Array {
        self.read_packed_array_inner("read_packed_vector4_array", Self::read_vector4_inner)
            .map(PackedVector4Array::from)
            .unwrap_or_else(PackedVector4Array::new)
    }

    /// Writes a Godot `Variant` to the stream.
    ///
    /// This function checks the variant's type and calls the corresponding
    /// `write_*` method. If the type is not supported for serialization,
    /// it sets an error and returns `false`.
    #[func]
    pub fn write_variant(&mut self, value: Variant) -> bool {
        match value.get_type() {
            VariantType::BOOL => self.write_bool(value.to()),
            VariantType::INT => self.write_i64(value.to()),
            VariantType::FLOAT => self.write_f64(value.to()),
            VariantType::STRING => self.write_string(value.to()),
            VariantType::VECTOR2 => self.write_vector2(value.to()),
            VariantType::VECTOR2I => self.write_vector2i(value.to()),
            VariantType::RECT2 => self.write_rect2(value.to()),
            VariantType::RECT2I => self.write_rect2i(value.to()),
            VariantType::VECTOR3 => self.write_vector3(value.to()),
            VariantType::VECTOR3I => self.write_vector3i(value.to()),
            VariantType::TRANSFORM2D => self.write_transform2d(value.to()),
            VariantType::VECTOR4 => self.write_vector4(value.to()),
            VariantType::VECTOR4I => self.write_vector4i(value.to()),
            VariantType::PLANE => self.write_plane(value.to()),
            VariantType::QUATERNION => self.write_quaternion(value.to()),
            VariantType::AABB => self.write_aabb(value.to()),
            VariantType::BASIS => self.write_basis(value.to()),
            VariantType::TRANSFORM3D => self.write_transform3d(value.to()),
            VariantType::PROJECTION => self.write_projection(value.to()),
            VariantType::COLOR => self.write_color(value.to()),
            VariantType::STRING_NAME => self.write_string_name(value.to()),
            VariantType::NODE_PATH => self.write_node_path(value.to()),
            VariantType::RID => self.write_rid(value.to()),
            VariantType::PACKED_BYTE_ARRAY => self.write_packed_byte_array(value.to()),
            VariantType::PACKED_INT32_ARRAY => self.write_packed_int32_array(value.to()),
            VariantType::PACKED_INT64_ARRAY => self.write_packed_int64_array(value.to()),
            VariantType::PACKED_FLOAT32_ARRAY => self.write_packed_float32_array(value.to()),
            VariantType::PACKED_FLOAT64_ARRAY => self.write_packed_float64_array(value.to()),
            VariantType::PACKED_STRING_ARRAY => self.write_packed_string_array(value.to()),
            VariantType::PACKED_VECTOR2_ARRAY => self.write_packed_vector2_array(value.to()),
            VariantType::PACKED_VECTOR3_ARRAY => self.write_packed_vector3_array(value.to()),
            VariantType::PACKED_COLOR_ARRAY => self.write_packed_color_array(value.to()),
            VariantType::PACKED_VECTOR4_ARRAY => self.write_packed_vector4_array(value.to()),
            _ => {
                self.set_error(format!(
                    "write_variant: unsupported type '{:?}'",
                    value.get_type()
                ));
                false
            }
        }
    }

    /// Serializes a Godot `Object`'s properties to the stream.
    ///
    /// It inspects the object's properties, filtering for those with the `STORAGE`
    /// usage flag and a serializable type. It then writes a hash of the property
    /// names and types, followed by the value of each property. This hash allows
    /// `read_object` to verify that the data schema matches.
    #[func]
    pub fn write_object(&mut self, object: Gd<Object>) -> bool {
        let Some(properties) = self.get_storable_properties(&object, "write_object") else {
            return false;
        };

        let hash = Self::compute_property_hash(&properties);
        if !self.write_fixed("write_object.hash", hash.to_le_bytes()) {
            return false;
        }

        for prop in properties.iter() {
            let value = object.get(prop.name.arg());
            if !self.write_variant(value) {
                // write_variant sets its own detailed error.
                // We'll add context that it happened during object serialization.
                let base_error = self.last_error.clone();
                self.set_error(format!(
                    "write_object: failed to write property '{}': {}",
                    prop.name, base_error
                ));
                return false;
            }
        }
        true
    }

    /// Deserializes data from the stream into an existing Godot `Object`.
    ///
    /// It first reads a schema hash and compares it to a hash generated from the
    /// target object's storable properties. If they match, it proceeds to read
    /// each property's value from the stream and sets it on the object. If the
    /// hashes mismatch, an error is set and the object is not modified.
    #[func]
    pub fn read_object(&mut self, mut object: Gd<Object>) -> bool {
        let Some(properties) = self.get_storable_properties(&object, "read_object") else {
            return false;
        };

        let expected_hash = Self::compute_property_hash(&properties);

        let Some(stored_hash) = self.read_u64_inner("read_object.hash") else {
            // read_u64_inner would have set a more specific error.
            return false;
        };

        if expected_hash != stored_hash {
            self.set_error(format!(
                "read_object: schema hash mismatch. Expected {expected_hash}, found {stored_hash}. The object's structure does not match the serialized data."
            ));
            return false;
        }

        for prop in properties.iter() {
            let Some(value) = self.read_variant_by_type(prop.type_) else {
                let base_error = self.last_error.clone();
                self.set_error(format!(
                    "read_object: failed to read property '{}': {}",
                    prop.name, base_error
                ));
                return false;
            };
            object.set(prop.name.arg(), &value);
        }
        true
    }
}
