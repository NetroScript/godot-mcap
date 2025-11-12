# Changelog

## [0.1.1]

### Added
- Introduced the `BinaryStream` Godot class for building binary payloads: cursor control, packing/reading primitives, vectors, transforms, colors, `StringName`, `NodePath`, `RID`, and all Packed* array types, with seamless conversion to and from `PackedByteArray`.
- Added `write_variant`, `read_variant_by_type`, and `{write,read}_object` helpers that serialize any supported `Variant` and storage-marked `Object` properties, including schema hashing to detect mismatches during deserialization.

### Documentation
- Expanded the README with binary stream usage examples and detailed how the reader constructs new Resource instances on each call.
- Updated Rust doc comments to spell out identity semantics for MCAP schema, channel, message, attachment, and metadata Resources, plus a minor replay iterator clarification.

## [0.1.0]

### Added
- Initial public release of the Godot MCAP extension.
