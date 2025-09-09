use std::collections::BTreeMap;
use godot::prelude::*;

pub fn dict_to_btreemap(dict: &Dictionary) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for (k, v) in dict.iter_shared() {
        // Be tolerant: stringify anything that’s not a GString
        let key = if let Ok(gs) = <GString>::try_from_variant(&k) {
            gs.to_string()
        } else {
            k.to_string()
        };
        let val = if let Ok(gs) = <GString>::try_from_variant(&v) {
            gs.to_string()
        } else {
            v.to_string()
        };
        map.insert(key, val);
    }
    map
}

pub fn btreemap_to_dict(map: &BTreeMap<String, String>) -> Dictionary {
    let mut dict = Dictionary::new();
    for (k, v) in map.iter() {
        let _ = dict.insert(GString::from(k.as_str()), GString::from(v.as_str()));
    }
    dict
}