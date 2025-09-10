mod writer;
mod reader;
mod util;
#[macro_use]
mod types;
#[macro_use]
mod api;
mod convert;

use godot::prelude::*;

struct MCAP;

#[gdextension]
unsafe impl ExtensionLibrary for MCAP {

    fn on_level_init(level: InitLevel) {
        println!("[godot-mcap]   Init level {level:?}");
    }

    fn on_level_deinit(level: InitLevel) {
        println!("[godot-mcap]   Deinit level {level:?}");
    }
}

