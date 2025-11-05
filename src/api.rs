use crate::types::*;
use godot::classes::{Os, Time};
use godot::prelude::*;

#[godot_api]
impl IResource for MCAPWriteOptions {
    fn init(_base: Base<Resource>) -> Self {
        // Mirror MCAP defaults (or close enough for Godot)
        Self {
            #[cfg(feature = "zstd")]
            compression: MCAPCompression::Zstd,
            #[cfg(all(not(feature = "zstd"), feature = "lz4"))]
            compression: MCAPCompression::Lz4,
            #[cfg(all(not(feature = "zstd"), not(feature = "lz4")))]
            compression: MCAPCompression::None,
            profile: GString::new(),
            library: GString::from(&format!("godot-mcap-{}", env!("CARGO_PKG_VERSION"))),
            // Default MCAP: Some(1024*768), with chunking enabled
            chunk_size: (1024 * 768) as i64,
            use_chunks: true,
            disable_seeking: false,
            emit_statistics: true,
            emit_summary_offsets: true,
            emit_message_indexes: true,
            emit_chunk_indexes: true,
            emit_attachment_indexes: true,
            emit_metadata_indexes: true,
            repeat_channels: true,
            repeat_schemas: true,
            calculate_chunk_crcs: true,
            calculate_data_section_crc: true,
            calculate_summary_section_crc: true,
            calculate_attachment_crcs: true,
            #[cfg(any(feature = "zstd", feature = "lz4"))]
            compression_level: 0,
            #[cfg(feature = "zstd")]
            compression_threads: Os::singleton().get_processor_count() as u32,
        }
    }
}

#[godot_api]
impl MCAPWriteOptions {
    #[constant]
    /// No compression.
    const MCAP_COMPRESSION_NONE: i64 = MCAPCompression::None as i64;
    #[cfg(feature = "zstd")]
    #[constant]
    /// Zstandard compression.
    const MCAP_COMPRESSION_ZSTD: i64 = MCAPCompression::Zstd as i64;
    #[cfg(feature = "lz4")]
    #[constant]
    /// LZ4 frame compression.
    const MCAP_COMPRESSION_LZ4: i64 = MCAPCompression::Lz4 as i64;
}

#[godot_api]
impl MCAPSchema {
    /// Create a schema resource (id will be assigned when written).
    #[func]
    fn create(name: GString, encoding: GString, data: PackedByteArray) -> Gd<Self> {
        Gd::from_object(Self {
            id: 0,
            name,
            encoding,
            data,
        })
    }
}

#[godot_api]
impl MCAPChannel {
    /// Create a channel resource with empty message encoding & metadata.
    #[func]
    fn create(topic: GString) -> Gd<Self> {
        Gd::from_object(Self {
            id: 0,
            topic,
            schema: None,
            message_encoding: GString::from(""),
            metadata: Dictionary::new(),
        })
    }
}

#[godot_api]
impl MCAPMessage {
    /// Create a message using the current engine time for log & publish timestamps.
    /// The persisted timestamp may differ if the writer applies a timestamp offset.
    #[func]
    fn create(channel: Gd<MCAPChannel>, data: PackedByteArray) -> Gd<Self> {
        let now = Time::singleton().get_ticks_usec();
        let mut obj = Gd::from_object(Self {
            channel: OnEditor::default(),
            sequence: 0,
            log_time: now as i64,
            publish_time: now as i64,
            data,
        });
        obj.bind_mut().channel.init(channel);
        obj
    }

    /// Create a message with an explicit microsecond timestamp (used for both log & publish).
    /// The persisted timestamp may differ if the writer applies a timestamp offset.
    #[func]
    fn create_with_timestamp(
        channel: Gd<MCAPChannel>,
        data: PackedByteArray,
        time: u64,
    ) -> Gd<Self> {
        let mut obj = Gd::from_object(Self {
            channel: OnEditor::default(),
            sequence: 0,
            log_time: time as i64,
            publish_time: time as i64,
            data,
        });
        obj.bind_mut().channel.init(channel);
        obj
    }
}

#[godot_api]
impl MCAPAttachment {
    /// Create an attachment using the current engine time for log & create timestamps.
    /// The persisted timestamps may differ if the writer applies a timestamp offset.
    #[func]
    fn create(name: GString, media_type: GString, data: PackedByteArray) -> Gd<Self> {
        let now = Time::singleton().get_ticks_usec();
        Gd::from_object(Self {
            log_time: now as i64,
            create_time: now as i64,
            name,
            media_type,
            data,
        })
    }

    /// Create an attachment with an explicit microsecond timestamp (used for both log & create).
    /// The persisted timestamps may differ if the writer applies a timestamp offset.
    #[func]
    fn create_with_timestamp(
        name: GString,
        media_type: GString,
        data: PackedByteArray,
        time: u64,
    ) -> Gd<Self> {
        Gd::from_object(Self {
            log_time: time as i64,
            create_time: time as i64,
            name,
            media_type,
            data,
        })
    }
}

#[godot_api]
impl MCAPMessageHeader {
    /// Create a message header using the current engine time for both timestamps.
    /// The persisted timestamp may differ if the writer applies a timestamp offset.
    #[func]
    fn create(channel_id: i32) -> Gd<Self> {
        let now = Time::singleton().get_ticks_usec();
        Gd::from_object(Self {
            channel_id: channel_id as u16,
            sequence: 0,
            log_time: now as i64,
            publish_time: now as i64,
        })
    }

    /// Create a message header with an explicit timestamp (applied to log & publish).
    /// The persisted timestamp may differ if the writer applies a timestamp offset.
    #[func]
    fn create_with_timestamp(channel_id: i32, time: u64) -> Gd<Self> {
        Gd::from_object(Self {
            channel_id: channel_id as u16,
            sequence: 0,
            log_time: time as i64,
            publish_time: time as i64,
        })
    }
}

#[godot_api]
impl MCAPMetadata {
    /// Create a metadata resource
    #[func]
    fn create(name: GString, metadata: Dictionary) -> Gd<Self> {
        Gd::from_object(Self { name, metadata })
    }
}
