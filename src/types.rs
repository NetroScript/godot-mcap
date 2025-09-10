use godot::prelude::*;

/// Compression methods supported when writing MCAP files
#[derive(GodotConvert, Var, Export)]
#[godot(via = GString)]
pub enum MCAPCompression {
    /// Do not compress chunks.
    None,
    #[cfg(feature = "zstd")]
    /// Use Zstandard compression.
    Zstd,
    #[cfg(feature = "lz4")]
    /// Use LZ4 frame compression.
    Lz4,
}

#[derive(GodotClass)]
#[class(base=Resource)]
pub struct MCAPWriteOptions {
    #[export]
    pub compression: MCAPCompression,
    #[export]
    pub profile: GString,
    #[export]
    pub library: GString,
    #[export]
    pub chunk_size: i64,
    #[export]
    pub use_chunks: bool,
    #[export]
    pub disable_seeking: bool,
    #[export]
    pub emit_statistics: bool,
    #[export]
    pub emit_summary_offsets: bool,
    #[export]
    pub emit_message_indexes: bool,
    #[export]
    pub emit_chunk_indexes: bool,
    #[export]
    pub emit_attachment_indexes: bool,
    #[export]
    pub emit_metadata_indexes: bool,
    #[export]
    pub repeat_channels: bool,
    #[export]
    pub repeat_schemas: bool,
    #[export]
    pub calculate_chunk_crcs: bool,
    #[export]
    pub calculate_data_section_crc: bool,
    #[export]
    pub calculate_summary_section_crc: bool,
    #[export]
    pub calculate_attachment_crcs: bool,
    #[cfg(any(feature = "zstd", feature = "lz4"))]
    #[export]
    pub compression_level: u32,
    #[cfg(feature = "zstd")]
    #[export]
    pub compression_threads: u32,
}

/// Footer information of an MCAP file
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPFooter {
    #[export]
    pub summary_start: i64,
    #[export]
    pub summary_offset_start: i64,
    #[export]
    pub summary_crc: i64,
}

/// Chunk index entry (from summary)
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPChunkIndex {
    #[export]
    pub message_start_time: i64,
    #[export]
    pub message_end_time: i64,
    #[export]
    pub chunk_start_offset: i64,
    #[export]
    pub chunk_length: i64,
    #[export]
    pub message_index_offsets: Dictionary, // u16 -> u64
    #[export]
    pub message_index_length: i64,
    #[export]
    pub compression: GString,
    #[export]
    pub compressed_size: i64,
    #[export]
    pub uncompressed_size: i64,
}

/// Per-message index entry within a chunk
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPMessageIndexEntry {
    #[export]
    pub channel_id: i32,
    #[export]
    pub log_time_usec: i64,
    #[export]
    pub offset_uncompressed: i64,
}

/// Attachment index
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPAttachmentIndex {
    #[export]
    pub offset: i64,
    #[export]
    pub length: i64,
    #[export]
    pub log_time: i64,
    #[export]
    pub create_time: i64,
    #[export]
    pub data_size: i64,
    #[export]
    pub name: GString,
    #[export]
    pub media_type: GString,
}

/// Metadata index
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPMetadataIndex {
    #[export]
    pub offset: i64,
    #[export]
    pub length: i64,
    #[export]
    pub name: GString,
}

/// Summary resource wrapper (channels/schemas/indexes)
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPSummary {
    #[export]
    pub stats: Dictionary,
    #[export]
    pub channels_by_id: Dictionary, // u16 -> MCAPChannel
    #[export]
    pub schemas_by_id: Dictionary,  // u16 -> MCAPSchema
    #[export]
    pub chunk_indexes: Array<Gd<MCAPChunkIndex>>,
    #[export]
    pub attachment_indexes: Array<Gd<MCAPAttachmentIndex>>,
    #[export]
    pub metadata_indexes: Array<Gd<MCAPMetadataIndex>>,
}

/// Describes a schema used by one or more [MCAPChannel]s in an MCAP file
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPSchema {
    #[export]
    /// Schema numeric identifier (assigned by writer). 0 means unassigned yet.
    pub id: u16,
    #[export]
    /// Human readable schema name.
    pub name: GString,
    #[export]
    /// Encoding / format of the schema (e.g. "ros1msg", "jsonschema").
    pub encoding: GString,
    #[export]
    /// Serialized schema data payload.
    pub data: PackedByteArray,
}

/// Describes a channel which [Message]s are published to in an MCAP file
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPChannel {
    #[export]
    /// Channel numeric identifier (assigned by writer). 0 means unassigned yet.
    pub id: u16,
    #[export]
    /// Topic / channel name.
    pub topic: GString,
    #[export]
    /// Optional schema Resource used by this channel.
    pub schema: Option<Gd<MCAPSchema>>,
    #[export]
    /// Message encoding for messages on this channel (e.g. "cdr", "json").
    pub message_encoding: GString,
    #[export]
    /// Arbitrary string key/value metadata for the channel.
    pub metadata: Dictionary,
}

/// An event in an MCAP file, published to a [MCAPChannel]
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPMessage {
    #[export]
    /// Channel Resource this message is published on.
    pub channel: OnEditor<Gd<MCAPChannel>>,
    #[export]
    /// Sequence number for ordering within the channel.
    pub sequence: u32,
    #[export]
    /// Timestamp (microseconds) when the message was observed/logged.
    pub log_time: i64,
    #[export]
    /// Timestamp (microseconds) when the message was published.
    pub publish_time: i64,
    #[export]
    /// Raw message payload bytes.
    pub data: PackedByteArray,
}

/// An attachment and its metadata in an MCAP file
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPAttachment {
    #[export]
    /// Timestamp (microseconds) indicating when attachment was logged.
    pub log_time: i64,
    #[export]
    /// Timestamp (microseconds) indicating creation time of attachment data.
    pub create_time: i64,
    #[export]
    /// Attachment name (e.g. filename) for display/reference.
    pub name: GString,
    #[export]
    /// MIME media type for the attachment payload.
    pub media_type: GString,
    #[export]
    /// Attachment binary data.
    pub data: PackedByteArray,
}

/// MCAP MessageHeader record (channel id + sequence + timestamps) without message payload
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPMessageHeader {
    #[export]
    /// Channel ID the message belonged to.
    pub channel_id: u16,
    #[export]
    /// Sequence number within the channel.
    pub sequence: u32,
    #[export]
    /// Log timestamp in microseconds.
    pub log_time: i64,
    #[export]
    /// Publish timestamp in microseconds.
    pub publish_time: i64,
}

/// MCAP Metadata record
#[derive(GodotClass)]
#[class(no_init, base=Resource)]
pub struct MCAPMetadata {
    #[export]
    pub name: GString,
    #[export]
    /// Only string key/value pairs are supported.
    pub metadata: Dictionary,
}
