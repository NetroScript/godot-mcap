use crate::types::*;
use crate::util::{btreemap_to_dict, dict_to_btreemap};
use godot::prelude::*;
use mcap::records::{MessageHeader as McapMessageHeader, Metadata};
use mcap::{
    Attachment as McapAttachment, Channel as McapChannel, Compression, Message as McapMessage,
    Schema as McapSchema, WriteOptions,
};
use std::borrow::Cow;
use std::sync::Arc;

impl MCAPWriteOptions {
    // Translate the Godot Resource to mcap::WriteOptions
    pub fn to_mcap_owned(&self) -> WriteOptions {
        // Start from MCAP defaults
        let mut opts = WriteOptions::default();

        // compression: Option<Compression>
        let comp: Option<Compression> = match self.compression {
            MCAPCompression::None => None,
            #[cfg(feature = "zstd")]
            MCAPCompression::Zstd => Some(Compression::Zstd),
            #[cfg(all(not(feature = "zstd"), feature = "lz4"))]
            MCAPCompression::Zstd => None, // fallback if zstd feature isn't compiled
            #[cfg(feature = "lz4")]
            MCAPCompression::Lz4 => Some(Compression::Lz4),
        };

        opts = opts.compression(comp);
        opts = opts.profile(self.profile.to_string());
        opts = opts.library(self.library.to_string());

        // chunk_size: Option<u64> + use_chunks flag
        let chunk_size_opt = if self.use_chunks {
            if self.chunk_size > 0 {
                Some(self.chunk_size as u64)
            } else {
                // “use chunks but don’t auto-rotate” => None (manual flush)
                None
            }
        } else {
            // no chunking at all
            None
        };
        opts = opts.chunk_size(chunk_size_opt);
        opts = opts.use_chunks(self.use_chunks);

        // simple bools
        opts = opts.disable_seeking(self.disable_seeking);
        opts = opts.emit_statistics(self.emit_statistics);
        opts = opts.emit_summary_offsets(self.emit_summary_offsets);
        opts = opts.emit_message_indexes(self.emit_message_indexes);
        opts = opts.emit_chunk_indexes(self.emit_chunk_indexes);
        opts = opts.emit_attachment_indexes(self.emit_attachment_indexes);
        opts = opts.emit_metadata_indexes(self.emit_metadata_indexes);
        opts = opts.repeat_channels(self.repeat_channels);
        opts = opts.repeat_schemas(self.repeat_schemas);
        opts = opts.calculate_chunk_crcs(self.calculate_chunk_crcs);
        opts = opts.calculate_data_section_crc(self.calculate_data_section_crc);
        opts = opts.calculate_summary_section_crc(self.calculate_summary_section_crc);
        opts = opts.calculate_attachment_crcs(self.calculate_attachment_crcs);

        // optional tuning
        #[cfg(any(feature = "zstd", feature = "lz4"))]
        {
            opts = opts.compression_level(self.compression_level);
        }
        #[cfg(feature = "zstd")]
        {
            opts = opts.compression_threads(self.compression_threads);
        }

        opts
    }
}

impl MCAPSchema {
    /// Convert to an owned MCAP Schema
    pub fn to_mcap_owned(&self) -> McapSchema<'static> {
        McapSchema {
            id: self.id,
            name: self.name.to_string(),
            encoding: self.encoding.to_string(),
            data: Cow::Owned(self.data.to_vec()),
        }
    }

    /// Create a Godot schema from an MCAP schema (cloning data as needed)
    pub fn from_mcap(schema: &McapSchema) -> Gd<Self> {
        Gd::from_object(Self {
            id: schema.id,
            name: GString::from(schema.name.as_str()),
            encoding: GString::from(schema.encoding.as_str()),
            data: PackedByteArray::from(match &schema.data {
                Cow::Borrowed(s) => (*s).to_vec(),
                Cow::Owned(v) => v.clone(),
            }),
        })
    }
}

impl MCAPChannel {
    /// Convert to an owned MCAP Channel<'static>. If a schema is present,
    /// it becomes Arc<Schema<'static>>.
    pub fn to_mcap_owned(&self) -> McapChannel<'static> {
        let schema_arc = self.schema.as_ref().map(|gd_schema| {
            let schema = gd_schema.bind(); // &MCAPSchema
            Arc::new(schema.to_mcap_owned())
        });

        McapChannel {
            id: self.id,
            topic: self.topic.to_string(),
            schema: schema_arc,
            message_encoding: self.message_encoding.to_string(),
            metadata: dict_to_btreemap(&self.metadata),
        }
    }

    /// Create a Godot channel from an MCAP channel (cloning data as needed).
    pub fn from_mcap(ch: &McapChannel) -> Gd<Self> {
        Gd::from_object(Self {
            id: ch.id,
            topic: GString::from(ch.topic.as_str()),
            schema: ch
                .schema
                .as_ref()
                .map(|arc_schema| MCAPSchema::from_mcap(arc_schema.as_ref())),
            message_encoding: GString::from(ch.message_encoding.as_str()),
            metadata: btreemap_to_dict(&ch.metadata),
        })
    }
}

impl MCAPMessage {
    /// Convert to owned MCAP Message<'static>.
    pub fn to_mcap_owned(&self) -> Result<McapMessage<'static>, &'static str> {
        let ch_arc = Arc::new(self.channel.bind().to_mcap_owned());

        let log_time = u64::try_from(self.log_time).map_err(|_| "log_time must be >= 0")?;
        let publish_time =
            u64::try_from(self.publish_time).map_err(|_| "publish_time must be >= 0")?;

        Ok(McapMessage {
            channel: ch_arc,
            sequence: self.sequence,
            log_time,
            publish_time,
            data: Cow::Owned(self.data.to_vec()),
        })
    }

    /// Create a Godot message. Since MCAP `Message` carries an `Arc<Channel>`,
    /// we also build a Godot `MCAPChannel` and attach it.
    pub fn from_mcap(msg: &McapMessage) -> Gd<Self> {
        let mut gd = Gd::from_object(Self {
            channel: OnEditor::default(),
            sequence: 0,
            log_time: 0,
            publish_time: 0,
            data: PackedByteArray::new(),
        });
        {
            let mut b = gd.bind_mut();
            let ch_gd = MCAPChannel::from_mcap(msg.channel.as_ref());
            b.channel.init(ch_gd);
            b.sequence = msg.sequence;
            b.log_time = msg.log_time as i64;
            b.publish_time = msg.publish_time as i64;
            b.data = PackedByteArray::from(match &msg.data {
                Cow::Borrowed(s) => (*s).to_vec(),
                Cow::Owned(v) => v.clone(),
            });
        }
        gd
    }
}

impl MCAPAttachment {
    /// Convert to owned MCAP Attachment<'static>.
    pub fn to_mcap_owned(&self) -> Result<McapAttachment<'static>, &'static str> {
        let log_time = u64::try_from(self.log_time).map_err(|_| "log_time must be >= 0")?;
        let create_time =
            u64::try_from(self.create_time).map_err(|_| "create_time must be >= 0")?;

        Ok(McapAttachment {
            log_time,
            create_time,
            name: self.name.to_string(),
            media_type: self.media_type.to_string(),
            data: Cow::Owned(self.data.to_vec()),
        })
    }

    /// Create a Godot attachment from an MCAP attachment (cloning data as needed).
    pub fn from_mcap(att: &McapAttachment) -> Gd<Self> {
        Gd::from_object(Self {
            log_time: att.log_time as i64,
            create_time: att.create_time as i64,
            name: GString::from(att.name.as_str()),
            media_type: GString::from(att.media_type.as_str()),
            data: PackedByteArray::from(match &att.data {
                Cow::Borrowed(s) => (*s).to_vec(),
                Cow::Owned(v) => v.clone(),
            }),
        })
    }
}

impl MCAPMessageHeader {
    /// Convert to owned MCAP MessageHeader.
    pub fn to_mcap_owned(&self) -> Result<McapMessageHeader, &'static str> {
        let log_time = u64::try_from(self.log_time).map_err(|_| "log_time must be >= 0")?;
        let publish_time =
            u64::try_from(self.publish_time).map_err(|_| "publish_time must be >= 0")?;
        Ok(McapMessageHeader {
            channel_id: self.channel_id,
            sequence: self.sequence,
            log_time,
            publish_time,
        })
    }

    /// Create a Godot message header from an MCAP message header (cloning data as needed).
    pub fn from_mcap(header: &McapMessageHeader) -> Gd<Self> {
        Gd::from_object(Self {
            channel_id: header.channel_id,
            sequence: header.sequence,
            log_time: header.log_time as i64,
            publish_time: header.publish_time as i64,
        })
    }
}

impl MCAPMetadata {
    /// Convert to owned MCAP Metadata.
    pub fn to_mcap_owned(&self) -> Metadata {
        Metadata {
            name: self.name.to_string(),
            metadata: dict_to_btreemap(&self.metadata),
        }
    }

    /// Create a Godot metadata from an MCAP metadata (cloning data as needed).
    pub fn from_mcap(meta: &Metadata) -> Gd<Self> {
        Gd::from_object(Self {
            name: GString::from(meta.name.as_str()),
            metadata: btreemap_to_dict(&meta.metadata),
        })
    }
}
