use crate::{types::*, util::*};
use enumset::EnumSet;
use godot::classes::file_access::ModeFlags;
use godot::classes::{RefCounted};
use godot::prelude::*;
use godot::tools::GFile;
use mcap::records::Metadata;
use mcap::write::PrivateRecordOptions;
use mcap::{Attachment, Message, Writer, records::MessageHeader};



#[derive(GodotClass)]
/// This class allows writing MCAP files.
#[class(init)]
struct MCAPWriter {
    base: Base<RefCounted>,
    path: GString,
    writer: Option<Writer<GFile>>,
    /// Options for writing the MCAP file. Modify these before calling `open()`.
    #[export]
    options: OnEditor<Gd<MCAPWriteOptions>>,
    #[var]
    last_error: GString,
}

#[godot_api]
impl MCAPWriter {
    /// Initializes the MCAPWriter with default values.
    /// Should any customization be needed, modify the `options` property before calling `open()`.
    #[func]
    pub fn open(&mut self, path: GString) -> bool {
        // If a file is already open, return false and print an error
        if self.writer.is_some() {
            godot_error!("open() called but a file is already open");
            return false;
        }

        self.path = path;

        // 1) open file
        let file = match GFile::open(&self.path, ModeFlags::WRITE) {
            Ok(f) => f,
            Err(err) => {
                godot_error!("Failed to open {}: {}", self.path, err);
                self.writer = None;
                return false;
            }
        };

        // 2) build MCAP WriteOptions from Resource
        let opts = self.options.bind().to_mcap_owned();

        // 3) create writer with options
        match opts.create(file) {
            Ok(w) => {
                self.writer = Some(w);
                true
            }
            Err(e) => {
                godot_error!("Failed to create MCAP writer: {}", e);
                self.writer = None;
                false
            }
        }
    }

    /// Adds a schema, returning its ID. If a schema with the same content has been added already,
    /// its ID is returned. Returns -1 on error.
    ///
    /// * `name`: an identifier for the schema.
    /// * `encoding`: Describes the schema format.  The [well-known schema
    ///   encodings](https://mcap.dev/spec/registry#well-known-schema-encodings) are preferred. An
    ///   empty string indicates no schema is available.
    /// * `data`: The serialized schema content. If `encoding` is an empty string, `data` should
    ///   have zero length.
    #[func]
    pub fn add_schema(&mut self, name: GString, encoding: GString, data: PackedByteArray) -> i64 {
        if let Some(w) = self.writer.as_mut() {
            match w.add_schema(
                name.to_string().as_str(),
                encoding.to_string().as_str(),
                data.as_slice(),
            ) {
                Ok(id) => id as i64,
                Err(e) => {
                    godot_error!("add_schema failed: {}", e);
                    -1
                }
            }
        } else {
            godot_error!("add_schema called before open()");
            -1
        }
    }

    /// Adds a channel, returning its ID. If a channel with equivalent content was added previously,
    /// its ID is returned. Returns -1 on error.
    ///
    /// Useful with subsequent calls to [`write_to_known_channel()`].
    ///
    /// * `schema_id`: a schema_id returned from [`add_schema()`], or 0 if the channel has no
    ///   schema.
    /// * `topic`: The topic name.
    /// * `message_encoding`: Encoding for messages on this channel. The [well-known message
    ///   encodings](https://mcap.dev/spec/registry#well-known-message-encodings) are preferred.
    ///  * `metadata`: Metadata about this channel. The dictionary should contain only string keys and
    ///    string values.
    #[func]
    pub fn add_channel(
        &mut self,
        schema_id: i32,
        topic: GString,
        message_encoding: GString,
        metadata: Dictionary,
    ) -> i64 {
        if let Some(w) = self.writer.as_mut() {
            // Convert Godot Dictionary to BTreeMap<String, String>
            let meta_map = dict_to_btreemap(&metadata);

            match w.add_channel(
                schema_id as u16,
                topic.to_string().as_str(),
                message_encoding.to_string().as_str(),
                &meta_map,
            ) {
                Ok(id) => id as i64,
                Err(e) => {
                    godot_error!("add_channel failed: {}", e);
                    -1
                }
            }
        } else {
            godot_error!("add_channel called before open()");
            -1
        }
    }

    /// Write the given message (and its provided channel, if not already added).
    /// The provided channel ID and schema ID will be used as IDs in the resulting MCAP.
    /// Write a full message resource (channel provided in the resource) to the file.
    #[func]
    pub fn write(&mut self, message: Gd<crate::types::MCAPMessage>) -> bool {
        if let Some(w) = self.writer.as_mut() {
            let mcap_msg: Message = match message.bind().to_mcap_owned() {
                Ok(m) => m,
                Err(e) => {
                    godot_error!(
                        "write failed to convert MCAPMessage to mcap::Message: {}",
                        e
                    );
                    return false;
                }
            };

            match w.write(&mcap_msg) {
                Ok(_) => true,
                Err(e) => {
                    godot_error!("write failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("write called before open()");
            false
        }
    }

    /// Write a message to an added channel, given its ID.
    ///
    /// This skips hash lookups of the channel and schema if you already added them.
    #[func]
    pub fn write_to_known_channel(
        &mut self,
        header: Gd<MCAPMessageHeader>,
        data: PackedByteArray,
    ) -> bool {
        if let Some(w) = self.writer.as_mut() {
            let mcap_header: MessageHeader = match header.bind().to_mcap_owned() {
                Ok(h) => h,
                Err(e) => {
                    godot_error!(
                        "write_to_known_channel failed to convert MCAPMessageHeader to mcap::MessageHeader: {}",
                        e
                    );
                    return false;
                }
            };

            match w.write_to_known_channel(&mcap_header, data.as_slice()) {
                Ok(_msg) => true,
                Err(e) => {
                    godot_error!(
                        "write_to_known_channel failed to create mcap::Message: {}",
                        e
                    );
                    return false;
                }
            }
        } else {
            godot_error!("write_to_known_channel called before open()");
            false
        }
    }

    /// Write a private record using the provided options.
    ///
    /// Private records must have an opcode >= 0x80.
    #[func]
    pub fn write_private_record(
        &mut self,
        opcode: u8,
        data: PackedByteArray,
        include_in_chunks: bool,
    ) -> bool {
        if let Some(w) = self.writer.as_mut() {
            if opcode < 0x80 {
                godot_error!("write_private_record called with opcode < 0x80");
                return false;
            }

            let opts: EnumSet<PrivateRecordOptions> = if include_in_chunks {
                EnumSet::only(PrivateRecordOptions::IncludeInChunks)
            } else {
                EnumSet::empty()
            };
            match w.write_private_record(opcode, data.as_slice(), opts) {
                Ok(_) => true,
                Err(e) => {
                    godot_error!("write_private_record failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("write_private_record called before open()");
            false
        }
    }

    /// Write an attachment to the MCAP file. This finishes any current chunk before writing the
    /// attachment.
    #[func]
    pub fn attach(&mut self, attachment: Gd<MCAPAttachment>) -> bool {
        if let Some(w) = self.writer.as_mut() {
            let mcap_attach: Attachment = match attachment.bind().to_mcap_owned() {
                Ok(a) => a,
                Err(e) => {
                    godot_error!(
                        "attach failed to convert MCAPAttachment to mcap::Attachment: {}",
                        e
                    );
                    return false;
                }
            };

            match w.attach(&mcap_attach) {
                Ok(_) => true,
                Err(e) => {
                    godot_error!("attach failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("attach called before open()");
            false
        }
    }

    /// Write a [Metadata](https://mcap.dev/spec#metadata-op0x0c) record to the MCAP file. This
    /// finishes any current chunk before writing the metadata.
    #[func]
    pub fn write_metadata(&mut self, metadata: Gd<MCAPMetadata>) -> bool {
        if let Some(w) = self.writer.as_mut() {
            let metadata: Metadata = metadata.bind().to_mcap_owned();
            match w.write_metadata(&metadata) {
                Ok(_) => true,
                Err(e) => {
                    godot_error!("write_metadata failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("write_metadata called before open()");
            false
        }
    }

    /// Finishes the current chunk, if we have one, and flushes the underlying
    /// [writer](Write).
    ///
    /// We finish the chunk to guarantee that the file can be streamed by future
    /// readers at least up to this point.
    /// (The alternative is to just flush the writer mid-chunk.
    /// But if we did that, and then writing was suddenly interrupted afterwards,
    /// readers would have to try to recover a half-written chunk,
    /// probably with an unfinished compression stream.)
    ///
    /// Note that lossless compression schemes like LZ4 and Zstd improve
    /// as they go, so larger chunks will tend to have better compression.
    /// (Of course, this depends heavily on the entropy of what's being compressed!
    /// A stream of zeroes will compress great at any chunk size, and a stream
    /// of random data will compress terribly at any chunk size.)
    #[func]
    pub fn flush(&mut self) -> bool {
        if let Some(w) = self.writer.as_mut() {
            match w.flush() {
                Ok(_) => true,
                Err(e) => {
                    godot_error!("flush failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("flush called before open()");
            false
        }
    }

    /// Finalizes and closes the MCAP file. Returns true on success.
    /// After calling this method, the MCAPWriter can be reused by calling `open()` again.
    /// If `close()` is not called, the file will be finalized when the MCAPWriter has no valid references.
    #[func]
    pub fn close(&mut self) -> bool {
        if let Some(mut w) = self.writer.take() {
            match w.finish() {
                Ok(_summary) => true,
                Err(e) => {
                    godot_error!("finish failed: {}", e);
                    false
                }
            }
        } else {
            godot_error!("finish called before open()");
            false
        }
    }
}

impl Drop for MCAPWriter {
    fn drop(&mut self) {
        if self.writer.is_some() {
            godot_print!("MCAPWriter dropped without calling close(); finalizing file now.");
            let _ = self.close();
        }
    }
}
