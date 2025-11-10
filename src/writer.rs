use crate::{types::*, util::*};
use enumset::EnumSet;
use godot::classes::{RefCounted, Time, file_access::ModeFlags};
use godot::prelude::*;
use godot::tools::GFile;
use mcap::Writer;
use mcap::records::Metadata;
use mcap::write::PrivateRecordOptions;

#[derive(GodotClass)]
/// MCAP file writer for Godot.
///
/// Overview
/// - Opens a file and writes MCAP records (channels, schemas, messages, attachments, metadata).
/// - Accepts either full `MCAPMessage` resources via `write()` or pairs of header+payload via `write_to_known_channel()`.
/// - Exposes a configurable `options` Resource to control chunking, compression, and emitted indexes before opening.
///
/// Error handling
/// - Methods returning `bool` yield `false` on failure and set an internal last-error string.
/// - Use `get_last_error()` to retrieve the message; success clears the last error.
/// - If the writer is dropped without `close()`, it will attempt to finalize the file in `Drop`.
///
/// Minimal example
/// ```gdscript
/// var writer := MCAPWriter.new()
/// if writer.open("user://test.mcap"):
///     var ch := MCAPChannel.create("messages")
///     # Optional: set encoding if needed
///     # ch.message_encoding = "raw" # e.g. "json"
///
///     var msg := MCAPMessage.create(ch, var_to_bytes_with_objects("Hello World"))
///
///     writer.write(msg)
///     writer.close()
/// else:
///     push_error(writer.get_last_error())
/// ```
///
/// Full example with options, schemas, and channels
/// ```gdscript
/// var w := MCAPWriter.new()
/// # Optional: tune options before opening
/// w.options = MCAPWriteOptions.new()
/// w.options.compression = MCAPCompression.None
/// w.options.emit_summary_offsets = true
/// w.options.emit_message_indexes = true
/// w.options.emit_chunk_indexes = true
///
/// if not w.open("user://out.mcap"):
///     push_error("open failed: %s" % w.get_last_error())
///     return
///
/// # Define a schema (optional)
/// var schema_id := w.add_schema("MyType", "jsonschema", PackedByteArray())
/// if schema_id < 0:
///     push_error("add_schema failed: %s" % w.get_last_error())
///
/// # Add a channel
/// var ch_id := w.add_channel(schema_id, "/ch", "json", {})
/// if ch_id < 0:
///     push_error("add_channel failed: %s" % w.get_last_error())
///
/// # Write messages to the known channel
/// var hdr := MCAPMessageHeader.create(ch_id)
/// hdr.sequence = 1
/// hdr.log_time = 1_000_000 # usec
/// hdr.publish_time = 1_000_000
/// var payload := PackedByteArray("{\"hello\":\"world\"}".to_utf8_buffer())
/// if not w.write_to_known_channel(hdr, payload):
///     push_error("write_to_known_channel failed: %s" % w.get_last_error())
///
/// # Optionally write an attachment or metadata
/// # var att := MCAPAttachment.create("snapshot.bin", "application/octet-stream", PackedByteArray())
/// # w.attach(att)
/// # var meta := MCAPMetadata.create("run_info", {"key": "value"})
/// # w.write_metadata(meta)
///
/// # Ensure chunks end cleanly for streaming readers
/// w.flush()
///
/// # Finalize the file
/// if not w.close():
///     push_error("close failed: %s" % w.get_last_error())
/// ```
///
/// Notes
/// - Set or replace `options` before calling `open()`; they’re read once to construct the writer.
/// - `write()` converts a full MCAPMessage Resource to mcap::Message (includes the channel fields).
/// - `write_to_known_channel()` avoids schema/channel lookups when you already have their IDs.
/// - `flush()` finishes the current chunk and flushes I/O to keep the file streamable mid-session.
/// - Timestamps are microseconds (usec). Configure `set_timestamp_offset_*()` if you need to
///   shift the stored timebase for messages created with engine-relative clocks.
#[class(init)]
struct MCAPWriter {
    base: Base<RefCounted>,
    path: GString,
    writer: Option<Writer<GFile>>,
    /// Options for writing the MCAP file. Modify these before calling `open()`.
    #[export]
    options: Option<Gd<MCAPWriteOptions>>,
    // Internal last error string
    last_error: String,
    // Microsecond offset applied when writing message/attachment timestamps
    timestamp_offset_usec: i64,
    // Once a time-bearing record has been written the offset can no longer change
    timestamp_offset_locked: bool,
}

impl MCAPWriter {
    /// Set and log the last error.
    fn set_error(&mut self, msg: impl Into<String>) {
        let s = msg.into();
        self.last_error = s.clone();
        godot_error!("{}", s);
    }

    /// Clear the last error.
    fn clear_error(&mut self) {
        self.last_error.clear();
    }

    /// Get a mutable reference to the writer or set an error if it's not open.
    fn writer_or_err_mut(&mut self, caller: &str) -> Option<&mut Writer<GFile>> {
        if self.writer.is_none() {
            self.set_error(format!("{} called before open()", caller));
            return None;
        }
        self.writer.as_mut()
    }

    /// Helper to run an operation on the writer and map Result to a return type with a default on error.
    fn with_writer<R, E>(
        &mut self,
        caller: &str,
        f: impl FnOnce(&mut Writer<GFile>) -> Result<R, E>,
        err_ret: R,
    ) -> R
    where
        R: Copy,
        E: core::fmt::Display,
    {
        if let Some(w) = self.writer_or_err_mut(caller) {
            match f(w) {
                Ok(v) => {
                    self.clear_error();
                    v
                }
                Err(e) => {
                    self.set_error(format!("{} failed: {}", caller, e));
                    err_ret
                }
            }
        } else {
            err_ret
        }
    }

    fn ensure_offset_mutable(&mut self, caller: &str) -> bool {
        if self.timestamp_offset_locked {
            self.set_error(format!(
                "{} called after writing time-bearing records; the timestamp offset is locked",
                caller
            ));
            return false;
        }
        true
    }

    fn set_timestamp_offset_internal(&mut self, caller: &str, offset: i64) -> bool {
        if !self.ensure_offset_mutable(caller) {
            return false;
        }
        self.timestamp_offset_usec = offset;
        self.clear_error();
        true
    }

    fn adjust_timestamp(&self, value: u64, what: &str) -> Result<u64, String> {
        if self.timestamp_offset_usec == 0 {
            return Ok(value);
        }

        if self.timestamp_offset_usec > 0 {
            let offset = self.timestamp_offset_usec as u64;
            if value < offset {
                return Err(format!(
                    "{what} ({value}) is earlier than the configured timestamp offset ({offset})"
                ));
            }
            Ok(value - offset)
        } else {
            let offset = (-self.timestamp_offset_usec) as u64;
            value
                .checked_add(offset)
                .ok_or_else(|| format!("{what} overflowed when applying the timestamp offset"))
        }
    }

    fn lock_timestamp_offset(&mut self) {
        self.timestamp_offset_locked = true;
    }
}

#[godot_api]
impl MCAPWriter {
    /// Initializes the MCAPWriter with default values.
    /// Should any customization be needed, modify the `options` property before calling `open()`.
    #[func]
    pub fn open(&mut self, path: GString) -> bool {
        // If a file is already open, return false and print an error
        if self.writer.is_some() {
            self.set_error("open() called but a file is already open");
            return false;
        }

        self.path = path;
        // reset last error for a fresh session
        self.clear_error();

        // 1) open file
        let file = match GFile::open(&self.path, ModeFlags::WRITE) {
            Ok(f) => f,
            Err(err) => {
                self.set_error(format!("Failed to open {}: {}", self.path, err));
                self.writer = None;
                return false;
            }
        };

        // 2) build MCAP WriteOptions from Resource if provided, else use defaults
        if self.options.is_none() {
            let default_opts = MCAPWriteOptions::new_gd();
            self.options = Some(default_opts);
        }
        let opts = self.options.as_ref().unwrap().bind().to_mcap_owned();

        // 3) create writer with options
        match opts.create(file) {
            Ok(w) => {
                self.writer = Some(w);
                self.timestamp_offset_locked = false;
                self.clear_error();
                true
            }
            Err(e) => {
                self.set_error(format!("Failed to create MCAP writer: {}", e));
                self.writer = None;
                false
            }
        }
    }

    /// Returns whether the MCAPWriter is currently open.
    /// Returns true if open, false otherwise.
    #[func]
    pub fn is_open(&self) -> bool {
        self.writer.is_some()
    }

    /// Returns the path of the currently opened MCAP file.
    /// If no file is open, returns an empty string.
    #[func]
    pub fn get_path(&self) -> GString {
        self.path.clone()
    }

    /// Sets the microsecond offset that will be applied to subsequent message and attachment timestamps.
    /// Positive offsets shift timestamps backwards (toward zero); negative offsets shift them forward.
    /// The offset can be changed freely until a time-bearing record is written, after which it locks.
    /// If applying the offset would underflow a timestamp, the write call will fail with an error.
    #[func]
    pub fn set_timestamp_offset_usec(&mut self, offset: i64) -> bool {
        self.set_timestamp_offset_internal("set_timestamp_offset_usec", offset)
    }

    /// Convenience helper that treats the current engine ticks as the zero point for future writes.
    #[func]
    pub fn set_timestamp_offset_to_now(&mut self) -> bool {
        let now = Time::singleton().get_ticks_usec() as i64;
        self.set_timestamp_offset_internal("set_timestamp_offset_to_now", now)
    }

    /// Returns the currently configured timestamp offset in microseconds.
    #[func]
    pub fn get_timestamp_offset_usec(&self) -> i64 {
        self.timestamp_offset_usec
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
        self.with_writer(
            "add_schema",
            |w| {
                w.add_schema(
                    name.to_string().as_str(),
                    encoding.to_string().as_str(),
                    data.as_slice(),
                )
                .map(|id| id as i64)
            },
            -1,
        )
    }

    /// Adds a schema using an MCAPSchema resource
    /// The ID of the schema resource will be updated with the assigned ID.
    ///
    /// * `schema`: The MCAPSchema resource to add.
    #[func]
    pub fn add_schema_object(&mut self, mut schema: Gd<crate::types::MCAPSchema>) {
        let mut sc = schema.bind_mut();
        let new_id = self.with_writer(
            "add_schema_object",
            |w| {
                w.add_schema(
                    sc.name.to_string().as_str(),
                    sc.encoding.to_string().as_str(),
                    sc.data.as_slice(),
                )
            },
            0,
        );
        sc.id = new_id;
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
        // Convert Godot Dictionary to BTreeMap<String, String>
        let meta_map = dict_to_btreemap(&metadata);
        self.with_writer(
            "add_channel",
            |w| {
                w.add_channel(
                    schema_id as u16,
                    topic.to_string().as_str(),
                    message_encoding.to_string().as_str(),
                    &meta_map,
                )
                .map(|id| id as i64)
            },
            -1,
        )
    }

    /// Adds a channel using an MCAPChannel resource
    /// The ID of the channel resource will be updated with the assigned ID.
    /// It is required that the schema (if any) has already been added via `add_schema()`.
    ///
    /// * `channel`: The MCAPChannel resource to add.
    #[func]
    pub fn add_channel_object(&mut self, mut channel: Gd<crate::types::MCAPChannel>) {
        let mut ch = channel.bind_mut();
        // Convert Godot Dictionary to BTreeMap<String, String>
        let meta_map = dict_to_btreemap(&ch.metadata);
        let new_id = self.with_writer(
            "add_channel_object",
            |w| {
                w.add_channel(
                    if let Some(schema) = &ch.schema {
                        schema.bind().id as u16
                    } else {
                        0
                    },
                    ch.topic.to_string().as_str(),
                    ch.message_encoding.to_string().as_str(),
                    &meta_map,
                )
            },
            0,
        );
        ch.id = new_id;
    }

    /// Write the given message (and its provided channel, if not already added).
    /// The provided channel ID and schema ID will be used as IDs in the resulting MCAP.
    /// The writer applies its configured timestamp offset before serializing the record.
    #[func]
    pub fn write(&mut self, message: Gd<crate::types::MCAPMessage>) -> bool {
        let mut mcap_msg = match message.bind().to_mcap_owned() {
            Ok(msg) => msg,
            Err(err) => {
                self.set_error(format!(
                    "write failed to convert MCAPMessage to mcap::Message: {}",
                    err
                ));
                return false;
            }
        };

        mcap_msg.log_time = match self.adjust_timestamp(mcap_msg.log_time, "message.log_time") {
            Ok(t) => t,
            Err(err) => {
                self.set_error(err);
                return false;
            }
        };
        mcap_msg.publish_time =
            match self.adjust_timestamp(mcap_msg.publish_time, "message.publish_time") {
                Ok(t) => t,
                Err(err) => {
                    self.set_error(err);
                    return false;
                }
            };

        let ok = self.with_writer("write", |w| w.write(&mcap_msg).map(|_| true), false);
        if ok {
            self.lock_timestamp_offset();
        }
        ok
    }

    /// Write a message to an added channel, given its ID.
    ///
    /// This skips hash lookups of the channel and schema if you already added them.
    /// The writer applies its configured timestamp offset before serializing the record.
    #[func]
    pub fn write_to_known_channel(
        &mut self,
        header: Gd<MCAPMessageHeader>,
        data: PackedByteArray,
    ) -> bool {
        let mut mcap_header = match header.bind().to_mcap_owned() {
            Ok(h) => h,
            Err(err) => {
                self.set_error(format!("write_to_known_channel failed to convert MCAPMessageHeader to mcap::MessageHeader: {}", err));
                return false;
            }
        };

        mcap_header.log_time = match self.adjust_timestamp(mcap_header.log_time, "header.log_time")
        {
            Ok(t) => t,
            Err(err) => {
                self.set_error(err);
                return false;
            }
        };
        mcap_header.publish_time =
            match self.adjust_timestamp(mcap_header.publish_time, "header.publish_time") {
                Ok(t) => t,
                Err(err) => {
                    self.set_error(err);
                    return false;
                }
            };

        let ok = self.with_writer(
            "write_to_known_channel",
            |w| {
                w.write_to_known_channel(&mcap_header, data.as_slice())
                    .map(|_| true)
            },
            false,
        );

        if ok {
            self.lock_timestamp_offset();
        }

        ok
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
        if opcode < 0x80 {
            self.set_error("write_private_record called with opcode < 0x80");
            return false;
        }

        let opts: EnumSet<PrivateRecordOptions> = if include_in_chunks {
            EnumSet::only(PrivateRecordOptions::IncludeInChunks)
        } else {
            EnumSet::empty()
        };

        self.with_writer(
            "write_private_record",
            |w| {
                w.write_private_record(opcode, data.as_slice(), opts)
                    .map(|_| true)
            },
            false,
        )
    }

    /// Write an attachment to the MCAP file. This finishes any current chunk before writing the
    /// attachment. The writer applies its configured timestamp offset to the attachment timestamps.
    #[func]
    pub fn attach(&mut self, attachment: Gd<MCAPAttachment>) -> bool {
        let mut mcap_attach = match attachment.bind().to_mcap_owned() {
            Ok(att) => att,
            Err(err) => {
                self.set_error(format!(
                    "attach failed to convert MCAPAttachment to mcap::Attachment: {}",
                    err
                ));
                return false;
            }
        };

        mcap_attach.log_time =
            match self.adjust_timestamp(mcap_attach.log_time, "attachment.log_time") {
                Ok(t) => t,
                Err(err) => {
                    self.set_error(err);
                    return false;
                }
            };
        mcap_attach.create_time =
            match self.adjust_timestamp(mcap_attach.create_time, "attachment.create_time") {
                Ok(t) => t,
                Err(err) => {
                    self.set_error(err);
                    return false;
                }
            };

        let ok = self.with_writer("attach", |w| w.attach(&mcap_attach).map(|_| true), false);
        if ok {
            self.lock_timestamp_offset();
        }
        ok
    }

    /// Write a [Metadata](https://mcap.dev/spec#metadata-op0x0c) record to the MCAP file. This
    /// finishes any current chunk before writing the metadata.
    #[func]
    pub fn write_metadata(&mut self, metadata: Gd<MCAPMetadata>) -> bool {
        let metadata: Metadata = metadata.bind().to_mcap_owned();
        self.with_writer(
            "write_metadata",
            |w| w.write_metadata(&metadata).map(|_| true),
            false,
        )
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
        self.with_writer("flush", |w| w.flush().map(|_| true), false)
    }

    /// Finalizes and closes the MCAP file. Returns true on success.
    /// After calling this method, the MCAPWriter can be reused by calling `open()` again.
    /// If `close()` is not called, the file will be finalized when the MCAPWriter has no valid references.
    #[func]
    pub fn close(&mut self) -> bool {
        if let Some(mut w) = self.writer.take() {
            match w.finish() {
                Ok(_summary) => {
                    self.clear_error();
                    self.timestamp_offset_locked = false;
                    true
                }
                Err(e) => {
                    self.set_error(format!("finish failed: {}", e));
                    self.timestamp_offset_locked = false;
                    false
                }
            }
        } else {
            self.set_error("finish called before open()");
            false
        }
    }

    /// Returns the last encountered error message, or empty string if none.
    #[func]
    pub fn get_last_error(&self) -> GString {
        GString::from(self.last_error.as_str())
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
