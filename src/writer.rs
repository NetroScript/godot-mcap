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
///
/// Error handling:
/// - Methods that return `bool` will return `false` on failure and set an internal last-error message.
/// - Call [`get_last_error()`] to retrieve the most recent error as a `GString`.
/// - On successful operations, the last error is cleared.
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

    /// Write the given message (and its provided channel, if not already added).
    /// The provided channel ID and schema ID will be used as IDs in the resulting MCAP.
    /// Write a full message resource (channel provided in the resource) to the file.
    #[func]
    pub fn write(&mut self, message: Gd<crate::types::MCAPMessage>) -> bool {
        let mcap_msg: Message = match message.bind().to_mcap_owned() {
            Ok(m) => m,
            Err(e) => {
                self.set_error(format!(
                    "write failed to convert MCAPMessage to mcap::Message: {}",
                    e
                ));
                return false;
            }
        };

        self.with_writer("write", |w| w.write(&mcap_msg).map(|_| true), false)
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
        let mcap_header: MessageHeader = match header.bind().to_mcap_owned() {
            Ok(h) => h,
            Err(e) => {
                self.set_error(format!(
                    "write_to_known_channel failed to convert MCAPMessageHeader to mcap::MessageHeader: {}",
                    e
                ));
                return false;
            }
        };

        self.with_writer(
            "write_to_known_channel",
            |w| w
                .write_to_known_channel(&mcap_header, data.as_slice())
                .map(|_| true),
            false,
        )
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
            |w| w.write_private_record(opcode, data.as_slice(), opts).map(|_| true),
            false,
        )
    }

    /// Write an attachment to the MCAP file. This finishes any current chunk before writing the
    /// attachment.
    #[func]
    pub fn attach(&mut self, attachment: Gd<MCAPAttachment>) -> bool {
        let mcap_attach: Attachment = match attachment.bind().to_mcap_owned() {
            Ok(a) => a,
            Err(e) => {
                self.set_error(format!(
                    "attach failed to convert MCAPAttachment to mcap::Attachment: {}",
                    e
                ));
                return false;
            }
        };

        self.with_writer("attach", |w| w.attach(&mcap_attach).map(|_| true), false)
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
                    true
                }
                Err(e) => {
                    self.set_error(format!("finish failed: {}", e));
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
