use crate::reader::MCAPMessageIterator;
use crate::reader::buf::{BufBackend, SharedBuf};
use crate::reader::filter::{MsgFilter, stream_chunk_apply};
use crate::types::*;
use godot::classes::ProjectSettings;
use godot::classes::file_access::ModeFlags;
use godot::prelude::*;
use godot::tools::GFile;
use mcap::read::{
    MessageStream, Options, RawMessage, RawMessageStream, Summary, footer as mcap_footer,
};
use std::borrow::Cow;
use std::collections::HashSet;
use std::io::Read;
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(GodotClass)]
/// MCAP file reader for Godot with sequential and indexed helpers.
///
/// Overview
/// - Loads an MCAP file into memory for fast random access and optional indexed queries.
/// - Provides direct streaming of messages (`messages`, `raw_messages`) and an indexed iterator (`stream_messages_iterator`).
/// - Exposes attachment and metadata access via summary indexes when present.
///
/// Memory & I/O
/// - When opening from a path, the reader first tries to memory-map (mmap) the file for zero-copy random access.
///   If mmap/OS open fails (e.g., due to platform or filesystem constraints), it falls back to loading into a PackedByteArray.
/// - When created from bytes via `from_bytes`, it stores the provided PackedByteArray (no mmap).
/// - Direct read APIs (`messages`, `raw_messages`) iterate the data stream and do not require a Summary.
/// - Indexed helpers (attachments, metadata, chunk/message indexes, and the iterator below) require a Summary section.
///
/// Summary requirements
/// - If the file has no summary, index-based methods return empty/0/false and set `last_error`.
/// - Check with `has_summary()` or call `read_summary()` to obtain a Resource view.
///
/// Errors
/// - On failure, methods set an internal error string retrievable with `get_last_error()`.
///
/// Basic usage (GDScript)
/// ```gdscript
/// # Open from file and iterate all messages (no summary required):
/// var reader := MCAPReader.open("res://capture.mcap", false)
/// for msg in reader.messages():
///     print(msg.channel.topic, " @ ", msg.log_time)
///
/// # Use the indexed iterator for efficient seeking (requires summary):
/// if reader.has_summary():
///     var it := reader.stream_messages_iterator()
///     # Optionally filter by channel id (first channel):
///     var ids := reader.channel_ids()
///     if ids.size() > 0:
///         it.for_channel(ids[0])
///     # Seek and iterate:
///     it.seek_to_time(1_000_000) # 1 second
///     for msg in it:
///         print("iter: ", msg.channel.topic, " @ ", msg.log_time)
///
/// # Read time-window messages using indexes:
/// var window := reader.messages_in_time_range(2_000_000, 3_000_000)
/// print("msgs in window: ", window.size())
///
/// # Attachments and metadata (require summary):
/// var attachments := reader.attachments()
/// var meta := reader.metadata_entries()
/// ```
#[class(no_init)]
pub struct MCAPReader {
    path: GString,
    /// Buffer for random access (mmap-backed or owned PackedByteArray).
    pub(super) buf: SharedBuf,
    /// Cached summary.
    pub(super) summary: Option<Summary>,
    /// If true, tolerate missing end-of-file magic.
    #[export]
    ignore_end_magic: bool,
    last_error: String,
}

impl MCAPReader {
    fn set_error(&mut self, msg: impl Into<String>) {
        let s = msg.into();
        self.last_error = s.clone();
        godot_error!("{}", s);
    }
    fn clear_error(&mut self) {
        self.last_error.clear();
    }

    /// Ensure summary exists and return a shared reference, setting last_error on failure.
    fn with_summary(&mut self) -> Result<&Summary, String> {
        if self.summary.is_none() {
            if let Err(e) = self.ensure_summary() {
                return Err(e);
            }
        }
        if self.summary.is_none() {
            let msg = "No summary available (indexed queries require summary)".to_string();
            self.set_error(&msg);
            return Err(msg);
        }
        Ok(self.summary.as_ref().unwrap())
    }

    // Core walker over indexed messages using chunk streaming
    fn for_each_indexed_msg<F>(&mut self, filter: &MsgFilter, mut visitor: F) -> Result<(), String>
    where
        F: FnMut(&Gd<MCAPMessage>) -> ControlFlow<()>,
    {
        // Clone the bytes handle first to avoid conflicting borrows with summary
        let bytes = self.buf.clone();
        let s = self.with_summary()?;
        for chunk_idx in &s.chunk_indexes {
            if !filter.chunk_might_match(chunk_idx) {
                continue;
            }
            // Stream and collect in a local vector to avoid borrowing self.buf across visitor calls
            let mut tmp: Vec<Gd<MCAPMessage>> = Vec::new();
            stream_chunk_apply(bytes.as_slice(), s, chunk_idx, filter, |_, gd| tmp.push(gd))?;
            for gd in tmp.iter() {
                if let ControlFlow::Break(()) = visitor(gd) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

#[godot_api]
impl MCAPReader {
    /// Open file and return a new reader instance.
    #[func]
    pub fn open(path: GString, ignore_end_magic: bool) -> Gd<Self> {
        let mut reader = Gd::from_object(Self {
            path: path.clone(),
            buf: Arc::new(BufBackend::Memory(PackedByteArray::new())),
            summary: None,
            ignore_end_magic,
            last_error: String::new(),
        });
        if !reader.bind_mut().load_from_path(path) {
            // keep error message; return object so caller can inspect get_last_error
        }
        reader
    }

    /// Create a reader from in-memory bytes.
    #[func]
    pub fn from_bytes(data: PackedByteArray, ignore_end_magic: bool) -> Gd<Self> {
        let mut reader = Gd::from_object(Self {
            path: GString::from("<memory>"),
            buf: Arc::new(BufBackend::Memory(data)),
            summary: None,
            ignore_end_magic,
            last_error: String::new(),
        });
        // Preload summary (non-fatal if missing)
        let _ = reader.bind_mut().ensure_summary();
        reader
    }

    /// Close and release buffers/caches.
    #[func]
    pub fn close(&mut self) {
        self.buf = Arc::new(BufBackend::Memory(PackedByteArray::new()));
        self.summary = None;
        self.path = GString::new();
        self.clear_error();
    }

    /// Returns true if the file contained a summary section.
    #[func]
    pub fn has_summary(&self) -> bool {
        self.summary.is_some()
    }

    /// Read or return cached summary as a Godot resource.
    #[func]
    pub fn read_summary(&mut self) -> Option<Gd<MCAPSummary>> {
        if self.ensure_summary().is_err() {
            return None;
        }
        self.summary.as_ref().map(|s| self.summary_to_resource(s))
    }

    /// Read and return the footer.
    #[func]
    pub fn footer(&mut self) -> Option<Gd<MCAPFooter>> {
        match mcap_footer(self.buf.as_slice()) {
            Ok(f) => Some(self.footer_to_resource(&f)),
            Err(e) => {
                self.set_error(format!("footer() failed: {}", e));
                None
            }
        }
    }

    /// Returns the last encountered error message, or empty string if none.
    #[func]
    pub fn get_last_error(&self) -> GString {
        GString::from(self.last_error.as_str())
    }

    /// Reads all messages as Godot `MCAPMessage` resources (allocates payloads as needed).
    /// Stops automatically before the summary section.
    #[func]
    pub fn messages(&mut self) -> Array<Gd<MCAPMessage>> {
        let mut out: Array<Gd<MCAPMessage>> = Array::new();
        self.clear_error();
        let opts = self.opts_enumset();
        let stream = match MessageStream::new_with_options(self.buf.as_slice(), opts) {
            Ok(s) => s,
            Err(e) => {
                self.set_error(format!("Creating MessageStream failed: {}", e));
                return out;
            }
        };

        for item in stream {
            match item {
                Ok(msg) => {
                    let gd = MCAPMessage::from_mcap(&msg);
                    out.push(&gd);
                }
                Err(e) => {
                    self.set_error(format!("Reading message failed: {}", e));
                    break;
                }
            }
        }
        out
    }

    /// Iterator version of messages() for GDScript `for` loops.
    ///
    /// Details
    /// - Requires a Summary section (uses chunk/message indexes for efficient seeking).
    /// - For files without a summary, this iterator will be empty; use `messages()` instead.
    #[func]
    pub fn stream_messages_iterator(&self) -> Gd<MCAPMessageIterator> {
        MCAPMessageIterator::new_from_reader(self, None)
    }

    /// Reads all raw messages (header + bytes) without constructing channels into Godot resources.
    /// Returns an array of Dictionaries: { header: MCAPMessageHeader, data: PackedByteArray }.
    #[func]
    pub fn raw_messages(&mut self) -> Array<Dictionary> {
        let mut out: Array<Dictionary> = Array::new();
        self.clear_error();
        let opts = self.opts_enumset();
        let stream = match RawMessageStream::new_with_options(self.buf.as_slice(), opts) {
            Ok(s) => s,
            Err(e) => {
                self.set_error(format!("Creating RawMessageStream failed: {}", e));
                return out;
            }
        };

        for item in stream {
            match item {
                Ok(RawMessage { header, data }) => {
                    let header_gd = MCAPMessageHeader::from_mcap(&header);
                    let mut dict = Dictionary::new();
                    let _ = dict.insert("header", header_gd);
                    match data {
                        Cow::Borrowed(b) => {
                            let _ = dict.insert("data", PackedByteArray::from(b.to_vec()));
                        }
                        Cow::Owned(v) => {
                            let _ = dict.insert("data", PackedByteArray::from(v));
                        }
                    };
                    out.push(&dict);
                }
                Err(e) => {
                    self.set_error(format!("Reading raw message failed: {}", e));
                    break;
                }
            }
        }
        out
    }

    /// Reads and returns attachment blobs using the summary's attachment indexes.
    /// If there is no summary or an error occurs, returns an empty array and sets last-error.
    #[func]
    pub fn attachments(&mut self) -> Array<Gd<MCAPAttachment>> {
        let mut out: Array<Gd<MCAPAttachment>> = Array::new();
        self.clear_error();
        let Some(summary) = &self.summary else {
            self.set_error("No summary available (attachment indexes require summary)");
            return out;
        };
        for idx in &summary.attachment_indexes {
            match mcap::read::attachment(self.buf.as_slice(), idx) {
                Ok(att) => {
                    let gd = MCAPAttachment::from_mcap(&att);
                    out.push(&gd);
                }
                Err(e) => {
                    self.set_error(format!("Reading attachment failed: {}", e));
                    break;
                }
            }
        }
        out
    }

    /// Reads and returns metadata records using the summary's metadata indexes.
    #[func]
    pub fn metadata_entries(&mut self) -> Array<Gd<MCAPMetadata>> {
        let mut out: Array<Gd<MCAPMetadata>> = Array::new();
        self.clear_error();
        let Some(summary) = &self.summary else {
            self.set_error("No summary available (metadata indexes require summary)");
            return out;
        };
        for idx in &summary.metadata_indexes {
            match mcap::read::metadata(self.buf.as_slice(), idx) {
                Ok(meta) => {
                    let gd = MCAPMetadata::from_mcap(&meta);
                    out.push(&gd);
                }
                Err(e) => {
                    self.set_error(format!("Reading metadata failed: {}", e));
                    break;
                }
            }
        }
        out
    }

    /// Returns the number of chunk indexes if a summary is present, else 0.
    #[func]
    pub fn chunk_count(&self) -> i32 {
        self.summary
            .as_ref()
            .map(|s| s.chunk_indexes.len() as i32)
            .unwrap_or(0)
    }

    /// Return chunk indexes (requires summary)
    #[func]
    pub fn chunk_indexes(&mut self) -> Array<Gd<MCAPChunkIndex>> {
        let mut out: Array<Gd<MCAPChunkIndex>> = Array::new();
        if self.ensure_summary().is_err() {
            return out;
        }
        if let Some(s) = &self.summary {
            for idx in &s.chunk_indexes {
                out.push(&self.chunk_index_to_resource(idx));
            }
        }
        out
    }

    /// Read per-channel message indexes for a given chunk.
    /// Returns a Dictionary mapping MCAPChannel -> Array[MCAPMessageIndexEntry]
    #[func]
    pub fn message_indexes_for_chunk(&mut self, idx: Gd<MCAPChunkIndex>) -> Dictionary {
        let mut out = Dictionary::new();
        if self.ensure_summary().is_err() {
            return out;
        }
        let Some(summary) = &self.summary else {
            return out;
        };

        let idx_native = self.chunk_index_from_resource(&idx);
        match summary.read_message_indexes(self.buf.as_slice(), &idx_native) {
            Ok(map) => {
                for (ch, entries) in map.into_iter() {
                    let ch_gd = MCAPChannel::from_mcap(ch.as_ref());
                    let mut arr: Array<Gd<MCAPMessageIndexEntry>> = Array::new();
                    for e in entries.iter() {
                        arr.push(&self.message_index_entry_to_resource(ch.id, e));
                    }
                    let _ = out.insert(ch_gd, arr);
                }
            }
            Err(e) => {
                self.set_error(format!("read_message_indexes failed: {}", e));
            }
        }
        out
    }

    /// Seek to a message given chunk index and message index entry.
    #[func]
    pub fn seek_message(
        &mut self,
        idx: Gd<MCAPChunkIndex>,
        entry: Gd<MCAPMessageIndexEntry>,
    ) -> Option<Gd<MCAPMessage>> {
        if self.ensure_summary().is_err() {
            return None;
        }
        let Some(summary) = &self.summary else {
            return None;
        };
        let idx_native = self.chunk_index_from_resource(&idx);
        let entry_native = self.message_index_entry_from_resource(&entry);
        match summary.seek_message(self.buf.as_slice(), &idx_native, &entry_native) {
            Ok(msg) => Some(MCAPMessage::from_mcap(&msg)),
            Err(e) => {
                self.set_error(format!("seek_message failed: {}", e));
                None
            }
        }
    }

    // ----- Indexed time-window and channel-filtered reads -----

    /// Read messages across all channels within [start_usec, end_usec] inclusive, using indexes.
    /// On error, returns an empty array and sets last-error.
    #[func]
    pub fn messages_in_time_range(
        &mut self,
        start_usec: i64,
        end_usec: i64,
    ) -> Array<Gd<MCAPMessage>> {
        let mut out: Array<Gd<MCAPMessage>> = Array::new();
        self.clear_error();
        if start_usec > end_usec {
            return out;
        }
        let start = if start_usec < 0 {
            0u64
        } else {
            start_usec as u64
        };
        let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
        let filter = MsgFilter {
            time_start: Some(start),
            time_end: Some(end),
            channels: None,
        };
        if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
            out.push(gd);
            ControlFlow::Continue(())
        }) {
            self.set_error(e);
        }
        out
    }

    /// Read all messages for a single channel id, in log-time order, using indexes.
    #[func]
    pub fn messages_for_channel(&mut self, channel_id: i32) -> Array<Gd<MCAPMessage>> {
        let mut out: Array<Gd<MCAPMessage>> = Array::new();
        self.clear_error();
        if channel_id < 0 {
            return out;
        }
        let mut set = HashSet::new();
        set.insert(channel_id as u16);
        let filter = MsgFilter {
            time_start: None,
            time_end: None,
            channels: Some(set),
        };
        if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
            out.push(gd);
            ControlFlow::Continue(())
        }) {
            self.set_error(e);
        }
        out
    }

    /// Read all messages for the first channel matching the given topic.
    #[func]
    pub fn messages_for_topic(&mut self, topic: GString) -> Array<Gd<MCAPMessage>> {
        let out: Array<Gd<MCAPMessage>> = Array::new();
        self.clear_error();
        let s = match self.with_summary() {
            Ok(s) => s,
            Err(_) => return out,
        };
        let t = topic.to_string();
        let mut channel_id: Option<u16> = None;
        for (id, ch) in s.channels.iter() {
            if ch.topic == t {
                channel_id = Some(*id);
                break;
            }
        }
        if let Some(ch_id) = channel_id {
            return self.messages_for_channel(ch_id as i32);
        }
        out
    }

    /// Read all messages for a set of channel ids, in log-time order, using indexes.
    #[func]
    pub fn messages_for_channels(
        &mut self,
        channel_ids: PackedInt32Array,
    ) -> Array<Gd<MCAPMessage>> {
        let mut out: Array<Gd<MCAPMessage>> = Array::new();
        self.clear_error();
        let mut set: HashSet<u16> = HashSet::new();
        let len = channel_ids.len();
        for i in 0..len {
            if let Some(id) = channel_ids.get(i) {
                if id >= 0 {
                    let _ = set.insert(id as u16);
                }
            }
        }
        if set.is_empty() {
            return out;
        }
        let filter = MsgFilter {
            time_start: None,
            time_end: None,
            channels: Some(set),
        };
        if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
            out.push(gd);
            ControlFlow::Continue(())
        }) {
            self.set_error(e);
        }
        out
    }

    // ----- Basic file info -----

    /// First message log time in microseconds, or -1 if unavailable.
    #[func]
    pub fn first_message_time_usec(&mut self) -> i64 {
        if self.ensure_summary().is_err() {
            return -1;
        }
        match &self.summary {
            Some(s) => s
                .stats
                .as_ref()
                .map(|st| st.message_start_time as i64)
                .unwrap_or(-1),
            None => -1,
        }
    }

    /// Last message log time in microseconds, or -1 if unavailable.
    #[func]
    pub fn last_message_time_usec(&mut self) -> i64 {
        if self.ensure_summary().is_err() {
            return -1;
        }
        match &self.summary {
            Some(s) => s
                .stats
                .as_ref()
                .map(|st| st.message_end_time as i64)
                .unwrap_or(-1),
            None => -1,
        }
    }

    /// Duration (end - start) in microseconds, or -1 if unavailable.
    #[func]
    pub fn duration_usec(&mut self) -> i64 {
        if self.ensure_summary().is_err() {
            return -1;
        }
        match &self.summary {
            Some(s) => {
                if let Some(st) = &s.stats {
                    (st.message_end_time as i64) - (st.message_start_time as i64)
                } else {
                    -1
                }
            }
            None => -1,
        }
    }

    /// All channel IDs present in the file (unsorted).
    #[func]
    pub fn channel_ids(&mut self) -> PackedInt32Array {
        let mut arr = PackedInt32Array::new();
        if self.ensure_summary().is_err() {
            return arr;
        }
        if let Some(s) = &self.summary {
            for id in s.channels.keys() {
                arr.push(*id as i32);
            }
        }
        arr
    }

    /// All topic names present in the file (may contain duplicates across channels with different schemas).
    #[func]
    pub fn topic_names(&mut self) -> PackedStringArray {
        let mut arr = PackedStringArray::new();
        if self.ensure_summary().is_err() {
            return arr;
        }
        if let Some(s) = &self.summary {
            for ch in s.channels.values() {
                let gs = GString::from(ch.topic.as_str());
                arr.push(&gs);
            }
        }
        arr
    }

    /// Resolve a topic name to the first matching channel id, or -1 if not found.
    #[func]
    pub fn topic_to_channel_id(&mut self, topic: GString) -> i32 {
        if self.ensure_summary().is_err() {
            return -1;
        }
        let t = topic.to_string();
        if let Some(s) = &self.summary {
            let mut best: Option<u16> = None;
            for (id, ch) in s.channels.iter() {
                if ch.topic == t {
                    best = match best {
                        Some(prev) => Some(prev.min(*id)),
                        None => Some(*id),
                    };
                }
            }
            return best.map(|v| v as i32).unwrap_or(-1);
        }
        -1
    }

    /// All channel IDs that reference a given schema id.
    #[func]
    pub fn channels_for_schema(&mut self, schema_id: i32) -> PackedInt32Array {
        let mut arr = PackedInt32Array::new();
        if self.ensure_summary().is_err() {
            return arr;
        }
        let sid = if schema_id < 0 {
            return arr;
        } else {
            schema_id as u16
        };
        if let Some(s) = &self.summary {
            for (id, ch) in s.channels.iter() {
                if let Some(schema_arc) = &ch.schema {
                    if schema_arc.id == sid {
                        arr.push(*id as i32);
                    }
                }
            }
        }
        arr
    }

    /// Return the schema object used by a channel, if any.
    #[func]
    pub fn schema_for_channel(&mut self, channel_id: i32) -> Option<Gd<MCAPSchema>> {
        if self.ensure_summary().is_err() {
            return None;
        }
        let Some(s) = &self.summary else {
            return None;
        };
        let ch = match s.channels.get(&(channel_id as u16)) {
            Some(c) => c,
            None => return None,
        };
        match &ch.schema {
            Some(schema_arc) => Some(MCAPSchema::from_mcap(schema_arc.as_ref())),
            None => None,
        }
    }

    // ----- Counts (fast using indexes) -----

    /// Total message count; uses summary stats when available, else sums per-chunk indexes. (also requires summary)
    #[func]
    pub fn message_count_total(&mut self) -> i64 {
        if self.ensure_summary().is_err() {
            return 0;
        }
        let Some(s) = &self.summary else {
            return 0;
        };
        if let Some(st) = &s.stats {
            return st.message_count as i64;
        }
        let mut total: i64 = 0;
        for chunk_idx in &s.chunk_indexes {
            match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
                Ok(map) => {
                    for (_ch, entries) in map.into_iter() {
                        total += entries.len() as i64;
                    }
                }
                Err(e) => {
                    self.set_error(format!(
                        "message_count_total: read_message_indexes failed: {}",
                        e
                    ));
                    break;
                }
            }
        }
        total
    }

    /// Message count for a specific channel id.
    #[func]
    pub fn message_count_for_channel(&mut self, channel_id: i32) -> i64 {
        if self.ensure_summary().is_err() {
            return 0;
        }
        let Some(s) = &self.summary else {
            return 0;
        };
        let ch_id = if channel_id < 0 {
            return 0;
        } else {
            channel_id as u16
        };
        let mut total: i64 = 0;
        for chunk_idx in &s.chunk_indexes {
            match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
                Ok(map) => {
                    for (ch, entries) in map.into_iter() {
                        if ch.id == ch_id {
                            total += entries.len() as i64;
                        }
                    }
                }
                Err(e) => {
                    self.set_error(format!(
                        "message_count_for_channel: read_message_indexes failed: {}",
                        e
                    ));
                    break;
                }
            }
        }
        total
    }

    /// Message count across all channels within [start_usec, end_usec] inclusive.
    #[func]
    pub fn message_count_in_range(&mut self, start_usec: i64, end_usec: i64) -> i64 {
        if self.ensure_summary().is_err() {
            return 0;
        }
        let Some(s) = &self.summary else {
            return 0;
        };
        if end_usec < start_usec {
            return 0;
        }
        let start = if start_usec < 0 {
            0u64
        } else {
            start_usec as u64
        };
        let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
        let mut total: i64 = 0;
        for chunk_idx in &s.chunk_indexes {
            if chunk_idx.message_start_time > end || chunk_idx.message_end_time < start {
                continue;
            }
            match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
                Ok(map) => {
                    for (_ch, entries) in map.into_iter() {
                        if entries.is_empty() {
                            continue;
                        }
                        let lo = match entries.binary_search_by(|e| e.log_time.cmp(&start)) {
                            Ok(i) => i,
                            Err(i) => i,
                        };
                        let hi = match entries.binary_search_by(|e| e.log_time.cmp(&end)) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        if hi > lo {
                            total += (hi - lo) as i64;
                        }
                    }
                }
                Err(e) => {
                    self.set_error(format!(
                        "message_count_in_range: read_message_indexes failed: {}",
                        e
                    ));
                    break;
                }
            }
        }
        total
    }

    /// Message count for a channel within [start_usec, end_usec] inclusive.
    #[func]
    pub fn message_count_for_channel_in_range(
        &mut self,
        channel_id: i32,
        start_usec: i64,
        end_usec: i64,
    ) -> i64 {
        if self.ensure_summary().is_err() {
            return 0;
        }
        let Some(s) = &self.summary else {
            return 0;
        };
        if end_usec < start_usec {
            return 0;
        }
        let ch_id = if channel_id < 0 {
            return 0;
        } else {
            channel_id as u16
        };
        let start = if start_usec < 0 {
            0u64
        } else {
            start_usec as u64
        };
        let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
        let mut total: i64 = 0;
        for chunk_idx in &s.chunk_indexes {
            if chunk_idx.message_start_time > end || chunk_idx.message_end_time < start {
                continue;
            }
            match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
                Ok(map) => {
                    for (ch, entries) in map.into_iter() {
                        if ch.id != ch_id {
                            continue;
                        }
                        if entries.is_empty() {
                            continue;
                        }
                        let lo = match entries.binary_search_by(|e| e.log_time.cmp(&start)) {
                            Ok(i) => i,
                            Err(i) => i,
                        };
                        let hi = match entries.binary_search_by(|e| e.log_time.cmp(&end)) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        };
                        if hi > lo {
                            total += (hi - lo) as i64;
                        }
                    }
                }
                Err(e) => {
                    self.set_error(format!(
                        "message_count_for_channel_in_range: read_message_indexes failed: {}",
                        e
                    ));
                    break;
                }
            }
        }
        total
    }
}

// ----- internal helpers -----
impl MCAPReader {
    fn load_from_path(&mut self, path: GString) -> bool {
        // Try memory-mapping the file via an absolute OS path.
        // Works for res:// and user:// by globalizing the path; fall back to GFile streaming copy if needed.
        let abs = ProjectSettings::singleton().globalize_path(&path);
        match std::fs::File::open(abs.to_string()) {
            Ok(file) => match unsafe { memmap2::MmapOptions::new().map(&file) } {
                Ok(mmap) => {
                    self.buf = Arc::new(BufBackend::Mmap(mmap));
                    let _ = self.ensure_summary();
                    return true;
                }
                Err(e) => {
                    godot_warn!("mmap failed, falling back to buffered read: {}", e);
                }
            },
            Err(e) => {
                godot_warn!("OS-open failed ({}), trying Godot FileAccess: {}", path, e);
            }
        }

        let mut file = match GFile::open(&path, ModeFlags::READ) {
            Ok(f) => f,
            Err(e) => {
                self.set_error(format!("Failed to open {}: {}", path, e));
                return false;
            }
        };
        let mut bytes = Vec::new();
        if let Err(e) = file.read_to_end(&mut bytes) {
            self.set_error(format!("Failed to read {}: {}", path, e));
            return false;
        }
        self.buf = Arc::new(BufBackend::Memory(PackedByteArray::from(bytes)));
        let _ = self.ensure_summary();
        true
    }

    fn ensure_summary(&mut self) -> Result<(), String> {
        if self.summary.is_some() {
            return Ok(());
        }
        match Summary::read(self.buf.as_slice()) {
            Ok(opt) => {
                self.summary = opt;
                Ok(())
            }
            Err(e) => {
                let msg = format!("Reading summary failed: {}", e);
                self.set_error(&msg);
                Err(msg)
            }
        }
    }

    fn opts_enumset(&self) -> enumset::EnumSet<Options> {
        let mut set = enumset::EnumSet::empty();
        if self.ignore_end_magic {
            set.insert(Options::IgnoreEndMagic);
        }
        set
    }

    fn footer_to_resource(&self, f: &mcap::records::Footer) -> Gd<MCAPFooter> {
        Gd::from_object(MCAPFooter {
            summary_start: f.summary_start as i64,
            summary_offset_start: f.summary_offset_start as i64,
            summary_crc: f.summary_crc as i64,
        })
    }

    fn chunk_index_to_resource(&self, idx: &mcap::records::ChunkIndex) -> Gd<MCAPChunkIndex> {
        let mut dict = Dictionary::new();
        for (k, v) in idx.message_index_offsets.iter() {
            let _ = dict.insert(*k as i32, *v as i64);
        }
        Gd::from_object(MCAPChunkIndex {
            message_start_time: idx.message_start_time as i64,
            message_end_time: idx.message_end_time as i64,
            chunk_start_offset: idx.chunk_start_offset as i64,
            chunk_length: idx.chunk_length as i64,
            message_index_offsets: dict,
            message_index_length: idx.message_index_length as i64,
            compression: GString::from(idx.compression.as_str()),
            compressed_size: idx.compressed_size as i64,
            uncompressed_size: idx.uncompressed_size as i64,
        })
    }

    fn chunk_index_from_resource(&self, idx: &Gd<MCAPChunkIndex>) -> mcap::records::ChunkIndex {
        let b = idx.bind();
        let mut map = std::collections::BTreeMap::new();
        for (k, v) in b.message_index_offsets.iter_shared() {
            let key: i64 = k.try_to().unwrap_or(0);
            let val: i64 = v.try_to().unwrap_or(0);
            map.insert(key as u16, val as u64);
        }
        mcap::records::ChunkIndex {
            message_start_time: b.message_start_time as u64,
            message_end_time: b.message_end_time as u64,
            chunk_start_offset: b.chunk_start_offset as u64,
            chunk_length: b.chunk_length as u64,
            message_index_offsets: map,
            message_index_length: b.message_index_length as u64,
            compression: b.compression.to_string(),
            compressed_size: b.compressed_size as u64,
            uncompressed_size: b.uncompressed_size as u64,
        }
    }

    fn message_index_entry_to_resource(
        &self,
        channel_id: u16,
        e: &mcap::records::MessageIndexEntry,
    ) -> Gd<MCAPMessageIndexEntry> {
        Gd::from_object(MCAPMessageIndexEntry {
            channel_id: channel_id as i32,
            log_time_usec: e.log_time as i64,
            offset_uncompressed: e.offset as i64,
        })
    }

    fn message_index_entry_from_resource(
        &self,
        e: &Gd<MCAPMessageIndexEntry>,
    ) -> mcap::records::MessageIndexEntry {
        let b = e.bind();
        mcap::records::MessageIndexEntry {
            log_time: b.log_time_usec as u64,
            offset: b.offset_uncompressed as u64,
        }
    }

    fn summary_to_resource(&self, s: &Summary) -> Gd<MCAPSummary> {
        // stats
        let mut stats = Dictionary::new();
        if let Some(st) = &s.stats {
            let _ = stats.insert("message_count", st.message_count as i64);
            let _ = stats.insert("schema_count", st.schema_count as i64);
            let _ = stats.insert("channel_count", st.channel_count as i64);
            let _ = stats.insert("attachment_count", st.attachment_count as i64);
            let _ = stats.insert("metadata_count", st.metadata_count as i64);
            let _ = stats.insert("chunk_count", st.chunk_count as i64);
            let _ = stats.insert("message_start_time", st.message_start_time as i64);
            let _ = stats.insert("message_end_time", st.message_end_time as i64);
        }

        // channels/schemas
        let mut channels = Dictionary::new();
        for (id, ch) in s.channels.iter() {
            let gd = MCAPChannel::from_mcap(ch.as_ref());
            let _ = channels.insert(*id as i32, gd);
        }
        let mut schemas = Dictionary::new();
        for (id, sc) in s.schemas.iter() {
            let gd = MCAPSchema::from_mcap(sc.as_ref());
            let _ = schemas.insert(*id as i32, gd);
        }

        // indexes
        let mut chunk_arr: Array<Gd<MCAPChunkIndex>> = Array::new();
        for idx in &s.chunk_indexes {
            chunk_arr.push(&self.chunk_index_to_resource(idx));
        }

        let mut att_arr: Array<Gd<MCAPAttachmentIndex>> = Array::new();
        for a in &s.attachment_indexes {
            att_arr.push(&Gd::from_object(MCAPAttachmentIndex {
                offset: a.offset as i64,
                length: a.length as i64,
                log_time: a.log_time as i64,
                create_time: a.create_time as i64,
                data_size: a.data_size as i64,
                name: GString::from(a.name.as_str()),
                media_type: GString::from(a.media_type.as_str()),
            }));
        }

        let mut meta_arr: Array<Gd<MCAPMetadataIndex>> = Array::new();
        for m in &s.metadata_indexes {
            meta_arr.push(&Gd::from_object(MCAPMetadataIndex {
                offset: m.offset as i64,
                length: m.length as i64,
                name: GString::from(m.name.as_str()),
            }));
        }

        Gd::from_object(MCAPSummary {
            stats,
            channels_by_id: channels,
            schemas_by_id: schemas,
            chunk_indexes: chunk_arr,
            attachment_indexes: att_arr,
            metadata_indexes: meta_arr,
        })
    }
}
