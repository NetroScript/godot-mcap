use crate::types::*;
use godot::classes::file_access::ModeFlags;
use godot::prelude::*;
use godot::tools::GFile;
use mcap::read::{footer as mcap_footer, MessageStream, Options, RawMessage, RawMessageStream, Summary};
use std::borrow::Cow;
use std::io::Read;
use std::ops::ControlFlow;
use std::collections::HashSet;

// Reusable message filter for time range and channel sets
struct MsgFilter {
	time_start: Option<u64>,
	time_end: Option<u64>,
	channels: Option<HashSet<u16>>, // if None: accept all
}

impl MsgFilter {
	#[inline]
	fn matches_time(&self, t: u64) -> bool {
		if let Some(s) = self.time_start { if t < s { return false; } }
		if let Some(e) = self.time_end { if t > e { return false; } }
		true
	}

	#[inline]
	fn matches_ch(&self, id: u16) -> bool {
		match &self.channels {
			Some(set) => set.contains(&id),
			None => true,
		}
	}

	#[inline]
	fn chunk_might_match(&self, idx: &mcap::records::ChunkIndex) -> bool {
		if let Some(s) = self.time_start { if idx.message_end_time < s { return false; } }
		if let Some(e) = self.time_end { if idx.message_start_time > e { return false; } }
		true
	}
}

// Shared helper: stream a chunk, apply filter, build MCAPMessage, and call a closure with (log_time, message)
fn stream_chunk_apply<F>(bytes: &[u8], summary: &Summary, chunk_idx: &mcap::records::ChunkIndex, filter: &MsgFilter, mut f: F) -> Result<(), String>
where
	F: FnMut(u64, Gd<MCAPMessage>),
{
	let iter = summary
		.stream_chunk(bytes, chunk_idx)
		.map_err(|e| format!("stream_chunk open failed: {}", e))?;
	for item in iter {
		match item {
			Ok(msg) => {
				if !filter.matches_time(msg.log_time) { continue; }
				if !filter.matches_ch(msg.channel.id) { continue; }
				let gd = MCAPMessage::from_mcap(&msg);
				f(msg.log_time, gd);
			}
			Err(e) => return Err(format!("stream_chunk failed: {}", e)),
		}
	}
	Ok(())
}
 

#[derive(GodotClass)]
/// Godot-facing MCAP reader with sequential and indexed helpers.
///
/// Memory & I/O
/// - Opening a file loads the entire MCAP file into a PackedByteArray in memory for fast random access.
/// - Direct read APIs (`messages`, `raw_messages`) iterate without requiring a summary.
/// - Indexed helpers (attachments, metadata, chunk/message indexes, and the iterator below) require a Summary section.
///
/// Summary requirements
/// - If the file has no summary, methods that depend on indexes will return an empty result and set `last_error`.
/// - Use `has_summary()` to check quickly.
///
/// Errors
/// - On failure, methods set an internal error string retrievable by `get_last_error()`.
#[class(no_init)]
pub struct MCAPReader {
	path: GString,
	/// File bytes for random access.
	buf: PackedByteArray,
	/// Cached summary.
	summary: Option<Summary>,
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
			if let Err(e) = self.ensure_summary() { return Err(e); }
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
			if !filter.chunk_might_match(chunk_idx) { continue; }
			// Stream and collect in a local vector to avoid borrowing self.buf across visitor calls
			let mut tmp: Vec<Gd<MCAPMessage>> = Vec::new();
			stream_chunk_apply(bytes.as_slice(), s, chunk_idx, filter, |_, gd| tmp.push(gd))?;
			for gd in tmp.iter() {
				if let ControlFlow::Break(()) = visitor(gd) { return Ok(()); }
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
			buf: PackedByteArray::new(),
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
			buf: data,
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
		self.buf = PackedByteArray::new();
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
		if self.ensure_summary().is_err() { return None; }
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
		if self.ensure_summary().is_err() { return out; }
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
		if self.ensure_summary().is_err() { return out; }
		let Some(summary) = &self.summary else { return out; };

		let idx_native = self.chunk_index_from_resource(&idx);
		match summary.read_message_indexes(self.buf.as_slice(), &idx_native) {
			Ok(map) => {
				for (ch, entries) in map.into_iter() {
					let ch_gd = MCAPChannel::from_mcap(ch.as_ref());
					let mut arr: Array<Gd<MCAPMessageIndexEntry>> = Array::new();
					for e in entries.iter() { arr.push(&self.message_index_entry_to_resource(ch.id, e)); }
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
	pub fn seek_message(&mut self, idx: Gd<MCAPChunkIndex>, entry: Gd<MCAPMessageIndexEntry>) -> Option<Gd<MCAPMessage>> {
		if self.ensure_summary().is_err() { return None; }
		let Some(summary) = &self.summary else { return None; };
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
	pub fn messages_in_time_range(&mut self, start_usec: i64, end_usec: i64) -> Array<Gd<MCAPMessage>> {
		let mut out: Array<Gd<MCAPMessage>> = Array::new();
		self.clear_error();
		if start_usec > end_usec { return out; }
		let start = if start_usec < 0 { 0u64 } else { start_usec as u64 };
		let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
		let filter = MsgFilter { time_start: Some(start), time_end: Some(end), channels: None };
		if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
			out.push(gd);
			ControlFlow::Continue(())
		}) { self.set_error(e); }
		out
	}

	/// Read all messages for a single channel id, in log-time order, using indexes.
	#[func]
	pub fn messages_for_channel(&mut self, channel_id: i32) -> Array<Gd<MCAPMessage>> {
		let mut out: Array<Gd<MCAPMessage>> = Array::new();
		self.clear_error();
		if channel_id < 0 { return out; }
		let mut set = HashSet::new(); set.insert(channel_id as u16);
		let filter = MsgFilter { time_start: None, time_end: None, channels: Some(set) };
		if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
			out.push(gd);
			ControlFlow::Continue(())
		}) { self.set_error(e); }
		out
	}

	/// Read all messages for the first channel matching the given topic.
	#[func]
	pub fn messages_for_topic(&mut self, topic: GString) -> Array<Gd<MCAPMessage>> {
		let out: Array<Gd<MCAPMessage>> = Array::new();
		self.clear_error();
		let s = match self.with_summary() { Ok(s) => s, Err(_) => return out };
		let t = topic.to_string();
		let mut channel_id: Option<u16> = None;
		for (id, ch) in s.channels.iter() { if ch.topic == t { channel_id = Some(*id); break; } }
		if let Some(ch_id) = channel_id { return self.messages_for_channel(ch_id as i32); }
		out
	}

	/// Read all messages for a set of channel ids, in log-time order, using indexes.
	#[func]
	pub fn messages_for_channels(&mut self, channel_ids: PackedInt32Array) -> Array<Gd<MCAPMessage>> {
		let mut out: Array<Gd<MCAPMessage>> = Array::new();
		self.clear_error();
		let mut set: HashSet<u16> = HashSet::new();
		let len = channel_ids.len();
		for i in 0..len { if let Some(id) = channel_ids.get(i) { if id >= 0 { let _ = set.insert(id as u16); } } }
		if set.is_empty() { return out; }
		let filter = MsgFilter { time_start: None, time_end: None, channels: Some(set) };
		if let Err(e) = self.for_each_indexed_msg(&filter, |gd| {
			out.push(gd);
			ControlFlow::Continue(())
		}) { self.set_error(e); }
		out
	}

	// ----- Basic file info -----

	/// First message log time in microseconds, or -1 if unavailable.
	#[func]
	pub fn first_message_time_usec(&mut self) -> i64 {
		if self.ensure_summary().is_err() { return -1; }
		match &self.summary { Some(s) => s.stats.as_ref().map(|st| st.message_start_time as i64).unwrap_or(-1), None => -1 }
	}

	/// Last message log time in microseconds, or -1 if unavailable.
	#[func]
	pub fn last_message_time_usec(&mut self) -> i64 {
		if self.ensure_summary().is_err() { return -1; }
		match &self.summary { Some(s) => s.stats.as_ref().map(|st| st.message_end_time as i64).unwrap_or(-1), None => -1 }
	}

	/// Duration (end - start) in microseconds, or -1 if unavailable.
	#[func]
	pub fn duration_usec(&mut self) -> i64 {
		if self.ensure_summary().is_err() { return -1; }
		match &self.summary {
			Some(s) => {
				if let Some(st) = &s.stats { (st.message_end_time as i64) - (st.message_start_time as i64) } else { -1 }
			}
			None => -1,
		}
	}

	/// All channel IDs present in the file (unsorted).
	#[func]
	pub fn channel_ids(&mut self) -> PackedInt32Array {
		let mut arr = PackedInt32Array::new();
		if self.ensure_summary().is_err() { return arr; }
		if let Some(s) = &self.summary {
			for id in s.channels.keys() { arr.push(*id as i32); }
		}
		arr
	}

	/// All topic names present in the file (may contain duplicates across channels with different schemas).
	#[func]
	pub fn topic_names(&mut self) -> PackedStringArray {
		let mut arr = PackedStringArray::new();
		if self.ensure_summary().is_err() { return arr; }
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
		if self.ensure_summary().is_err() { return -1; }
		let t = topic.to_string();
		if let Some(s) = &self.summary {
			let mut best: Option<u16> = None;
			for (id, ch) in s.channels.iter() {
				if ch.topic == t {
					best = match best { Some(prev) => Some(prev.min(*id)), None => Some(*id) };
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
		if self.ensure_summary().is_err() { return arr; }
		let sid = if schema_id < 0 { return arr; } else { schema_id as u16 };
		if let Some(s) = &self.summary {
			for (id, ch) in s.channels.iter() {
				if let Some(schema_arc) = &ch.schema {
					if schema_arc.id == sid { arr.push(*id as i32); }
				}
			}
		}
		arr
	}

	/// Return the schema object used by a channel, if any.
	#[func]
	pub fn schema_for_channel(&mut self, channel_id: i32) -> Option<Gd<MCAPSchema>> {
		if self.ensure_summary().is_err() { return None; }
		let Some(s) = &self.summary else { return None; };
		let ch = match s.channels.get(&(channel_id as u16)) { Some(c) => c, None => return None };
		match &ch.schema {
			Some(schema_arc) => Some(MCAPSchema::from_mcap(schema_arc.as_ref())),
			None => None,
		}
	}

	// ----- Counts (fast using indexes) -----

	/// Total message count; uses summary stats when available, else sums per-chunk indexes. (also requires summary)
	#[func]
	pub fn message_count_total(&mut self) -> i64 {
		if self.ensure_summary().is_err() { return 0; }
		let Some(s) = &self.summary else { return 0; };
		if let Some(st) = &s.stats { return st.message_count as i64; }
		let mut total: i64 = 0;
		for chunk_idx in &s.chunk_indexes {
			match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (_ch, entries) in map.into_iter() { total += entries.len() as i64; }
				}
				Err(e) => {
					self.set_error(format!("message_count_total: read_message_indexes failed: {}", e));
					break;
				}
			}
		}
		total
	}

	/// Message count for a specific channel id.
	#[func]
	pub fn message_count_for_channel(&mut self, channel_id: i32) -> i64 {
		if self.ensure_summary().is_err() { return 0; }
		let Some(s) = &self.summary else { return 0; };
		let ch_id = if channel_id < 0 { return 0; } else { channel_id as u16 };
		let mut total: i64 = 0;
		for chunk_idx in &s.chunk_indexes {
			match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (ch, entries) in map.into_iter() {
						if ch.id == ch_id { total += entries.len() as i64; }
					}
				}
				Err(e) => {
					self.set_error(format!("message_count_for_channel: read_message_indexes failed: {}", e));
					break;
				}
			}
		}
		total
	}

	/// Message count across all channels within [start_usec, end_usec] inclusive.
	#[func]
	pub fn message_count_in_range(&mut self, start_usec: i64, end_usec: i64) -> i64 {
		if self.ensure_summary().is_err() { return 0; }
		let Some(s) = &self.summary else { return 0; };
		if end_usec < start_usec { return 0; }
		let start = if start_usec < 0 { 0u64 } else { start_usec as u64 };
		let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
		let mut total: i64 = 0;
		for chunk_idx in &s.chunk_indexes {
			if chunk_idx.message_start_time > end || chunk_idx.message_end_time < start { continue; }
			match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (_ch, entries) in map.into_iter() {
						if entries.is_empty() { continue; }
						let lo = match entries.binary_search_by(|e| e.log_time.cmp(&start)) { Ok(i) => i, Err(i) => i };
						let hi = match entries.binary_search_by(|e| e.log_time.cmp(&end)) { Ok(i) => i + 1, Err(i) => i };
						if hi > lo { total += (hi - lo) as i64; }
					}
				}
				Err(e) => {
					self.set_error(format!("message_count_in_range: read_message_indexes failed: {}", e));
					break;
				}
			}
		}
		total
	}

	/// Message count for a channel within [start_usec, end_usec] inclusive.
	#[func]
	pub fn message_count_for_channel_in_range(&mut self, channel_id: i32, start_usec: i64, end_usec: i64) -> i64 {
		if self.ensure_summary().is_err() { return 0; }
		let Some(s) = &self.summary else { return 0; };
		if end_usec < start_usec { return 0; }
		let ch_id = if channel_id < 0 { return 0; } else { channel_id as u16 };
		let start = if start_usec < 0 { 0u64 } else { start_usec as u64 };
		let end = if end_usec < 0 { 0u64 } else { end_usec as u64 };
		let mut total: i64 = 0;
		for chunk_idx in &s.chunk_indexes {
			if chunk_idx.message_start_time > end || chunk_idx.message_end_time < start { continue; }
			match s.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (ch, entries) in map.into_iter() {
						if ch.id != ch_id { continue; }
						if entries.is_empty() { continue; }
						let lo = match entries.binary_search_by(|e| e.log_time.cmp(&start)) { Ok(i) => i, Err(i) => i };
						let hi = match entries.binary_search_by(|e| e.log_time.cmp(&end)) { Ok(i) => i + 1, Err(i) => i };
						if hi > lo { total += (hi - lo) as i64; }
					}
				}
				Err(e) => {
					self.set_error(format!("message_count_for_channel_in_range: read_message_indexes failed: {}", e));
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
		self.buf = PackedByteArray::from(bytes);
		let _ = self.ensure_summary();
		true
	}

	fn ensure_summary(&mut self) -> Result<(), String> {
		if self.summary.is_some() { return Ok(()); }
		match Summary::read(self.buf.as_slice()) {
			Ok(opt) => { self.summary = opt; Ok(()) }
			Err(e) => {
				let msg = format!("Reading summary failed: {}", e);
				self.set_error(&msg);
				Err(msg)
			}
		}
	}

	fn opts_enumset(&self) -> enumset::EnumSet<Options> {
		let mut set = enumset::EnumSet::empty();
	if self.ignore_end_magic { set.insert(Options::IgnoreEndMagic); }
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

	fn message_index_entry_to_resource(&self, channel_id: u16, e: &mcap::records::MessageIndexEntry) -> Gd<MCAPMessageIndexEntry> {
		Gd::from_object(MCAPMessageIndexEntry {
			channel_id: channel_id as i32,
			log_time_usec: e.log_time as i64,
			offset_uncompressed: e.offset as i64,
		})
	}

	fn message_index_entry_from_resource(&self, e: &Gd<MCAPMessageIndexEntry>) -> mcap::records::MessageIndexEntry {
		let b = e.bind();
		mcap::records::MessageIndexEntry { log_time: b.log_time_usec as u64, offset: b.offset_uncompressed as u64 }
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
		for idx in &s.chunk_indexes { chunk_arr.push(&self.chunk_index_to_resource(idx)); }

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

// -------- Message iterator (streaming) --------

#[derive(GodotClass)]
/// Custom GDScript iterator for streaming MCAP messages using summary indexes.
///
/// Usage
/// - Obtain from `MCAPReader.stream_messages_iterator()` and iterate in GDScript:
///   `for msg in reader.stream_messages_iterator():`
///
/// Behavior
/// - Requires a Summary section; otherwise `_iter_init` returns false and iteration is empty.
/// - Iterates in log-time order, merging per-channel message indexes across chunks.
/// - Optional per-channel filtering via `for_channel(channel_id)` before iteration.
#[class(no_init, base=RefCounted)]
pub struct MCAPMessageIterator {
	// immutable input
	buf: PackedByteArray,
	filter_channel: Option<u16>,
	// iterator state
	index: i64,
	peek: Option<Gd<MCAPMessage>>, // next element ready for _iter_get
	summary: Option<Summary>,
	chunk_i: usize,
	// per-chunk buffered messages sorted by log_time
	chunk_msgs: Vec<(u64, Gd<MCAPMessage>)>,
	chunk_pos: usize,
}

impl MCAPMessageIterator {
	fn new_from_reader(reader: &MCAPReader, filter_channel: Option<u16>) -> Gd<Self> {
		Gd::from_object(Self {
			buf: reader.buf.clone(),
			filter_channel,
			index: 0,
			peek: None,
			summary: None,
			chunk_i: 0,
			chunk_msgs: Vec::new(),
			chunk_pos: 0,
		})
	}

	fn reset_iteration_state(&mut self) {
		self.index = 0;
		self.peek = None;
		self.chunk_i = 0;
		self.chunk_msgs.clear();
		self.chunk_pos = 0;
	}

	fn ensure_summary(&mut self) -> bool {
		if self.summary.is_none() {
			match Summary::read(self.buf.as_slice()) {
				Ok(opt) => self.summary = opt,
				Err(e) => {
					godot_error!("MCAPMessageIterator: reading summary failed: {}", e);
					self.summary = None;
				}
			}
		}
		self.summary.is_some()
	}

	fn prepare_next_chunk(&mut self) -> bool {
		let Some(summary) = &self.summary else { return false; };
		while self.chunk_i < summary.chunk_indexes.len() {
			let chunk_idx = &summary.chunk_indexes[self.chunk_i];
			self.chunk_msgs.clear();
			self.chunk_pos = 0;
			let filter = MsgFilter { time_start: None, time_end: None, channels: self.filter_channel.map(|id| { let mut s = HashSet::new(); s.insert(id); s }) };
			if let Err(e) = stream_chunk_apply(self.buf.as_slice(), summary, chunk_idx, &filter, |t, gd| self.chunk_msgs.push((t, gd))) {
				godot_error!("MCAPMessageIterator: {}", e);
			} else {
				self.chunk_msgs.sort_by_key(|(t, _)| *t);
				if !self.chunk_msgs.is_empty() { return true; }
			}
			self.chunk_i += 1;
		}
		false
	}

	fn next_message_internal(&mut self) -> Option<Gd<MCAPMessage>> {
		if !self.ensure_summary() { return None; }
		loop {
			if self.chunk_msgs.is_empty() {
				// Load first available chunk
				if !self.prepare_next_chunk() { return None; }
			}
			if self.chunk_pos >= self.chunk_msgs.len() {
				// Finished current chunk; move to next
				self.chunk_i += 1;
				if !self.prepare_next_chunk() { return None; }
			}
			if self.chunk_pos < self.chunk_msgs.len() {
				let msg = self.chunk_msgs[self.chunk_pos].1.clone();
				self.chunk_pos += 1;
				return Some(msg);
			}
		}
	}

	// Load chunk at index and position to first message with time >= t; if no such message in this chunk, advance to next non-empty chunk.
	fn load_and_seek_at_or_after(&mut self, chunk_index: usize, t: u64) -> bool {
		self.reset_iteration_state();
		self.chunk_i = chunk_index;
		if !self.prepare_next_chunk() { return false; }
		let pos = match self.chunk_msgs.binary_search_by_key(&t, |(lt, _)| *lt) { Ok(i) => i, Err(i) => i };
		if pos < self.chunk_msgs.len() {
			self.chunk_pos = pos;
			true
		} else {
			self.chunk_i += 1;
			if !self.prepare_next_chunk() { return false; }
			self.chunk_pos = 0;
			!self.chunk_msgs.is_empty()
		}
	}

	// Find the nearest message time at or before t and return (chunk_index, time).
	fn find_nearest_at_or_before(&self, t: u64) -> Option<(usize, u64)> {
		let summary = self.summary.as_ref()?;
		let bytes = self.buf.as_slice();
		let mut best: Option<(usize, u64)> = None;
		let filter = MsgFilter { time_start: None, time_end: Some(t), channels: self.filter_channel.map(|id| { let mut s = HashSet::new(); s.insert(id); s }) };
		for (i, chunk_idx) in summary.chunk_indexes.iter().enumerate() {
			if chunk_idx.message_start_time > t { break; }
			let _ = stream_chunk_apply(bytes, summary, chunk_idx, &filter, |time, _gd| {
				if best.map(|(_, bt)| time > bt).unwrap_or(true) { best = Some((i, time)); }
			});
		}
		best
	}
}

#[godot_api]
impl MCAPMessageIterator {
	/// Filter to only a specific channel id
	#[func]
	pub fn for_channel(&mut self, channel_id: i32) {
		self.filter_channel = Some(channel_id as u16);
	self.reset_iteration_state();
	}

	/// Godot iterator protocol: initialize and prepare first value.
	#[func]
	pub fn _iter_init(&mut self, _iter: Array<Variant>) -> bool {
	self.reset_iteration_state();
	self.peek = self.next_message_internal();
		self.peek.is_some()
	}

	/// Godot iterator protocol: advance and report if another value exists.
	#[func]
	pub fn _iter_next(&mut self, _iter: Array<Variant>) -> bool {
	self.index += 1;
	self.peek = self.next_message_internal();
		self.peek.is_some()
	}

	/// Godot iterator protocol: return current value.
	#[func]
	pub fn _iter_get(&self, _value: Variant) -> Variant {
		match &self.peek {
			Some(gd) => Variant::from(gd.clone()),
			None => Variant::nil(),
		}
	}

	/// Reset iterator to the start, clearing any peeked value and state.
	#[func]
	pub fn rewind(&mut self) {
		self.reset_iteration_state();
	}

	/// Remove any channel filter and reset iteration.
	#[func]
	pub fn clear_filter(&mut self) {
		self.filter_channel = None;
		self.reset_iteration_state();
	}

	/// Return the number of messages yielded so far.
	#[func]
	pub fn current_index(&self) -> i64 {
		self.index
	}

	/// Seek iterator to the first message with log_time >= given timestamp (microseconds).
	/// Returns true if positioned on or before a valid next message.
	#[func]
	pub fn seek_to_time(&mut self, log_time_usec: i64) -> bool {
		if !self.ensure_summary() { return false; }
		let t: u64 = if log_time_usec < 0 { 0 } else { log_time_usec as u64 };
		let ci = {
			let summary = match &self.summary { Some(s) => s, None => return false };
			let mut found: Option<usize> = None;
			for (i, ch) in summary.chunk_indexes.iter().enumerate() {
				if ch.message_end_time >= t { found = Some(i); break; }
			}
			match found { Some(i) => i, None => return false }
		};
		self.load_and_seek_at_or_after(ci, t)
	}

	/// Seek to the first message at or after time; if none exists, position to nearest at or before.
	#[func]
	pub fn seek_to_time_nearest(&mut self, log_time_usec: i64) -> bool {
		if self.seek_to_time(log_time_usec) { return true; }
		if !self.ensure_summary() { return false; }
		let t: u64 = if log_time_usec < 0 { 0 } else { log_time_usec as u64 };
		let (ci, start_time) = match self.find_nearest_at_or_before(t) { Some((i, time)) => (i, time), None => return false };
		self.load_and_seek_at_or_after(ci, start_time)
	}

	/// Seek to the first message on the given channel strictly after after_time_usec.
	#[func]
	pub fn seek_to_next_on_channel(&mut self, channel_id: i32, after_time_usec: i64) -> bool {
		if !self.ensure_summary() { return false; }
		if channel_id < 0 { return false; }
		let ch_id = channel_id as u16;
		let t: u64 = if after_time_usec < 0 { 0 } else { after_time_usec as u64 };
		// Scan for earliest message strictly after t on the given channel
		let summary = match &self.summary { Some(s) => s, None => return false };
		let bytes = self.buf.as_slice();
		let mut found: Option<(usize, u64)> = None;
		let filter = MsgFilter { time_start: Some(t.saturating_add(1)), time_end: None, channels: Some({ let mut s = HashSet::new(); s.insert(ch_id); s }) };
		for (i, chunk_idx) in summary.chunk_indexes.iter().enumerate() {
			if chunk_idx.message_end_time <= t { continue; }
			let mut best_in_chunk: Option<u64> = None;
			let _ = stream_chunk_apply(bytes, summary, chunk_idx, &filter, |time, _gd| {
				if best_in_chunk.map(|bt| time < bt).unwrap_or(true) { best_in_chunk = Some(time); }
			});
			if let Some(start_time) = best_in_chunk { found = Some((i, start_time)); break; }
		}
		if let Some((ci, start_time)) = found { return self.load_and_seek_at_or_after(ci, start_time); }
		false
	}

	/// Get the message at an exact log time for a given channel, if present.
	#[func]
	pub fn get_message_at_time(&mut self, channel_id: i32, log_time_usec: i64) -> Option<Gd<MCAPMessage>> {
		if !self.ensure_summary() { return None; }
		if channel_id < 0 { return None; }
		let ch_id = channel_id as u16;
		let t: u64 = if log_time_usec < 0 { 0 } else { log_time_usec as u64 };
		let summary = self.summary.as_ref()?;
		for chunk_idx in &summary.chunk_indexes {
			if t < chunk_idx.message_start_time || t > chunk_idx.message_end_time { continue; }
			match summary.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (ch, entries) in map.into_iter() {
						if ch.id != ch_id { continue; }
						if let Ok(pos) = entries.binary_search_by(|e| e.log_time.cmp(&t)) {
							let entry = &entries[pos];
							match summary.seek_message(self.buf.as_slice(), chunk_idx, entry) {
								Ok(msg) => return Some(MCAPMessage::from_mcap(&msg)),
								Err(e) => { godot_error!("get_message_at_time: seek_message failed: {}", e); return None; }
							}
						}
					}
				}
				Err(e) => { godot_error!("get_message_at_time: read_message_indexes failed: {}", e); return None; }
			}
		}
		None
	}

	/// Check if another message is available without consuming it.
	#[func]
	pub fn has_next_message(&mut self) -> bool {
		if self.peek.is_none() {
			self.peek = self.next_message_internal();
		}
		self.peek.is_some()
	}

	/// Fetch and advance to the next message; returns null if none.
	#[func]
	pub fn get_next_message(&mut self) -> Option<Gd<MCAPMessage>> {
		if self.peek.is_none() {
			self.peek = self.next_message_internal();
		}
		match self.peek.take() {
			Some(gd) => {
				self.index += 1;
				Some(gd)
			}
			None => None,
		}
	}

	/// Return, without consuming, the next message if available.
	#[func]
	pub fn peek_message(&mut self) -> Option<Gd<MCAPMessage>> {
		if self.peek.is_none() {
			self.peek = self.next_message_internal();
		}
		self.peek.clone()
	}
}

