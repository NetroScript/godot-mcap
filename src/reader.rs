use crate::types::*;
use godot::classes::file_access::ModeFlags;
use godot::prelude::*;
use godot::tools::GFile;
use mcap::read::{footer as mcap_footer, MessageStream, Options, RawMessage, RawMessageStream, Summary};
use std::borrow::Cow;
use std::io::Read;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

#[derive(GodotClass)]
/// Godot-facing MCAP reader with streaming and indexed helpers.
///
/// Memory & I/O
/// - Opening a file loads the entire MCAP file into a PackedByteArray in memory for fast random access.
/// - Streaming APIs (`stream_messages`, `stream_raw_messages`) iterate without requiring a summary.
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

	/// Streams all messages as Godot `MCAPMessage` resources (allocates payloads as needed).
	/// Stops automatically before the summary section.
	#[func]
	pub fn stream_messages(&mut self) -> Array<Gd<MCAPMessage>> {
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

	/// Iterator version of stream_messages() for GDScript `for` loops.
	///
	/// Details
	/// - Requires a Summary section (uses chunk/message indexes for efficient seeking).
	/// - For files without a summary, this iterator will be empty; use `stream_messages()` instead.
	#[func]
	pub fn stream_messages_iterator(&self) -> Gd<MCAPMessageIterator> {
		MCAPMessageIterator::new_from_reader(self, None)
	}

	/// Streams all raw messages (header + bytes) without constructing channels into Godot resources.
	/// Returns an array of Dictionaries: { header: MCAPMessageHeader, data: PackedByteArray }.
	#[func]
	pub fn stream_raw_messages(&mut self) -> Array<Dictionary> {
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
	// per-chunk merged iteration state
	per_channel: HashMap<u16, Vec<mcap::records::MessageIndexEntry>>,
	heap: BinaryHeap<Reverse<(u64, u16, usize)>>, // (log_time, channel_id, idx)
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
			per_channel: HashMap::new(),
			heap: BinaryHeap::new(),
		})
	}

	fn reset_iteration_state(&mut self) {
		self.index = 0;
		self.peek = None;
		self.chunk_i = 0;
		self.per_channel.clear();
		self.heap.clear();
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
			self.per_channel.clear();
			self.heap.clear();

			match summary.read_message_indexes(self.buf.as_slice(), chunk_idx) {
				Ok(map) => {
					for (ch, entries) in map.into_iter() {
						let ch_id = ch.id;
						if let Some(filter) = self.filter_channel { if ch_id != filter { continue; } }
						if entries.is_empty() { continue; }
						self.per_channel.insert(ch_id, entries);
					}
					// seed heap with first entry from each channel
					for (&ch_id, entries) in self.per_channel.iter() {
						let e = &entries[0];
						self.heap.push(Reverse((e.log_time, ch_id, 0usize)));
					}
					if !self.heap.is_empty() {
						return true;
					}
				}
				Err(e) => {
					godot_error!("MCAPMessageIterator: read_message_indexes failed: {}", e);
				}
			}
			// advance to next chunk and try again
			self.chunk_i += 1;
		}
		false
	}

	fn next_message_internal(&mut self) -> Option<Gd<MCAPMessage>> {
		if !self.ensure_summary() { return None; }
		loop {
			if self.heap.is_empty() {
				// If we previously had a chunk, advance to next
				if !self.per_channel.is_empty() {
					self.chunk_i += 1;
					self.per_channel.clear();
				}
				// move to next chunk with data
				if !self.prepare_next_chunk() { return None; }
			}
			// safe to unwrap after prepare
			if let Some(Reverse((_, ch_id, idx))) = self.heap.pop() {
				// fetch entry
				let entries = self.per_channel.get(&ch_id).expect("channel entries exist");
				let entry = &entries[idx];
				let chunk_idx = &self.summary.as_ref().unwrap().chunk_indexes[self.chunk_i];
				// seek message
				match self.summary.as_ref().unwrap().seek_message(self.buf.as_slice(), chunk_idx, entry) {
					Ok(msg) => {
						// push next from same channel if exists
						let next_idx = idx + 1;
						if next_idx < entries.len() {
							let next_entry = &entries[next_idx];
							self.heap.push(Reverse((next_entry.log_time, ch_id, next_idx)));
						} else {
							// done with this channel for this chunk
						}
						return Some(MCAPMessage::from_mcap(&msg));
					}
					Err(e) => {
						godot_error!("MCAPMessageIterator: seek_message failed: {}", e);
						// try continue with next
						continue;
					}
				}
			} else {
				// Should not happen due to is_empty() check
				continue;
			}
		}
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
}

