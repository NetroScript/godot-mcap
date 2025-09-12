use crate::reader::buf::SharedBuf;
use crate::reader::filter::{stream_chunk_apply, MsgFilter};
use crate::reader::mcap_reader::MCAPReader;
use crate::types::*;
use godot::prelude::*;
use mcap::read::Summary;
use std::collections::HashSet;

#[derive(GodotClass)]
/// Iterator for streaming MCAP messages using summary indexes.
///
/// Overview
/// - Obtained from `MCAPReader.stream_messages_iterator()`.
/// - Iterates messages in log-time order across chunks and channels.
/// - Supports optional per-channel filtering and multiple seek helpers.
/// - Requires a Summary section in the file.
///
/// Usage (GDScript)
/// ```gdscript
/// var it := reader.stream_messages_iterator()
/// # Optionally restrict to a channel id:
/// it.for_channel(42)
///
/// # Simple for-in iteration:
/// for msg in it:
///     print(msg.log_time, msg.channel.topic)
///
/// # Random access helpers:
/// it.seek_to_time(1_500_000) # position to first message at/after 1.5s
/// var ok := it.seek_to_time_nearest(500_000) # nearest at or before 0.5s if none after
/// var ok2 := it.seek_to_next_on_channel(42, 2_000_000) # first message on ch 42 after 2.0s
/// var exact := it.get_message_at_time(42, 2_500_000) # message on ch 42 exactly at time, if any
///
/// # Manual stepping:
/// while it.has_next_message():
///     var msg := it.get_next_message()
///     if msg:
///         print("next:", msg.log_time)
/// ```
///
/// Notes
/// - The iterator buffers messages per-chunk and merges them by log_time.
/// - Using `for_channel()` before iteration applies an efficient filter for a single channel.
/// - All time values are microseconds (usec).
#[class(no_init, base=RefCounted)]
pub struct MCAPMessageIterator {
    // immutable input
    pub(super) buf: SharedBuf,
    filter_channel: Option<u16>,
    // iterator state
    index: i64,
    peek: Option<Gd<MCAPMessage>>, // next element ready for _iter_get
    pub(super) summary: Option<Summary>,
    chunk_i: usize,
    // per-chunk buffered messages sorted by log_time
    chunk_msgs: Vec<(u64, Gd<MCAPMessage>)>,
    chunk_pos: usize,
}

impl MCAPMessageIterator {
    pub(super) fn new_from_reader(reader: &MCAPReader, filter_channel: Option<u16>) -> Gd<Self> {
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
        let Some(summary) = &self.summary else {
            return false;
        };
        while self.chunk_i < summary.chunk_indexes.len() {
            let chunk_idx = &summary.chunk_indexes[self.chunk_i];
            self.chunk_msgs.clear();
            self.chunk_pos = 0;
            let filter = MsgFilter {
                time_start: None,
                time_end: None,
                channels: self.filter_channel.map(|id| {
                    let mut s = HashSet::new();
                    s.insert(id);
                    s
                }),
            };
            if let Err(e) =
                stream_chunk_apply(self.buf.as_slice(), summary, chunk_idx, &filter, |t, gd| {
                    self.chunk_msgs.push((t, gd))
                })
            {
                godot_error!("MCAPMessageIterator: {}", e);
            } else {
                self.chunk_msgs.sort_by_key(|(t, _)| *t);
                if !self.chunk_msgs.is_empty() {
                    return true;
                }
            }
            self.chunk_i += 1;
        }
        false
    }

    fn next_message_internal(&mut self) -> Option<Gd<MCAPMessage>> {
        if !self.ensure_summary() {
            return None;
        }
        loop {
            if self.chunk_msgs.is_empty() {
                // Load first available chunk
                if !self.prepare_next_chunk() {
                    return None;
                }
            }
            if self.chunk_pos >= self.chunk_msgs.len() {
                // Finished current chunk; move to next
                self.chunk_i += 1;
                if !self.prepare_next_chunk() {
                    return None;
                }
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
        if !self.prepare_next_chunk() {
            return false;
        }
        let pos = match self.chunk_msgs.binary_search_by_key(&t, |(lt, _)| *lt) {
            Ok(i) => i,
            Err(i) => i,
        };
        if pos < self.chunk_msgs.len() {
            self.chunk_pos = pos;
            true
        } else {
            self.chunk_i += 1;
            if !self.prepare_next_chunk() {
                return false;
            }
            self.chunk_pos = 0;
            !self.chunk_msgs.is_empty()
        }
    }

    // Find the nearest message time at or before t and return (chunk_index, time).
    fn find_nearest_at_or_before(&self, t: u64) -> Option<(usize, u64)> {
        let summary = self.summary.as_ref()?;
        let bytes = self.buf.as_slice();
        let mut best: Option<(usize, u64)> = None;
        let filter = MsgFilter {
            time_start: None,
            time_end: Some(t),
            channels: self.filter_channel.map(|id| {
                let mut s = HashSet::new();
                s.insert(id);
                s
            }),
        };
        for (i, chunk_idx) in summary.chunk_indexes.iter().enumerate() {
            if chunk_idx.message_start_time > t {
                break;
            }
            let _ = stream_chunk_apply(bytes, summary, chunk_idx, &filter, |time, _gd| {
                if best.map(|(_, bt)| time > bt).unwrap_or(true) {
                    best = Some((i, time));
                }
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
        if !self.ensure_summary() {
            return false;
        }
        let t: u64 = if log_time_usec < 0 {
            0
        } else {
            log_time_usec as u64
        };
        let ci = {
            let summary = match &self.summary {
                Some(s) => s,
                None => return false,
            };
            let mut found: Option<usize> = None;
            for (i, ch) in summary.chunk_indexes.iter().enumerate() {
                if ch.message_end_time >= t {
                    found = Some(i);
                    break;
                }
            }
            match found {
                Some(i) => i,
                None => return false,
            }
        };
        self.load_and_seek_at_or_after(ci, t)
    }

    /// Seek to the first message at or after time; if none exists, position to nearest at or before.
    #[func]
    pub fn seek_to_time_nearest(&mut self, log_time_usec: i64) -> bool {
        if self.seek_to_time(log_time_usec) {
            return true;
        }
        if !self.ensure_summary() {
            return false;
        }
        let t: u64 = if log_time_usec < 0 {
            0
        } else {
            log_time_usec as u64
        };
        let (ci, start_time) = match self.find_nearest_at_or_before(t) {
            Some((i, time)) => (i, time),
            None => return false,
        };
        self.load_and_seek_at_or_after(ci, start_time)
    }

    /// Seek to the first message on the given channel strictly after after_time_usec.
    #[func]
    pub fn seek_to_next_on_channel(&mut self, channel_id: i32, after_time_usec: i64) -> bool {
        if !self.ensure_summary() {
            return false;
        }
        if channel_id < 0 {
            return false;
        }
        let ch_id = channel_id as u16;
        let t: u64 = if after_time_usec < 0 {
            0
        } else {
            after_time_usec as u64
        };
        // Scan for earliest message strictly after t on the given channel
        let summary = match &self.summary {
            Some(s) => s,
            None => return false,
        };
        let bytes = self.buf.as_slice();
        let mut found: Option<(usize, u64)> = None;
        let filter = MsgFilter {
            time_start: Some(t.saturating_add(1)),
            time_end: None,
            channels: Some({
                let mut s = HashSet::new();
                s.insert(ch_id);
                s
            }),
        };
        for (i, chunk_idx) in summary.chunk_indexes.iter().enumerate() {
            if chunk_idx.message_end_time <= t {
                continue;
            }
            let mut best_in_chunk: Option<u64> = None;
            let _ = stream_chunk_apply(bytes, summary, chunk_idx, &filter, |time, _gd| {
                if best_in_chunk.map(|bt| time < bt).unwrap_or(true) {
                    best_in_chunk = Some(time);
                }
            });
            if let Some(start_time) = best_in_chunk {
                found = Some((i, start_time));
                break;
            }
        }
        if let Some((ci, start_time)) = found {
            return self.load_and_seek_at_or_after(ci, start_time);
        }
        false
    }

    /// Get the message at an exact log time for a given channel, if present.
    #[func]
    pub fn get_message_at_time(
        &mut self,
        channel_id: i32,
        log_time_usec: i64,
    ) -> Option<Gd<MCAPMessage>> {
        if !self.ensure_summary() {
            return None;
        }
        if channel_id < 0 {
            return None;
        }
        let ch_id = channel_id as u16;
        let t: u64 = if log_time_usec < 0 {
            0
        } else {
            log_time_usec as u64
        };
        let summary = self.summary.as_ref()?;
        for chunk_idx in &summary.chunk_indexes {
            if t < chunk_idx.message_start_time || t > chunk_idx.message_end_time {
                continue;
            }
            match summary.read_message_indexes(self.buf.as_slice(), chunk_idx) {
                Ok(map) => {
                    for (ch, entries) in map.into_iter() {
                        if ch.id != ch_id {
                            continue;
                        }
                        if let Ok(pos) = entries.binary_search_by(|e| e.log_time.cmp(&t)) {
                            let entry = &entries[pos];
                            match summary.seek_message(self.buf.as_slice(), chunk_idx, entry) {
                                Ok(msg) => return Some(MCAPMessage::from_mcap(&msg)),
                                Err(e) => {
                                    godot_error!("get_message_at_time: seek_message failed: {}", e);
                                    return None;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    godot_error!("get_message_at_time: read_message_indexes failed: {}", e);
                    return None;
                }
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