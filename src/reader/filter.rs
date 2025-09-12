use crate::types::*;
use godot::prelude::*;
use mcap::read::Summary;
use std::collections::HashSet;

// Reusable message filter for time range and channel sets
pub(super) struct MsgFilter {
    pub time_start: Option<u64>,
    pub time_end: Option<u64>,
    pub channels: Option<HashSet<u16>>, // if None: accept all
}

impl MsgFilter {
    #[inline]
    pub fn matches_time(&self, t: u64) -> bool {
        if let Some(s) = self.time_start {
            if t < s {
                return false;
            }
        }
        if let Some(e) = self.time_end {
            if t > e {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn matches_ch(&self, id: u16) -> bool {
        match &self.channels {
            Some(set) => set.contains(&id),
            None => true,
        }
    }

    #[inline]
    pub fn chunk_might_match(&self, idx: &mcap::records::ChunkIndex) -> bool {
        if let Some(s) = self.time_start {
            if idx.message_end_time < s {
                return false;
            }
        }
        if let Some(e) = self.time_end {
            if idx.message_start_time > e {
                return false;
            }
        }
        true
    }
}

// Shared helper: stream a chunk, apply filter, build MCAPMessage, and call a closure with (log_time, message)
pub(super) fn stream_chunk_apply<F>(
    bytes: &[u8],
    summary: &Summary,
    chunk_idx: &mcap::records::ChunkIndex,
    filter: &MsgFilter,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(u64, Gd<MCAPMessage>),
{
    let iter = summary
        .stream_chunk(bytes, chunk_idx)
        .map_err(|e| format!("stream_chunk open failed: {}", e))?;
    for item in iter {
        match item {
            Ok(msg) => {
                if !filter.matches_time(msg.log_time) {
                    continue;
                }
                if !filter.matches_ch(msg.channel.id) {
                    continue;
                }
                let gd = MCAPMessage::from_mcap(&msg);
                f(msg.log_time, gd);
            }
            Err(e) => return Err(format!("stream_chunk failed: {}", e)),
        }
    }
    Ok(())
}
