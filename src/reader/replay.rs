use crate::reader::iterator::MCAPMessageIterator;
use crate::reader::mcap_reader::MCAPReader;
use crate::types::*;
use godot::classes::notify::NodeNotification;
use godot::prelude::*;
use std::collections::HashSet;
use std::time::Instant;

#[derive(GodotConvert, Var, Export, PartialEq, Debug)]
#[godot(via = i64)]
/// Update mode used by [MCAPReplay] to advance playback time.
///
/// - IDLE: uses `_process(delta)` updates.
/// - PHYSICS: uses `_physics_process(delta)` updates.
pub enum ProcessingMode {
    /// Use `_process(delta)` for timing updates (default).
    IDLE,
    /// Use `_physics_process(delta)` for timing updates.
    PHYSICS,
}

#[derive(GodotClass)]
/// Node that replays MCAP messages from an [MCAPReader] in log-time order.
///
/// Overview
/// - Streams messages in the same order and relative timing as recorded in the MCAP file.
/// - Behaves similarly to Godot's Timer by managing internal processing (idle/physics) while running.
/// - Optional channel filter and inclusive time range.
/// - Supports playback speed (time scaling), seeking, and looping.
/// - It is assumed that timestamps are in microseconds (usec).
///
/// Properties
/// - `speed: float` — Time scale (1.0 = real-time, 2.0 = double speed, 0.5 = half speed). Minimum 0.0 (clamped to 1.0 if <= 0).
/// - `looping: bool` — If true, restarts playback upon reaching the end of the selected time range or data.
/// - `processing_mode: ProcessingMode` — Whether to advance time in idle or physics.
///
/// Signals
/// - `message(MCAPMessage msg)` — Emitted each time a message becomes due according to the current logical replay time.
///
/// Basic usage (GDScript)
/// ```gdscript
/// var reader := MCAPReader.open("res://capture.mcap", false)
/// var replay := MCAPReplay.new()
/// add_child(replay)
/// replay.set_reader(reader)
/// replay.set_time_range(-1, -1) # play full recording
/// replay.speed = 1.0
/// replay.looping = false
/// replay.processing_mode = MCAPReplay.PROCESSING_MODE_IDLE
/// replay.message.connect(_on_replay_message)
/// var ok := replay.start()
/// if not ok:
///     push_error("MCAPReplay failed to start (missing summary or no data)")
/// ```
///
/// Notes
/// - Requires a Summary section; if missing, `start()` returns false and no messages are emitted.
/// - When `looping` is enabled, the replay restarts at `set_time_range()` start (if set) or at the file's first message time.
/// - `current_time_usec()` returns the logical replay time = start time + elapsed real time × `speed`, clamped to `time_end` when set.
/// - Channel filters with a single channel are optimized internally; multiple channels are filtered while iterating.
/// - All times are in microseconds (usec).
#[class(init, base=Node)]
pub struct MCAPReplay {
    // immutable input
    reader: Option<Gd<MCAPReader>>,
    filter_channels: Option<HashSet<u16>>,
    time_start: Option<u64>,
    time_end: Option<u64>,
    // replay state
    running: bool,
    #[export(range = (0.0, 10.0, or_greater))]
    #[var(set = set_speed)]
    /// Playback speed (time scale); minimum 0.0 (clamped to 1.0 if <= 0).
    speed: f64,
    #[export]
    /// If true, restarts playback upon reaching the end of the selected time range or data.
    looping: bool,
    #[init(val = ProcessingMode::IDLE)]
    #[export]
    #[var(set = set_processing_mode)]
    /// Whether to advance time in the _process (idle) or _physics_process (physics) callback.
    processing_mode: ProcessingMode,
    iter: Option<Gd<MCAPMessageIterator>>,
    start_real_time: Option<Instant>,
    start_log_time: Option<u64>,
    base: Base<Node>,
}

impl MCAPReplay {
    fn update_replay(&mut self) {
        if !self.running {
            return;
        }
        let Some(start_rt) = self.start_real_time else {
            return;
        };
        let Some(start_lt) = self.start_log_time else {
            return;
        };

        // Compute target log-time based on elapsed real time and speed
        let elapsed = start_rt.elapsed();
        let elapsed_us = (elapsed.as_secs_f64() * 1_000_000.0 * self.speed) as u64;
        let mut target = start_lt.saturating_add(elapsed_us);
        if let Some(t_end) = self.time_end {
            if target > t_end {
                target = t_end;
            }
        }

        // Collect messages to emit up to target time
        let mut to_emit: Vec<Gd<MCAPMessage>> = Vec::new();
        enum EndAction {
            None,
            Stop,
            Restart,
        }
        let mut action = EndAction::None;

        loop {
            let next_opt = {
                match self.iter.as_mut() {
                    Some(it) => it.bind_mut().peek_message(),
                    None => None,
                }
            };
            let Some(next) = next_opt else {
                // End of stream
                if self.looping {
                    action = EndAction::Restart;
                } else {
                    action = EndAction::Stop;
                }
                break;
            };

            // Check end/time bounds and channel filter
            let nb = next.bind();
            let msg_time = nb.log_time as u64;
            drop(nb);

            if let Some(t_end) = self.time_end {
                if msg_time > t_end {
                    action = if self.looping {
                        EndAction::Restart
                    } else {
                        EndAction::Stop
                    };
                    break;
                }
            }

            if msg_time <= target {
                // Channel filter (optional multi-channel)
                if let Some(chset) = &self.filter_channels {
                    let ch_id = next.bind().channel.bind().id;
                    if !chset.contains(&ch_id) {
                        // consume and skip
                        let _ = {
                            if let Some(it) = self.iter.as_mut() {
                                it.bind_mut().get_next_message()
                            } else {
                                None
                            }
                        };
                        continue;
                    }
                }

                // consume and emit
                let msg_opt = {
                    if let Some(it) = self.iter.as_mut() {
                        it.bind_mut().get_next_message()
                    } else {
                        None
                    }
                };
                if let Some(msg) = msg_opt {
                    to_emit.push(msg);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        // Now emit outside of iterator borrows
        for msg in to_emit.into_iter() {
            self.signals().message().emit(&msg);
        }

        match action {
            EndAction::Restart => self.restart_from_range_start(),
            EndAction::Stop => self.stop(),
            EndAction::None => {}
        }
    }

    fn apply_process_state(&mut self) {
        let running = self.running;
        match self.processing_mode {
            ProcessingMode::IDLE => {
                self.base_mut().set_process_internal(running);
                self.base_mut().set_physics_process_internal(false);
            }
            ProcessingMode::PHYSICS => {
                self.base_mut().set_process_internal(false);
                self.base_mut().set_physics_process_internal(running);
            }
        }
    }

    fn setup_iterator(&mut self, start_time: Option<u64>) -> bool {
        let reader = match &self.reader {
            Some(r) => r.clone(),
            None => return false,
        };
        // Build a fresh iterator from reader
        let mut it = reader.bind().stream_messages_iterator();
        // Fast-path single-channel filter
        if let Some(set) = &self.filter_channels {
            if set.len() == 1 {
                if let Some(&cid) = set.iter().next() {
                    it.bind_mut().for_channel(cid as i32);
                }
            }
        }
        if let Some(t) = start_time {
            let _ = it.bind_mut().seek_to_time(t as i64);
        }
        self.iter = Some(it);
        true
    }

    fn restart_from_range_start(&mut self) {
        // Determine new logical start time: explicit time_start or first available
        let mut start_t: u64 = 0;
        if let Some(s) = self.time_start {
            start_t = s;
        } else if let Some(r) = &self.reader {
            start_t = r.clone().bind_mut().first_message_time_usec().max(0) as u64;
        }
        if !self.setup_iterator(Some(start_t)) {
            self.stop();
            return;
        }
        self.start_log_time = Some(start_t);
        self.start_real_time = Some(Instant::now());
        self.running = true;
        self.apply_process_state();
    }
}

#[godot_api]
impl INode for MCAPReplay {
    fn on_notification(&mut self, what: NodeNotification) {
        if what == NodeNotification::INTERNAL_PROCESS
            && self.processing_mode == ProcessingMode::IDLE
        {
            self.update_replay();
        } else if what == NodeNotification::INTERNAL_PHYSICS_PROCESS
            && self.processing_mode == ProcessingMode::PHYSICS
        {
            self.update_replay();
        }
    }
}

#[godot_api]
impl MCAPReplay {
    /// Emitted when a message becomes due for replay.
    #[signal]
    pub fn message(msg: Gd<MCAPMessage>);

    /// Processing mode constant for idle updates.
    #[constant]
    const PROCESSING_MODE_IDLE: i64 = ProcessingMode::IDLE as i64;
    /// Processing mode constant for physics updates.
    #[constant]
    const PROCESSING_MODE_PHYSICS: i64 = ProcessingMode::PHYSICS as i64;

    // --- Configuration API ---

    /// Set the reader used for replay. Resets iterator.
    #[func]
    pub fn set_reader(&mut self, reader: Gd<MCAPReader>) {
        self.reader = Some(reader);
        self.iter = None;
    }

    /// Clear the reader.
    #[func]
    pub fn clear_reader(&mut self) {
        self.stop();
        self.reader = None;
        self.iter = None;
    }

    /// Filter to a set of channel IDs. Pass an empty array to accept all.
    #[func]
    pub fn set_filter_channels(&mut self, channel_ids: PackedInt32Array) {
        let mut set: HashSet<u16> = HashSet::new();
        for i in 0..channel_ids.len() {
            if let Some(v) = channel_ids.get(i) {
                if v >= 0 {
                    let _ = set.insert(v as u16);
                }
            }
        }
        self.filter_channels = if set.is_empty() { None } else { Some(set) };
        // Rebuild iterator at current logical time if running
        if self.running {
            let now = self.current_time_usec();
            self.setup_iterator(Some(now.max(0) as u64));
            self.start_log_time = Some(now.max(0) as u64);
            self.start_real_time = Some(Instant::now());
        }
    }

    /// Clear channel filter.
    #[func]
    pub fn clear_filter_channels(&mut self) {
        self.filter_channels = None;
        if self.running {
            let now = self.current_time_usec();
            self.setup_iterator(Some(now.max(0) as u64));
            self.start_log_time = Some(now.max(0) as u64);
            self.start_real_time = Some(Instant::now());
        }
    }

    /// Set an inclusive time range filter in microseconds. Use -1 to clear a bound.
    #[func]
    pub fn set_time_range(&mut self, start_usec: i64, end_usec: i64) {
        self.time_start = if start_usec >= 0 {
            Some(start_usec as u64)
        } else {
            None
        };
        self.time_end = if end_usec >= 0 {
            Some(end_usec as u64)
        } else {
            None
        };
        if self.running {
            self.restart_from_range_start();
        }
    }

    /// Start replay. If a time range start is set, starts from there, else from file's first message time.
    #[func]
    pub fn start(&mut self) -> bool {
        if self.reader.is_none() {
            return false;
        }
        // Determine start time
        let mut start_t: u64 = 0;
        if let Some(s) = self.time_start {
            start_t = s;
        } else if let Some(r) = &self.reader {
            start_t = r.clone().bind_mut().first_message_time_usec().max(0) as u64;
        }
        if !self.setup_iterator(Some(start_t)) {
            return false;
        }
        self.start_log_time = Some(start_t);
        self.start_real_time = Some(Instant::now());
        self.running = true;
        self.apply_process_state();
        true
    }

    /// Stop replay and disable processing.
    #[func]
    pub fn stop(&mut self) {
        self.running = false;
        self.apply_process_state();
        self.iter = None;
        self.start_real_time = None;
        self.start_log_time = None;
    }

    /// Seek to a specific log time (microseconds) and continue replay from there.
    #[func]
    pub fn seek_to_time(&mut self, log_time_usec: i64) -> bool {
        let t = if log_time_usec < 0 {
            0
        } else {
            log_time_usec as u64
        };
        if self.iter.is_none() && !self.setup_iterator(Some(t)) {
            return false;
        }
        if let Some(it) = &mut self.iter {
            if !it.bind_mut().seek_to_time(log_time_usec) {
                return false;
            }
        }
        self.start_log_time = Some(t);
        self.start_real_time = Some(Instant::now());
        true
    }

    /// Return whether replay is currently running.
    #[func]
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Get the current logical replay time in microseconds. Returns -1 if not started.
    #[func]
    pub fn current_time_usec(&self) -> i64 {
        match (self.start_log_time, self.start_real_time) {
            (Some(sl), Some(sr)) => {
                let elapsed_us = (sr.elapsed().as_secs_f64() * 1_000_000.0 * self.speed) as i64;
                let mut cur = sl as i64 + elapsed_us;
                if let Some(e) = self.time_end {
                    if cur as u64 > e {
                        cur = e as i64;
                    }
                }
                cur
            }
            _ => -1,
        }
    }

    /// Set playback speed (1.0 = real-time, 2.0 = double speed, etc.).
    #[func]
    pub fn set_speed(&mut self, speed: f64) {
        self.speed = if speed <= 0.0 { 1.0 } else { speed };
    }

    /// Set processing mode to use either idle or physics ticks.
    #[func]
    pub fn set_processing_mode(&mut self, mode: ProcessingMode) {
        self.processing_mode = mode;
        self.apply_process_state();
    }
}
