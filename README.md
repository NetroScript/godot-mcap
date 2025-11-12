# godot-mcap — MCAP reader/writer for Godot 4

This project adds MCAP bindings to GDScript using a native GDExtension written in Rust.

> MCAP is an open source container file format for multimodal log data. It supports multiple channels of timestamped pre-serialized data, and is ideal for use in pub/sub or robotics applications. ~ [mcap.dev](https://mcap.dev)

It lets you:
- Write MCAP files from GDScript
- Read MCAP files (stream or indexed) and access channels, schemas, metadata, and attachments
- Iterate messages efficiently (with seeking and channel/time filters)
- Replay messages in real-time with a dedicated Node

This extension is built with godot-rust and targets Godot >=4.3 APIs.


## Features

- Writer
	- Add schemas, channels, attachments, metadata
	- Write full messages or header+payload to known channels
	- Chunking and compression (Zstd and/or LZ4 when enabled at build time)
	- Timestamp offset for aligning engine-relative clocks
- Reader
	- Direct message streaming without indexes
	- Indexed queries when a Summary is present (time windows, per-channel, counts)
	- Attachments and metadata access via summary indexes
	- Zero-copy mmap when possible, otherwise fallback to the FileAccess API (so supports reading files from `res://` and `user://`)
- Iterator and replay
	- `MCAPMessageIterator` for efficient for-in iteration with seeks and filters
	- `MCAPReplay` Node to emit messages over time (idle or physics), with speed/looping
- Godot-friendly Resources for common MCAP types (Channel, Schema, Message, Attachment, Metadata)
- Error handling via `get_last_error()` on reader/writer
- Binary stream helper
	- `BinaryStream` to pack/unpack primitives and Godot builtins (Vector2/3, Transform2D/3D, Basis)
	- Load existing `PackedByteArray` instances, seek, and export the stream back to Godot


## Installation

Visit the [Releases Page](https://github.com/NetroScript/godot-mcap/releases) to download the `godot_mcap.zip` archive of the latest release.

Unzip the contents of that archive into your Godot project folder. The resulting structure should look like this:

```
your-godot-project/
├── addons/godot-mcap/
│   ├── godot_mcap.dll
│   ├── godot_mcap.dylib
│   ├── godot_mcap.gdextension
│   ├── godot_mcap.so
│   └── ...
├── project.godot
└── ...
```

Reload or restart your Godot project. You should now see the MCAP classes available in the Create dialog (Script/Node) and documentation accessible in the editor.


The released archive contains the pre-built native libraries for Windows (`.dll`), macOS (`.dylib`), Linux (`.so`) and 64-bit ARM for Android (`.so`). 

To use the extension on other platforms (e.g. iOS, WebAssembly) or architectures, you will need to build the native library from source (see below).

## Building from source

### Prerequisites
- Godot >=4.3 (matching the `api-4-3` bindings used here)
- Rust toolchain (stable)

### Build the native library

```fish
cargo build --release
```

### Resulting Artifacts (Example: Linux x86_64)

- Debug: `target/debug/libgodot_mcap.so`
- Release: `target/release/libgodot_mcap.so`


#### Godot project setup (GDExtension)

1) Copy the built library into your project, e.g.
- `res://bin/linux_x86_64/libgodot_mcap.so`

2) Create a `res://godot-mcap.gdextension` file pointing to the library:

```ini
[configuration]
entry_symbol = "gdext_rust_init"
compatibility_minimum = 4.3
reloadable = true

[libraries]
linux.x86_64 = "res://bin/linux_x86_64/libgodot_mcap.so"
```

3) Open the project in Godot. You should see classes like `MCAPWriter`, `MCAPReader`, and `MCAPReplay` in the Create dialog (Script/Node) and documentation available in the editor.

Notes
- The `entry_symbol` string is provided by godot-rust and should remain `gdext_rust_init`.
- On other platforms, add matching `libraries` entries with the correct paths and filenames for your target (Windows: `.dll`, macOS: `.dylib`).


## Quickstart

All timestamps are microseconds (usec) by default. They use the engine time since startup, but you can shift the stored values with `MCAPWriter.set_timestamp_offset_usec()` (or `set_timestamp_offset_to_now()`) to align it them differently (or to have the timestamps start at 0). Alternatively, you can not provide a timestamp offset and write absolute timestamps if you prefer.

### Write an MCAP file (GDScript)

```gdscript
var w := MCAPWriter.new()

# Optional: configure options before open
w.options = MCAPWriteOptions.new()
w.options.compression = MCAPWriteOptions.MCAP_COMPRESSION_ZSTD # or LZ4/None depending on build features

if not w.open("user://out.mcap"):
	push_error("open failed: %s" % w.get_last_error())
	return

# Optional: treat the current engine ticks as time zero in the file
w.set_timestamp_offset_to_now()

# Optional schema
var schema_id := w.add_schema("MyType", "jsonschema", PackedByteArray())

# Channel
var ch_id := w.add_channel(schema_id, "/topic", "json", {})

# Write via known channel (header + payload)
var hdr := MCAPMessageHeader.create(ch_id)
hdr.sequence = 1
var payload := PackedByteArray("{\"hello\":\"world\"}".to_utf8_buffer())
if not w.write_to_known_channel(hdr, payload):
	push_error("write failed: %s" % w.get_last_error())

# Or write a full message with an embedded channel
var ch := MCAPChannel.create("/alt")
ch.message_encoding = "json"
var msg := MCAPMessage.create(ch, payload)
w.write(msg)

# Optional: attachments & metadata
# var att := MCAPAttachment.create("snapshot.bin", "application/octet-stream", PackedByteArray())
# w.attach(att)
# var meta := MCAPMetadata.create("run_info", {"key": "value"})
# w.write_metadata(meta)

w.flush() # finish current chunk and flush I/O
if not w.close():
	push_error("close failed: %s" % w.get_last_error())
```

> Note: The timestamp offset can be changed freely until you write the first message or attachment.
> After a time-bearing record is emitted the offset locks for the lifetime of the writer, and
> writes that would underflow the offset will fail with an error.


### Read messages (GDScript)

```gdscript
var reader := MCAPReader.open("user://out.mcap", false)

# Stream all messages (no summary required)
for msg in reader.messages():
	print(msg.channel.topic, " @ ", msg.log_time)

# Indexed helpers (require summary)
if reader.has_summary():
	var it := reader.stream_messages_iterator()
	it.seek_to_time(1_000_000) # 1s
	for msg in it:
		print("iter: ", msg.channel.topic, " @ ", msg.log_time)

	var window := reader.messages_in_time_range(2_000_000, 3_000_000)
	print("msgs in window: ", window.size())

	var atts := reader.attachments()
	var metas := reader.metadata_entries()

if reader.get_last_error() != "":
	push_error(reader.get_last_error())
```


### Serialize binary data (GDScript)

```gdscript
var stream := BinaryStream.new()
stream.write_u32(123)
stream.write_vector3(Vector3(1, 2, 3))
var bytes := stream.to_packed_byte_array()

var reader := BinaryStream.new()
reader.load_bytes(bytes)
reader.seek(0)
var id := reader.read_u32()
var pos := reader.read_vector3()
```

Use `seek`, `skip`, and the typed read/write helpers (integers, floats, half, Vector2/3, Basis, Transform2D/3D, etc.) to build binary payloads that round-trip cleanly between Rust and GDScript.


### Replay in real-time (Node)

```gdscript
var reader := MCAPReader.open("user://out.mcap", false)
var replay := MCAPReplay.new()
add_child(replay)
replay.set_reader(reader)
replay.set_time_range(-1, -1) # full file
replay.speed = 1.0
replay.looping = false
replay.processing_mode = MCAPReplay.PROCESSING_MODE_IDLE
replay.message.connect(_on_replay_message)
var ok := replay.start()
if not ok:
	push_error("MCAPReplay failed to start: missing summary or no data")

func _on_replay_message(msg: MCAPMessage) -> void:
	# Handle per-message payload
	print(msg.channel.topic, msg.log_time)
```


## API Overview

The following is an overview of the main classes and methods provided by this extension.
For detailed documentation, refer to the in-editor docs or the source code comments.

Writer: `MCAPWriter` (RefCounted)
- `open(path: String) -> bool`
- `add_schema(name: String, encoding: String, data: PackedByteArray) -> int`
- `add_channel(schema_id: int, topic: String, message_encoding: String, metadata: Dictionary) -> int`
- `write(message: MCAPMessage) -> bool`
- `write_to_known_channel(header: MCAPMessageHeader, data: PackedByteArray) -> bool`
- `set_timestamp_offset_to_now() -> bool`, `set_timestamp_offset_usec(offset: int) -> bool`, `get_timestamp_offset_usec() -> int` (configure before writing time-bearing records)
- `write_private_record(opcode: int, data: PackedByteArray, include_in_chunks: bool) -> bool`
- `attach(attachment: MCAPAttachment) -> bool`
- `write_metadata(meta: MCAPMetadata) -> bool`
- `flush() -> bool`, `close() -> bool`, `get_last_error() -> String`

Reader: `MCAPReader` (factory methods, no public `new()`)
- `open(path: String, ignore_end_magic: bool) -> MCAPReader`
- `from_bytes(data: PackedByteArray, ignore_end_magic: bool) -> MCAPReader`
- `messages() -> Array[MCAPMessage]`, `raw_messages() -> Array[Dictionary]`
- `stream_messages_iterator() -> MCAPMessageIterator`
- `attachments() -> Array[MCAPAttachment]`, `metadata_entries() -> Array[MCAPMetadata]`
- Indexed helpers: `messages_in_time_range`, `messages_for_channel`, `messages_for_channels`, `messages_for_topic`
- Info: `first_message_time_usec`, `last_message_time_usec`, `duration_usec`, `channel_ids`, `topic_names`, `topic_to_channel_id`, `channels_for_schema`, `schema_for_channel`
- Counts: `message_count_total`, `message_count_for_channel`, `message_count_in_range`, `message_count_for_channel_in_range`
- `read_summary() -> MCAPSummary?`, `has_summary() -> bool`, `get_last_error() -> String`

Iterator: `MCAPMessageIterator` (RefCounted)
- Godot iterator protocol: usable directly in `for` loops
- `for_channel(id)`, `seek_to_time(t)`, `seek_to_time_nearest(t)`, `seek_to_next_on_channel(id, after_t)`
- `get_message_at_time(id, t)`, `peek_message()`, `get_next_message()`, `has_next_message()`

Replay: `MCAPReplay` (Node)
- Properties: `speed: float`, `looping: bool`, `processing_mode: ProcessingMode`
- Methods: `set_reader()`, `set_filter_channels()`, `set_time_range()`, `start()`, `stop()`, `seek_to_time()`
- Signals: `message(MCAPMessage)`

Types (Resources)
- `MCAPWriteOptions`, `MCAPCompression`
- `MCAPSchema`, `MCAPChannel`, `MCAPMessage`, `MCAPMessageHeader`, `MCAPAttachment`, `MCAPMetadata`
- Summary/index wrappers: `MCAPSummary`, `MCAPFooter`, `MCAPChunkIndex`, `MCAPMessageIndexEntry`, `MCAPAttachmentIndex`, `MCAPMetadataIndex`


### Object identity and equality

When reading, this extension constructs new Godot Resource instances every time:

- Each call to `MCAPReader.messages()`, `raw_messages()`, `attachments()`, `metadata_entries()`, or when iterating via `MCAPMessageIterator`, creates new `MCAPMessage`, `MCAPChannel`, `MCAPSchema`, `MCAPAttachment`, and `MCAPMetadata` objects.
- The same logical channel or schema may therefore be represented by different Resource instances across calls. Do not compare objects by identity/reference.

Compare by stable properties instead:

- Channels: compare `channel.id` (preferred) or `channel.topic`.
- Schemas: compare `schema.id` (preferred), optionally with `schema.name`/`schema.encoding`.
- Messages: compare `message.channel.id` together with `message.sequence` or `message.log_time`.
- Attachments: compare a tuple such as `(name, log_time)` or use offsets if you maintain them externally.
- Metadata: compare `name` (and/or specific keys in `metadata`).


## Compression features

By default, both `zstd` and `lz4` features are enabled and passed through to the underlying `mcap` crate.

- Disable all compression:

```fish
cargo build --no-default-features
```

- Enable only LZ4:

```fish
cargo build --no-default-features --features lz4
```

- Enable only Zstd:

```fish
cargo build --no-default-features --features zstd
```

At runtime, choose the compression via `MCAPWriteOptions.compression`.


## Tips and troubleshooting

- Library fails to load in Godot
	- Ensure `entry_symbol = "gdext_rust_init"` and your `libraries` paths are correct per-platform
	- Build type must match the copied file (Debug vs Release) and your CPU/OS
	- Godot 4.3 is required for the `api-4-3` bindings used by this build
- Reading partial files
	- Use `MCAPReader.open(path, true)` to `ignore_end_magic` for truncated/incomplete captures
- Performance
	- When a Summary exists, prefer `stream_messages_iterator()` or the indexed helpers for large files
	- `flush()` on the writer ends the current chunk to make in-progress files streamable


## Development

- Build: `cargo build` or `cargo build --release`
- Lints/tests: at the moment there are no Rust tests bundled; contributions welcome
- Target Godot API: 4.3 (see `Cargo.toml` dependency features for `godot = { features = ["api-4-3"] }`)


## Maybe planned

- Add a `demo/` Godot project with example scenes and scripts

## Acknowledgements

- [mcap (Rust)](https://crates.io/crates/mcap)
- [godot-rust](https://github.com/godot-rust/gdext)


## License

The code of this repository is licensed under the MIT License (see `LICENSE`).
