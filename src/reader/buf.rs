use godot::prelude::*;
use std::sync::Arc;

// Prefer on-demand access via memory mapping to avoid copying the whole file.
// Fall back to an in-memory PackedByteArray when constructed from bytes or if mmap fails.
pub(super) enum BufBackend {
    Memory(PackedByteArray),
    Mmap(memmap2::Mmap),
}

impl BufBackend {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            BufBackend::Memory(p) => p.as_slice(),
            BufBackend::Mmap(m) => &m[..],
        }
    }
}

pub type SharedBuf = Arc<BufBackend>;
