//! A segmented, non-contiguous buffer.
//!
//! The RTM hands back [`AggregatedBytes`], which is explicitly non-contiguous, and
//! its escape hatch to copy-free consumption is `into_segments()`. Producing a
//! contiguous buffer from that means a copy whenever a read spans segments — so the
//! read API propagates the segmentation instead.
//!
//! Every operation here is refcount arithmetic on [`Bytes`] handles; no byte is ever copied.
//!
//! [`AggregatedBytes`]: https://docs.rs/aws-sdk-s3-transfer-manager

use std::collections::VecDeque;

use bytes::Bytes;

/// A run of bytes held as an ordered sequence of non-contiguous chunks.
///
/// Logically this is one byte range; physically it is however many chunks the
/// transfer layer delivered it in.
#[derive(Debug, Default, Clone)]
pub struct Segments {
    chunks: VecDeque<Bytes>,
    len: usize,
}

impl Segments {
    /// An empty run.
    pub fn new() -> Self {
        Self::default()
    }

    /// Total bytes across all chunks.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of physical chunks. Diagnostic only — a caller should not depend on
    /// how the transfer layer chose to fragment a read, but the count is worth
    /// reporting, since it is the cost segmentation imposes on a consumer that
    /// needs contiguity (a real FUSE `reply.data()`, for instance).
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Append one chunk. Empty chunks are dropped rather than stored, so
    /// `chunk_count` stays meaningful.
    pub fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        self.len += chunk.len();
        self.chunks.push_back(chunk);
    }

    /// Move up to `limit` bytes from the front of `other` onto the back of `self`,
    /// returning how many moved.
    ///
    /// The fast path — `self` empty and `other` fits entirely — is a `swap`, so the
    /// common case of "this chunk satisfies the whole read" moves no data and
    /// touches no refcounts.
    pub fn extend_from(&mut self, other: &mut Self, limit: usize) -> usize {
        if self.is_empty() && other.len() <= limit {
            std::mem::swap(self, other);
            return self.len;
        }
        let mut moved = 0;
        while moved < limit {
            let chunk = other.take_front(limit - moved);
            if chunk.is_empty() {
                break;
            }
            moved += chunk.len();
            self.push(chunk);
        }
        moved
    }

    /// Split at most `max` bytes off the front, splitting a chunk if it straddles
    /// the boundary. `Bytes::split_to` is a refcount operation, not a copy.
    pub fn take_front(&mut self, max: usize) -> Bytes {
        if max == 0 {
            return Bytes::new();
        }
        let Some(front) = self.chunks.front_mut() else {
            return Bytes::new();
        };
        if front.len() > max {
            let chunk = front.split_to(max);
            self.len -= max;
            chunk
        } else {
            let chunk = self.chunks.pop_front().expect("front exists");
            self.len -= chunk.len();
            chunk
        }
    }

    /// Iterate the chunks in order.
    pub fn chunks(&self) -> impl Iterator<Item = &Bytes> {
        self.chunks.iter()
    }

    /// Flatten into one contiguous `Bytes`.
    ///
    /// **This copies** unless there is exactly one chunk. Consumers that can work
    /// chunk-by-chunk should use [`chunks`](Self::chunks) and skip the copy; those
    /// that genuinely require one slice — FUSE's `reply.data()`, for one — have to
    /// call this, and the cost of doing so is precisely the cost segmentation defers
    /// rather than removes.
    pub fn to_contiguous(&self) -> Bytes {
        match self.chunks.len() {
            0 => Bytes::new(),
            1 => self.chunks[0].clone(),
            _ => {
                let mut out = Vec::with_capacity(self.len);
                for chunk in &self.chunks {
                    out.extend_from_slice(chunk);
                }
                Bytes::from(out)
            }
        }
    }

    /// Copy every chunk, in order, into `dst`, leaving `dst` holding exactly this run's bytes.
    ///
    /// The reusable-buffer counterpart of [`to_contiguous`](Self::to_contiguous): a caller that
    /// consumes many reads can keep one `Vec` and refill it here, paying no per-read allocation.
    /// That allocation — a fresh heap buffer freed immediately after each read — is the dominant
    /// cost of `to_contiguous` when reads are small and frequent, so a consumer that needs
    /// contiguity but can reuse storage should prefer this.
    ///
    /// `dst` is cleared first; its capacity is retained across calls, so after the first read that
    /// reaches this run's size no reallocation happens.
    pub fn copy_into(&self, dst: &mut Vec<u8>) {
        dst.clear();
        dst.reserve(self.len);
        for chunk in &self.chunks {
            dst.extend_from_slice(chunk);
        }
    }
}

impl From<Bytes> for Segments {
    fn from(bytes: Bytes) -> Self {
        let mut s = Self::new();
        s.push(bytes);
        s
    }
}

impl FromIterator<Bytes> for Segments {
    fn from_iter<I: IntoIterator<Item = Bytes>>(iter: I) -> Self {
        let mut s = Self::new();
        for chunk in iter {
            s.push(chunk);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(parts: &[&str]) -> Segments {
        parts.iter().map(|p| Bytes::copy_from_slice(p.as_bytes())).collect()
    }

    #[test]
    fn empty_chunks_are_not_stored() {
        let mut s = Segments::new();
        s.push(Bytes::new());
        s.push(Bytes::from_static(b"ab"));
        s.push(Bytes::new());
        assert_eq!(s.len(), 2);
        assert_eq!(s.chunk_count(), 1);
    }

    #[test]
    fn len_tracks_pushes_and_takes() {
        let mut s = seg(&["abc", "de"]);
        assert_eq!(s.len(), 5);
        assert_eq!(s.chunk_count(), 2);
        assert_eq!(&s.take_front(2)[..], b"ab");
        assert_eq!(s.len(), 3);
        assert_eq!(&s.take_front(99)[..], b"c");
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn take_front_zero_yields_nothing() {
        let mut s = seg(&["abc"]);
        assert!(s.take_front(0).is_empty());
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn take_front_on_empty_yields_nothing() {
        let mut s = Segments::new();
        assert!(s.take_front(4).is_empty());
    }

    #[test]
    fn extend_from_swaps_when_it_fits() {
        let mut dst = Segments::new();
        let mut src = seg(&["abc", "de"]);
        let moved = dst.extend_from(&mut src, 10);
        assert_eq!(moved, 5);
        assert!(src.is_empty());
        // Swapped whole, so fragmentation is preserved rather than flattened.
        assert_eq!(dst.chunk_count(), 2);
    }

    #[test]
    fn extend_from_respects_limit_and_splits() {
        let mut dst = Segments::new();
        let mut src = seg(&["abcd", "efgh"]);
        let moved = dst.extend_from(&mut src, 6);
        assert_eq!(moved, 6);
        assert_eq!(&dst.to_contiguous()[..], b"abcdef");
        assert_eq!(src.len(), 2);
        assert_eq!(&src.to_contiguous()[..], b"gh");
    }

    #[test]
    fn extend_from_stops_when_source_dries_up() {
        let mut dst = seg(&["x"]);
        let mut src = seg(&["ab"]);
        let moved = dst.extend_from(&mut src, 100);
        assert_eq!(moved, 2);
        assert!(src.is_empty());
        assert_eq!(&dst.to_contiguous()[..], b"xab");
    }

    #[test]
    fn to_contiguous_joins_in_order() {
        assert_eq!(&seg(&["ab", "cd", "e"]).to_contiguous()[..], b"abcde");
        assert!(Segments::new().to_contiguous().is_empty());
    }

    #[test]
    fn single_chunk_to_contiguous_does_not_reallocate() {
        let original = Bytes::from_static(b"abcdef");
        let s = Segments::from(original.clone());
        // Same allocation, so this is a refcount bump rather than a copy.
        assert_eq!(s.to_contiguous().as_ptr(), original.as_ptr());
    }

    #[test]
    fn copy_into_joins_in_order_and_reuses_capacity() {
        let mut dst = Vec::new();
        seg(&["ab", "cd", "e"]).copy_into(&mut dst);
        assert_eq!(&dst[..], b"abcde");
        let cap = dst.capacity();
        // A smaller run reuses the buffer without growing it.
        seg(&["xy"]).copy_into(&mut dst);
        assert_eq!(&dst[..], b"xy");
        assert_eq!(dst.capacity(), cap);
        // An empty run empties the buffer.
        Segments::new().copy_into(&mut dst);
        assert!(dst.is_empty());
    }
}
