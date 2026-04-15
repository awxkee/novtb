/*
 * // Copyright (c) Radzivon Bartoshyk 4/2026. All rights reserved.
 * //
 * // Redistribution and use in source and binary forms, with or without modification,
 * // are permitted provided that the following conditions are met:
 * //
 * // 1.  Redistributions of source code must retain the above copyright notice, this
 * // list of conditions and the following disclaimer.
 * //
 * // 2.  Redistributions in binary form must reproduce the above copyright notice,
 * // this list of conditions and the following disclaimer in the documentation
 * // and/or other materials provided with the distribution.
 * //
 * // 3.  Neither the name of the copyright holder nor the names of its
 * // contributors may be used to endorse or promote products derived from
 * // this software without specific prior written permission.
 * //
 * // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * // AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * // IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * // DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * // FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * // DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * // SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * // CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * // OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * // OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
use crate::{CORE_RING_INDEX, ChunksMut, ParallelZonedIterator, ThreadPool, odd_rounding_div_ceil};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;

/// Zips `ChunksMut` with any iterator. The shorter side determines
/// how many pairs are produced (like `Iterator::zip`).
pub struct ZipChunks<'data, T: Send, I: Iterator> {
    inner: ChunksMut<'data, T>,
    other: I,
}

/// Like `ZipChunks`, but panics at construction time if the number of
/// chunks does not exactly equal `other.size_hint().0`.
pub struct ZipChunksExact<'data, T: Send, I: ExactSizeIterator> {
    inner: ChunksMut<'data, T>,
    other: I,
}

impl<'data, T: Send> ChunksMut<'data, T> {
    /// Zip with any iterator. Extra items on either side are silently dropped.
    pub fn zip<I: Iterator>(self, other: I) -> ZipChunks<'data, T, I> {
        ZipChunks { inner: self, other }
    }

    /// Zip with an exact-size iterator. Panics if chunk count ≠ iterator length.
    pub fn zip_exact<I: ExactSizeIterator>(self, other: I) -> ZipChunksExact<'data, T, I> {
        let n_chunks =
            (self.slice.len() / self.chunk_size).max(if self.slice.is_empty() { 0 } else { 1 });
        assert_eq!(
            n_chunks,
            other.len(),
            "zip_exact: chunk count ({n_chunks}) != iterator length ({})",
            other.len()
        );
        ZipChunksExact { inner: self, other }
    }
}

impl<'data, T, I> ParallelZonedIterator for ZipChunks<'data, T, I>
where
    T: Send,
    I: Iterator + Send,
    I::Item: Send,
{
    type Item = (&'data mut [T], I::Item);

    fn for_each<'scope, F>(mut self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(Self::Item) + Send + Sync,
    {
        if self.inner.slice.is_empty() {
            return;
        }

        let chunk_size = self.inner.chunk_size;
        let slice = std::mem::take(&mut self.inner.slice);
        let total_chunks = (slice.len() / chunk_size).min(self.other.size_hint().0);

        if total_chunks == 0 {
            return;
        }

        let slice = &mut slice[..total_chunks * chunk_size];

        // No threading needed — consume the iterator directly, no allocation.
        if pool.amount <= 1 {
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        // Not enough chunks to distribute across threads — also no allocation.
        if group_size == rows_to_execute {
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        // Only here do we pay for the allocation — threading is confirmed needed.
        let others: Vec<I::Item> = self.other.take(total_chunks).collect();
        let chunks_per_group = group_size / chunk_size;

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            // We need to split `others` in sync with the slice groups.
            // Build an iterator over (slice_group, others_group) pairs.
            let mut slice_chunks = slice.chunks_mut(group_size);
            let mut others_iter = others.into_iter();
            let mut grouped_others: Vec<Vec<I::Item>> = Vec::new();
            loop {
                let group: Vec<I::Item> = others_iter.by_ref().take(chunks_per_group).collect();
                if group.is_empty() {
                    break;
                }
                grouped_others.push(group);
            }

            let first_slice_group = slice_chunks.next();
            let first_others_group = if !grouped_others.is_empty() {
                Some(grouped_others.swap_remove(0))
            } else {
                None
            };

            // Spawn workers for all groups except the first.
            for (slice_group, others_group) in slice_chunks.zip(grouped_others) {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for (chunk, item) in slice_group.chunks_mut(chunk_size).zip(others_group) {
                        job((chunk, item));
                    }
                });
            }

            // Run the first group on the calling thread.
            if let (Some(sg), Some(og)) = (first_slice_group, first_others_group) {
                for (chunk, item) in sg.chunks_mut(chunk_size).zip(og) {
                    job((chunk, item));
                }
            }
        });
    }

    fn for_each_enumerated<'scope, F>(mut self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
    {
        // Same as for_each but passes the global chunk index.
        if self.inner.slice.is_empty() {
            return;
        }

        let chunk_size = self.inner.chunk_size;
        let slice = std::mem::take(&mut self.inner.slice);
        let total_chunks = (slice.len() / chunk_size).min(self.other.size_hint().0);

        if total_chunks == 0 {
            return;
        }

        let slice = &mut slice[..total_chunks * chunk_size];

        if pool.amount <= 1 {
            for (i, (chunk, item)) in slice
                .chunks_mut(chunk_size)
                .zip(&mut self.other)
                .enumerate()
            {
                f(i, (chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            for (i, (chunk, item)) in slice
                .chunks_mut(chunk_size)
                .zip(&mut self.other)
                .enumerate()
            {
                f(i, (chunk, item));
            }
            return;
        }

        let others: Vec<I::Item> = self.other.take(total_chunks).collect();
        let chunks_per_group = group_size / chunk_size;
        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut others_iter = others.into_iter();
            let mut grouped_others: Vec<Vec<I::Item>> = Vec::new();
            loop {
                let group: Vec<I::Item> = others_iter.by_ref().take(chunks_per_group).collect();
                if group.is_empty() {
                    break;
                }
                grouped_others.push(group);
            }

            let mut slice_chunks = slice.chunks_mut(group_size);
            let first_slice_group = slice_chunks.next();
            let first_others_group = if !grouped_others.is_empty() {
                Some(grouped_others.swap_remove(0))
            } else {
                None
            };

            let first_group_chunks = first_slice_group
                .as_ref()
                .map(|g| g.len() / chunk_size)
                .unwrap_or(0);

            let mut base_offset = first_group_chunks;

            for (slice_group, others_group) in slice_chunks.zip(grouped_others) {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();
                let local_count = slice_group.len() / chunk_size;
                let copied_base = base_offset;

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for (i, (chunk, item)) in slice_group
                        .chunks_mut(chunk_size)
                        .zip(others_group)
                        .enumerate()
                    {
                        job(copied_base + i, (chunk, item));
                    }
                });

                base_offset += local_count;
            }

            if let (Some(sg), Some(og)) = (first_slice_group, first_others_group) {
                for (i, (chunk, item)) in sg.chunks_mut(chunk_size).zip(og).enumerate() {
                    job(i, (chunk, item));
                }
            }
        });
    }
}

/// ZipChunksExact delegates entirely — the length check already happened at construction.
impl<'data, T, I> ParallelZonedIterator for ZipChunksExact<'data, T, I>
where
    T: Send,
    I: ExactSizeIterator + Send,
    I::Item: Send,
{
    type Item = (&'data mut [T], I::Item);

    fn for_each<'scope, F>(self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(Self::Item) + Send + Sync,
    {
        ZipChunks {
            inner: self.inner,
            other: self.other,
        }
        .for_each(pool, f)
    }

    fn for_each_enumerated<'scope, F>(self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
    {
        ZipChunks {
            inner: self.inner,
            other: self.other,
        }
        .for_each_enumerated(pool, f)
    }
}
