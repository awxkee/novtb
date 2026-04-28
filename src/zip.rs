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

        if pool.amount <= 1 {
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        let others: Vec<I::Item> = self.other.take(total_chunks).collect();

        // split_off(first_n): `others` keeps [0..first_n] (group-0 items),
        // `rest_items` gets [first_n..] (worker items). Both are owned Vecs
        // that can be moved into different threads independently — no iterator
        // ordering dependency.
        let first_group_len = group_size.min(slice.len());
        let first_n = first_group_len / chunk_size;
        let mut others = others;
        let rest_items = others.split_off(first_n);
        let first_items = others;

        let (first_group, remaining) = slice.split_at_mut(first_group_len);

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            // Spawn workers for remaining groups, consuming rest_items eagerly.
            let mut rest_iter = rest_items.into_iter();
            for slice_group in remaining.chunks_mut(group_size) {
                let n = slice_group.len() / chunk_size;
                let group_items: Vec<I::Item> = rest_iter.by_ref().take(n).collect();

                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for (chunk, item) in slice_group.chunks_mut(chunk_size).zip(group_items) {
                        job((chunk, item));
                    }
                });
            }

            // Run group-0 on the calling thread while workers run concurrently.
            for (chunk, item) in first_group.chunks_mut(chunk_size).zip(first_items) {
                job((chunk, item));
            }
        });
    }

    fn for_each_enumerated<'scope, F>(mut self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
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

        let first_group_len = group_size.min(slice.len());
        let first_n = first_group_len / chunk_size;
        let mut others = others;
        let rest_items = others.split_off(first_n);
        let first_items = others;

        let (first_group, remaining) = slice.split_at_mut(first_group_len);

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            // Workers cover groups 1..N with base offsets starting at first_n.
            let mut rest_iter = rest_items.into_iter();
            let mut base_offset = first_n;

            for slice_group in remaining.chunks_mut(group_size) {
                let n = slice_group.len() / chunk_size;
                let group_items: Vec<I::Item> = rest_iter.by_ref().take(n).collect();

                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();
                let copied_base = base_offset;

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for (i, (chunk, item)) in slice_group
                        .chunks_mut(chunk_size)
                        .zip(group_items)
                        .enumerate()
                    {
                        job(copied_base + i, (chunk, item));
                    }
                });

                base_offset += n;
            }

            // Group-0 on calling thread: indices 0..first_n, items 0..first_n.
            for (i, (chunk, item)) in first_group
                .chunks_mut(chunk_size)
                .zip(first_items)
                .enumerate()
            {
                job(i, (chunk, item));
            }
        });
    }

    fn for_each_with_context<'scope, F, C, CF>(mut self, pool: &ThreadPool, make_context: CF, f: F)
    where
        F: 'scope + Fn(&mut C, Self::Item) + Send + Sync,
        C: Send,
        CF: Fn() -> C + Send + Sync,
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

        if pool.amount <= 1 {
            let mut ctx = make_context();
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f(&mut ctx, (chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            let mut ctx = make_context();
            for (chunk, item) in slice.chunks_mut(chunk_size).zip(&mut self.other) {
                f(&mut ctx, (chunk, item));
            }
            return;
        }

        let others: Vec<I::Item> = self.other.take(total_chunks).collect();

        let first_group_len = group_size.min(slice.len());
        let first_n = first_group_len / chunk_size;
        let mut others = others;
        let rest_items = others.split_off(first_n);
        let first_items = others;

        let (first_group, remaining) = slice.split_at_mut(first_group_len);

        thread::scope(|s| {
            let job = Arc::new(f);
            let make_context = Arc::new(make_context);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut rest_iter = rest_items.into_iter();
            for slice_group in remaining.chunks_mut(group_size) {
                let n = slice_group.len() / chunk_size;
                let group_items: Vec<I::Item> = rest_iter.by_ref().take(n).collect();

                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let make_context = Arc::clone(&make_context);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    let mut ctx = make_context();
                    for (chunk, item) in slice_group.chunks_mut(chunk_size).zip(group_items) {
                        job(&mut ctx, (chunk, item));
                    }
                });
            }

            let mut ctx = make_context();
            for (chunk, item) in first_group.chunks_mut(chunk_size).zip(first_items) {
                job(&mut ctx, (chunk, item));
            }
        });
    }

    fn for_each_enumerated_with_context<'scope, F, C, CF>(
        mut self,
        pool: &ThreadPool,
        make_context: CF,
        f: F,
    ) where
        F: 'scope + Fn(usize, &mut C, Self::Item) + Send + Sync,
        C: Send,
        CF: Fn() -> C + Send + Sync,
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

        if pool.amount <= 1 {
            let mut ctx = make_context();
            for (i, (chunk, item)) in slice
                .chunks_mut(chunk_size)
                .zip(&mut self.other)
                .enumerate()
            {
                f(i, &mut ctx, (chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            let mut ctx = make_context();
            for (i, (chunk, item)) in slice
                .chunks_mut(chunk_size)
                .zip(&mut self.other)
                .enumerate()
            {
                f(i, &mut ctx, (chunk, item));
            }
            return;
        }

        let others: Vec<I::Item> = self.other.take(total_chunks).collect();

        let first_group_len = group_size.min(slice.len());
        let first_n = first_group_len / chunk_size;
        let mut others = others;
        let rest_items = others.split_off(first_n);
        let first_items = others;

        let (first_group, remaining) = slice.split_at_mut(first_group_len);

        thread::scope(|s| {
            let job = Arc::new(f);
            let make_context = Arc::new(make_context);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut rest_iter = rest_items.into_iter();
            let mut base_offset = first_n;

            for slice_group in remaining.chunks_mut(group_size) {
                let n = slice_group.len() / chunk_size;
                let group_items: Vec<I::Item> = rest_iter.by_ref().take(n).collect();

                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
                let job = Arc::clone(&job);
                let make_context = Arc::clone(&make_context);
                let core = cores.get(offset % cores_len).cloned();
                let copied_base = base_offset;

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    let mut ctx = make_context();
                    for (i, (chunk, item)) in slice_group
                        .chunks_mut(chunk_size)
                        .zip(group_items)
                        .enumerate()
                    {
                        job(copied_base + i, &mut ctx, (chunk, item));
                    }
                });

                base_offset += n;
            }

            let mut ctx = make_context();
            for (i, (chunk, item)) in first_group
                .chunks_mut(chunk_size)
                .zip(first_items)
                .enumerate()
            {
                job(i, &mut ctx, (chunk, item));
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

    fn for_each_with_context<'scope, F, C, CF>(self, pool: &ThreadPool, make_context: CF, f: F)
    where
        F: 'scope + Fn(&mut C, Self::Item) + Send + Sync,
        C: Send,
        CF: Fn() -> C + Send + Sync,
    {
        ZipChunks {
            inner: self.inner,
            other: self.other,
        }
        .for_each_with_context(pool, make_context, f)
    }

    fn for_each_enumerated_with_context<'scope, F, C, CF>(
        self,
        pool: &ThreadPool,
        make_context: CF,
        f: F,
    ) where
        F: 'scope + Fn(usize, &mut C, Self::Item) + Send + Sync,
        C: Send,
        CF: Fn() -> C + Send + Sync,
    {
        ZipChunks {
            inner: self.inner,
            other: self.other,
        }
        .for_each_enumerated_with_context(pool, make_context, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ChunksMut, ParallelZonedIterator, ThreadPool};
    use std::sync::{Arc, Mutex};

    fn pool(n: usize) -> ThreadPool {
        ThreadPool { amount: n }
    }

    // Collect (chunk_index, item) pairs sorted by chunk index.
    // Uses for_each_enumerated so the chunk position comes from the idx
    // parameter — correct for any data values, not just sequential ones.
    fn collect_pairing(
        data: &mut [u32],
        chunk_size: usize,
        items: Vec<u32>,
        pool: &ThreadPool,
    ) -> Vec<(usize, u32)> {
        let results: Arc<Mutex<Vec<(usize, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);

        ChunksMut::new(chunk_size, data)
            .zip(items.into_iter())
            .for_each_enumerated(pool, move |idx, (_chunk, item)| {
                r2.lock().unwrap().push((idx, item));
            });

        let mut out = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        out.sort_by_key(|(pos, _)| *pos);
        out
    }

    // Collects (idx, item) via for_each_enumerated, sorted by idx.
    // idx is the ground truth chunk position provided by the iterator contract.
    fn collect_enumerated(
        data: &mut [u32],
        chunk_size: usize,
        items: Vec<u32>,
        pool: &ThreadPool,
    ) -> Vec<(usize, u32)> {
        let results: Arc<Mutex<Vec<(usize, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);

        ChunksMut::new(chunk_size, data)
            .zip(items.into_iter())
            .for_each_enumerated(pool, move |idx, (_chunk, item)| {
                r2.lock().unwrap().push((idx, item));
            });

        let mut out = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        out.sort_by_key(|(idx, _)| *idx);
        out
    }

    // ── single-thread baseline ────────────────────────────────────────────

    #[test]
    fn single_thread_visits_all_pairs() {
        let mut data = vec![10u32, 20, 30, 40, 50, 60];
        let items = vec![1u32, 2, 3];
        let got = collect_pairing(&mut data, 2, items, &pool(1));
        assert_eq!(got, vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn single_thread_enumerated_indices_are_0_1_2() {
        let mut data = vec![10u32, 20, 30, 40, 50, 60];
        let items = vec![100u32, 200, 300];
        let got = collect_enumerated(&mut data, 2, items, &pool(1));
        let indices: Vec<usize> = got.iter().map(|(i, _)| *i).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn single_thread_pairing_correct() {
        let mut data = vec![1u32, 2, 3, 4, 5, 6];
        let items = vec![10u32, 20, 30];
        let got = collect_pairing(&mut data, 2, items, &pool(1));
        assert_eq!(got, vec![(0, 10), (1, 20), (2, 30)]);
    }

    // ── edge cases ────────────────────────────────────────────────────────

    #[test]
    fn empty_slice_does_nothing() {
        let mut data: Vec<u32> = vec![];
        let got = collect_pairing(&mut data, 2, vec![1u32, 2], &pool(4));
        assert!(got.is_empty());
    }

    #[test]
    fn single_chunk() {
        let mut data = vec![42u32, 43];
        let got = collect_pairing(&mut data, 2, vec![99u32], &pool(4));
        assert_eq!(got, vec![(0, 99)]);
    }

    #[test]
    fn fewer_items_than_chunks_limits_output() {
        let mut data = vec![1u32, 2, 3, 4, 5, 6];
        let got = collect_pairing(&mut data, 2, vec![10u32, 20], &pool(4));
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn non_power_of_two_chunk_size() {
        let mut data: Vec<u32> = (0..15).collect();
        let items: Vec<u32> = (0..5).collect();
        let got = collect_pairing(&mut data, 3, items, &pool(4));
        assert_eq!(got.len(), 5);
        for (idx, item) in &got {
            assert_eq!(*item, *idx as u32, "idx={idx}");
        }
    }

    // ── multi-thread: pairing integrity (the core invariant) ─────────────
    //
    // The scheduler runs group-0 on the calling thread while other groups run
    // on worker threads concurrently.  The ORDER in which groups finish is
    // non-deterministic, but the PAIRING must always be:
    //   chunk at position K  <->  items[K]
    // This is the invariant we test.  We never assert on execution order.

    #[test]
    fn every_chunk_paired_with_matching_item_multi_thread() {
        let n = 24usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).map(|i| i * 100).collect();
        let got = collect_pairing(&mut data, 2, items, &pool(6));

        assert_eq!(got.len(), n);
        for (idx, item) in &got {
            assert_eq!(
                *item,
                *idx as u32 * 100,
                "chunk at position {idx} got wrong item"
            );
        }
    }

    #[test]
    fn ceiling_group_pairing_correct() {
        for pool_size in [2, 3, 4, 5, 7] {
            for n in [5, 7, 9, 10, 11, 13, 17, 19, 23] {
                let mut data: Vec<u32> = (0..n as u32 * 2).collect();
                let items: Vec<u32> = (0..n as u32).map(|i| i * 7).collect();
                let got = collect_pairing(&mut data, 2, items, &pool(pool_size));

                assert_eq!(got.len(), n, "pool={pool_size} n={n}: wrong count");
                for (idx, item) in &got {
                    assert_eq!(
                        *item,
                        *idx as u32 * 7,
                        "pool={pool_size} n={n} idx={idx}: item wrong"
                    );
                }
            }
        }
    }

    // ── for_each_enumerated: idx must equal chunk position ────────────────
    //
    // The enumerated index is a logical position label, not an execution-order
    // counter.  After sorting by idx, item at position K must equal items[K].
    // entry — regardless of which thread ran it.

    #[test]
    fn enumerated_idx_matches_item_position_single_thread() {
        let n = 10usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).map(|i| i * 11).collect();
        let got = collect_enumerated(&mut data, 2, items, &pool(1));
        // idx is the chunk position; item at position K must be items[K] = K*11.
        for (idx, item) in &got {
            assert_eq!(*item, *idx as u32 * 11, "idx={idx}");
        }
    }

    #[test]
    fn enumerated_idx_matches_item_position_multi_thread() {
        let n = 20usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).map(|i| i * 11).collect();
        let got = collect_enumerated(&mut data, 2, items, &pool(4));

        assert_eq!(got.len(), n);
        for (idx, item) in &got {
            assert_eq!(*item, *idx as u32 * 11, "idx={idx}");
        }
    }

    #[test]
    fn enumerated_indices_unique_and_cover_0_to_n() {
        let n = 16usize;
        let mut data: Vec<u32> = (0..n as u32 * 3).collect();
        let items: Vec<u32> = (0..n as u32).collect();
        let got = collect_enumerated(&mut data, 3, items, &pool(4));

        assert_eq!(got.len(), n);
        for expected in 0..n {
            let count = got.iter().filter(|(idx, _)| *idx == expected).count();
            assert_eq!(count, 1, "idx {expected} should appear exactly once");
        }
    }

    #[test]
    fn enumerated_pairing_matches_for_each_pairing() {
        // for_each and for_each_enumerated must agree on which item goes with
        // which chunk.  Run both on identical data and compare.
        let n = 18usize;
        let mut data1: Vec<u32> = (0..n as u32 * 2).collect();
        let mut data2 = data1.clone();
        let items: Vec<u32> = (0..n as u32).map(|i| i * 13).collect();

        let by_for_each = collect_pairing(&mut data1, 2, items.clone(), &pool(4));
        let by_enumerated = collect_enumerated(&mut data2, 2, items, &pool(4));

        assert_eq!(by_for_each, by_enumerated);
    }

    // ── zip_exact variant ─────────────────────────────────────────────────

    #[test]
    fn zip_exact_pairing_matches_zip() {
        let n = 12usize;
        let mut data1: Vec<u32> = (0..n as u32 * 2).collect();
        let mut data2 = data1.clone();
        let items: Vec<u32> = (0..n as u32).collect();

        let got_zip = collect_pairing(&mut data1, 2, items.clone(), &pool(4));

        let results: Arc<Mutex<Vec<(usize, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);
        ChunksMut::new(2, &mut data2)
            .zip_exact(items.into_iter())
            .for_each_enumerated(&pool(4), move |idx, (_chunk, item)| {
                r2.lock().unwrap().push((idx, item));
            });
        let mut got_exact = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        got_exact.sort_by_key(|(pos, _)| *pos);

        assert_eq!(got_zip, got_exact);
    }

    #[test]
    #[should_panic(expected = "zip_exact")]
    fn zip_exact_panics_on_length_mismatch() {
        let mut data = vec![0u32; 8];
        ChunksMut::new(2, &mut data).zip_exact(vec![1u32, 2, 3].into_iter());
    }

    // ── mutations reach original slice ────────────────────────────────────

    #[test]
    fn mutations_applied_single_thread() {
        let mut data = vec![0u32; 8];
        ChunksMut::new(2, &mut data)
            .zip(vec![10u32, 20, 30, 40].into_iter())
            .for_each(&pool(1), |(chunk, item)| {
                for v in chunk.iter_mut() {
                    *v = item;
                }
            });
        assert_eq!(data, vec![10, 10, 20, 20, 30, 30, 40, 40]);
    }

    #[test]
    fn mutations_applied_multi_thread() {
        let n = 20usize;
        let mut data = vec![0u32; n * 2];
        let items: Vec<u32> = (1..=n as u32).collect();

        ChunksMut::new(2, &mut data)
            .zip(items.into_iter())
            .for_each(&pool(4), |(chunk, item)| {
                chunk[0] = item;
                chunk[1] = item * 10;
            });

        for i in 0..n {
            assert_eq!(data[i * 2], i as u32 + 1);
            assert_eq!(data[i * 2 + 1], (i as u32 + 1) * 10);
        }
    }

    // ── stress ────────────────────────────────────────────────────────────

    #[test]
    fn stress_pairing_correct_large() {
        let n = 200usize;
        let mut data: Vec<u32> = (0..n as u32 * 4).collect();
        let items: Vec<u32> = (0..n as u32).collect();
        let got = collect_pairing(&mut data, 4, items, &pool(8));

        assert_eq!(got.len(), n);
        for (idx, item) in &got {
            assert_eq!(*item, *idx as u32);
        }
    }
}
