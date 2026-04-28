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
use crate::{
    CORE_RING_INDEX, ChunksExactMut, ParallelZonedIterator, ThreadPool, odd_rounding_div_ceil,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;

pub struct ZipChunksExactMut<'data, T: Send, I: Iterator> {
    inner: ChunksExactMut<'data, T>,
    other: I,
}

pub struct ZipChunksExactMutExact<'data, T: Send, I: ExactSizeIterator> {
    inner: ChunksExactMut<'data, T>,
    other: I,
}

impl<'data, T: Send> ChunksExactMut<'data, T> {
    pub fn zip<I: Iterator>(self, other: I) -> ZipChunksExactMut<'data, T, I> {
        ZipChunksExactMut { inner: self, other }
    }

    pub fn zip_exact<I: ExactSizeIterator>(self, other: I) -> ZipChunksExactMutExact<'data, T, I> {
        let n_chunks = self.slice.len() / self.chunk_size;
        assert_eq!(
            n_chunks,
            other.len(),
            "zip_exact: chunk count ({n_chunks}) != iterator length ({})",
            other.len()
        );
        ZipChunksExactMutExact { inner: self, other }
    }
}

impl<'data, T, I> ParallelZonedIterator for ZipChunksExactMut<'data, T, I>
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
            for (chunk, item) in slice.chunks_exact_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            for (chunk, item) in slice.chunks_exact_mut(chunk_size).zip(&mut self.other) {
                f((chunk, item));
            }
            return;
        }

        let others: Vec<I::Item> = self.other.take(total_chunks).collect();

        let first_group_len = group_size.min(slice.len());
        let first_n = first_group_len / chunk_size;
        // Pre-split before thread::scope: first_items owns [0..first_n],
        // rest_items owns [first_n..]. Both are independent owned Vecs so
        // the calling thread and worker threads consume disjoint item ranges
        // regardless of scheduling order.
        let mut others = others;
        let rest_items = others.split_off(first_n);
        let first_items = others;

        let (first_group, remaining) = slice.split_at_mut(first_group_len);

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

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
                    for (chunk, item) in slice_group.chunks_exact_mut(chunk_size).zip(group_items) {
                        job((chunk, item));
                    }
                });
            }

            for (chunk, item) in first_group.chunks_exact_mut(chunk_size).zip(first_items) {
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
                .chunks_exact_mut(chunk_size)
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
                .chunks_exact_mut(chunk_size)
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

            // Workers take indices [first_n..total], base_offset starts at first_n.
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
                        .chunks_exact_mut(chunk_size)
                        .zip(group_items)
                        .enumerate()
                    {
                        job(copied_base + i, (chunk, item));
                    }
                });

                base_offset += n;
            }

            for (i, (chunk, item)) in first_group
                .chunks_exact_mut(chunk_size)
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
            for (chunk, item) in slice.chunks_exact_mut(chunk_size).zip(&mut self.other) {
                f(&mut ctx, (chunk, item));
            }
            return;
        }

        let rows_to_execute = total_chunks * chunk_size;
        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);

        if group_size == rows_to_execute {
            let mut ctx = make_context();
            for (chunk, item) in slice.chunks_exact_mut(chunk_size).zip(&mut self.other) {
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
                    for (chunk, item) in slice_group.chunks_exact_mut(chunk_size).zip(group_items) {
                        job(&mut ctx, (chunk, item));
                    }
                });
            }

            let mut ctx = make_context();
            for (chunk, item) in first_group.chunks_exact_mut(chunk_size).zip(first_items) {
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
                .chunks_exact_mut(chunk_size)
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
                .chunks_exact_mut(chunk_size)
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
                        .chunks_exact_mut(chunk_size)
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
                .chunks_exact_mut(chunk_size)
                .zip(first_items)
                .enumerate()
            {
                job(i, &mut ctx, (chunk, item));
            }
        });
    }
}

/// Delegates to ZipChunksExactMut — the length check already happened at construction.
impl<'data, T, I> ParallelZonedIterator for ZipChunksExactMutExact<'data, T, I>
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
        ZipChunksExactMut {
            inner: self.inner,
            other: self.other,
        }
        .for_each(pool, f)
    }

    fn for_each_enumerated<'scope, F>(self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
    {
        ZipChunksExactMut {
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
        ZipChunksExactMut {
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
        ZipChunksExactMut {
            inner: self.inner,
            other: self.other,
        }
        .for_each_enumerated_with_context(pool, make_context, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ChunksExactMut, ParallelZonedIterator, ThreadPool};
    use std::sync::{Arc, Mutex};

    // ── helpers ──────────────────────────────────────────────────────────────

    fn pool(n: usize) -> ThreadPool {
        ThreadPool { amount: n }
    }

    /// Collect (chunk_index, chunk_value, paired_item) for every invocation of
    /// `for_each_enumerated`, then sort by index so tests can assert on order.
    fn collect_enumerated(
        data: &mut [u32],
        chunk_size: usize,
        items: Vec<u32>,
        pool: &ThreadPool,
    ) -> Vec<(usize, Vec<u32>, u32)> {
        let results: Arc<Mutex<Vec<(usize, Vec<u32>, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);

        ChunksExactMut::new(chunk_size, data)
            .zip(items.into_iter())
            .for_each_enumerated(pool, move |idx, (chunk, item)| {
                r2.lock().unwrap().push((idx, chunk.to_vec(), item));
            });

        let mut out = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        out.sort_by_key(|(i, _, _)| *i);
        out
    }

    /// Collect (chunk_value, paired_item) for `for_each`, then sort by first
    /// element of the chunk so order-sensitive assertions are stable.
    fn collect_unordered(
        data: &mut [u32],
        chunk_size: usize,
        items: Vec<u32>,
        pool: &ThreadPool,
    ) -> Vec<(Vec<u32>, u32)> {
        let results: Arc<Mutex<Vec<(Vec<u32>, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);

        ChunksExactMut::new(chunk_size, data)
            .zip(items.into_iter())
            .for_each(pool, move |(chunk, item)| {
                r2.lock().unwrap().push((chunk.to_vec(), item));
            });

        let mut out = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        out.sort_by_key(|(c, _)| c[0]);
        out
    }

    // ── basic single-thread (pool.amount == 1) ────────────────────────────

    #[test]
    fn single_thread_for_each_visits_all_pairs() {
        let mut data = vec![10u32, 20, 30, 40, 50, 60];
        let items = vec![1u32, 2, 3];
        let got = collect_unordered(&mut data, 2, items, &pool(1));

        assert_eq!(
            got,
            vec![(vec![10, 20], 1), (vec![30, 40], 2), (vec![50, 60], 3),]
        );
    }

    #[test]
    fn single_thread_for_each_enumerated_indices_are_0_1_2() {
        let mut data = vec![10u32, 20, 30, 40, 50, 60];
        let items = vec![100u32, 200, 300];
        let got = collect_enumerated(&mut data, 2, items, &pool(1));

        let indices: Vec<usize> = got.iter().map(|(i, _, _)| *i).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn single_thread_enumerated_chunk_item_pairing_correct() {
        let mut data = vec![1u32, 2, 3, 4, 5, 6];
        let items = vec![10u32, 20, 30];
        let got = collect_enumerated(&mut data, 2, items, &pool(1));

        assert_eq!(got[0], (0, vec![1, 2], 10));
        assert_eq!(got[1], (1, vec![3, 4], 20));
        assert_eq!(got[2], (2, vec![5, 6], 30));
    }

    // ── empty / edge cases ────────────────────────────────────────────────

    #[test]
    fn empty_slice_does_nothing() {
        let mut data: Vec<u32> = vec![];
        let items = vec![1u32, 2];
        let got = collect_enumerated(&mut data, 2, items, &pool(4));
        assert!(got.is_empty());
    }

    #[test]
    fn single_chunk_single_item() {
        let mut data = vec![42u32, 43];
        let items = vec![99u32];
        let got = collect_enumerated(&mut data, 2, items, &pool(4));
        assert_eq!(got, vec![(0, vec![42, 43], 99)]);
    }

    #[test]
    fn remainder_elements_are_ignored_like_chunks_exact() {
        // 7 elements with chunk_size 2 → 3 chunks processed, 1 element ignored
        let mut data = vec![1u32, 2, 3, 4, 5, 6, 7];
        let items = vec![10u32, 20, 30, 40]; // 4 items but only 3 chunks fit
        let got = collect_enumerated(&mut data, 2, items, &pool(1));
        assert_eq!(got.len(), 3);
        assert_eq!(got[2].0, 2);
    }

    #[test]
    fn fewer_items_than_chunks_limits_output() {
        // zip stops at the shorter of the two
        let mut data = vec![1u32, 2, 3, 4, 5, 6];
        let items = vec![10u32, 20]; // only 2 items despite 3 chunks
        let got = collect_enumerated(&mut data, 2, items, &pool(4));
        assert_eq!(got.len(), 2);
    }

    // ── multi-thread index correctness ────────────────────────────────────

    #[test]
    fn multi_thread_indices_are_contiguous_0_to_n() {
        let n = 20usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).collect();

        let got = collect_enumerated(&mut data, 2, items, &pool(4));

        assert_eq!(got.len(), n);
        let indices: Vec<usize> = got.iter().map(|(i, _, _)| *i).collect();
        let expected: Vec<usize> = (0..n).collect();
        assert_eq!(
            indices, expected,
            "indices must be 0..n with no gaps or duplicates"
        );
    }

    #[test]
    fn multi_thread_each_index_appears_exactly_once() {
        let n = 16usize;
        let mut data: Vec<u32> = (0..n as u32 * 3).collect();
        let items: Vec<u32> = (0..n as u32).collect();

        let got = collect_enumerated(&mut data, 3, items, &pool(4));

        assert_eq!(got.len(), n);
        for expected_idx in 0..n {
            let count = got.iter().filter(|(i, _, _)| *i == expected_idx).count();
            assert_eq!(count, 1, "index {expected_idx} should appear exactly once");
        }
    }

    #[test]
    fn multi_thread_index_zero_corresponds_to_first_chunk() {
        let mut data: Vec<u32> = (0..20).collect();
        let items: Vec<u32> = (100..110).collect();

        let got = collect_enumerated(&mut data, 2, items, &pool(4));

        // After sorting by index, index 0 must pair with data[0..2] and items[0]
        let (idx, chunk, item) = &got[0];
        assert_eq!(*idx, 0);
        assert_eq!(chunk, &vec![0u32, 1]);
        assert_eq!(*item, 100u32);
    }

    #[test]
    fn multi_thread_index_n_minus_1_corresponds_to_last_chunk() {
        let n = 10usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).collect();

        let got = collect_enumerated(&mut data, 2, items, &pool(4));

        let (idx, chunk, item) = got.last().unwrap();
        assert_eq!(*idx, n - 1);
        assert_eq!(chunk[0], (n as u32 - 1) * 2);
        assert_eq!(*item, n as u32 - 1);
    }

    // ── chunk↔item pairing integrity ─────────────────────────────────────

    #[test]
    fn every_chunk_is_paired_with_its_matching_item_multi_thread() {
        // chunk i contains [i*2, i*2+1], item i = i*100
        // After sort-by-index we can verify the pairing at every position.
        let n = 24usize;
        let mut data: Vec<u32> = (0..n as u32 * 2).collect();
        let items: Vec<u32> = (0..n as u32).map(|i| i * 100).collect();

        let got = collect_enumerated(&mut data, 2, items, &pool(6));

        for (idx, chunk, item) in &got {
            let expected_chunk = vec![*idx as u32 * 2, *idx as u32 * 2 + 1];
            let expected_item = *idx as u32 * 100;
            assert_eq!(chunk, &expected_chunk, "chunk at index {idx} mismatch");
            assert_eq!(*item, expected_item, "item at index {idx} mismatch");
        }
    }

    // ── zip_exact variant ─────────────────────────────────────────────────

    #[test]
    fn zip_exact_produces_same_result_as_zip() {
        let n = 12usize;
        let mut data1: Vec<u32> = (0..n as u32 * 2).collect();
        let mut data2 = data1.clone();
        let items: Vec<u32> = (0..n as u32).collect();

        let mut got_zip = collect_enumerated(&mut data1, 2, items.clone(), &pool(4));
        got_zip.sort_by_key(|(i, _, _)| *i);

        // zip_exact
        let results: Arc<Mutex<Vec<(usize, Vec<u32>, u32)>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&results);
        ChunksExactMut::new(2, &mut data2)
            .zip_exact(items.into_iter())
            .for_each_enumerated(&pool(4), move |idx, (chunk, item)| {
                r2.lock().unwrap().push((idx, chunk.to_vec(), item));
            });
        let mut got_exact = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        got_exact.sort_by_key(|(i, _, _)| *i);

        assert_eq!(got_zip, got_exact);
    }

    #[test]
    #[should_panic(expected = "zip_exact")]
    fn zip_exact_panics_on_length_mismatch() {
        let mut data = vec![0u32; 8];
        let items = vec![1u32, 2, 3]; // 4 chunks but only 3 items
        ChunksExactMut::new(2, &mut data).zip_exact(items.into_iter());
    }

    // ── mutations are visible after for_each ─────────────────────────────

    #[test]
    fn for_each_mutations_applied_to_original_slice() {
        let mut data = vec![0u32; 8];
        let items = vec![10u32, 20, 30, 40];

        ChunksExactMut::new(2, &mut data)
            .zip(items.into_iter())
            .for_each(&pool(2), |(chunk, item)| {
                for v in chunk.iter_mut() {
                    *v = item;
                }
            });

        assert_eq!(data, vec![10, 10, 20, 20, 30, 30, 40, 40]);
    }

    #[test]
    fn for_each_mutations_applied_multi_thread() {
        let n = 20usize;
        let mut data = vec![0u32; n * 2];
        let items: Vec<u32> = (0..n as u32).map(|i| i + 1).collect();

        ChunksExactMut::new(2, &mut data)
            .zip(items.into_iter())
            .for_each(&pool(4), |(chunk, item)| {
                chunk[0] = item;
                chunk[1] = item * 10;
            });

        for i in 0..n {
            assert_eq!(data[i * 2], (i as u32) + 1);
            assert_eq!(data[i * 2 + 1], ((i as u32) + 1) * 10);
        }
    }

    // ── stress: many threads, large workload ──────────────────────────────

    #[test]
    fn stress_indices_unique_and_contiguous_large() {
        let n = 200usize;
        let mut data: Vec<u32> = (0..n as u32 * 4).collect();
        let items: Vec<u32> = (0..n as u32).collect();

        let got = collect_enumerated(&mut data, 4, items, &pool(8));

        assert_eq!(got.len(), n);
        let indices: Vec<usize> = got.iter().map(|(i, _, _)| *i).collect();
        let expected: Vec<usize> = (0..n).collect();
        assert_eq!(indices, expected);
    }
}
