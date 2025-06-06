/*
 * // Copyright (c) Radzivon Bartoshyk 6/2025. All rights reserved.
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
#![deny(clippy::float_arithmetic)]
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

static CORE_RING_INDEX: AtomicUsize = AtomicUsize::new(0);

pub struct ThreadPool {
    amount: usize,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        ThreadPool {
            amount: size.max(1),
        }
    }

    pub fn parallel_for<'scope, F>(&self, job: F)
    where
        F: 'scope + Fn(usize) + Send + Sync,
    {
        if self.amount <= 1 {
            job(0);
            return;
        }
        thread::scope(|s| {
            let job = Arc::new(job);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            for i in 1..self.amount {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);

                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();
                
                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    job(i);
                });
            }

            job(0);
        });
    }
}

/// Parallel iterator over mutable non-overlapping chunks of a slice
#[derive(Debug)]
pub struct ChunksExactMut<'data, T: Send> {
    chunk_size: usize,
    slice: &'data mut [T],
    rem: &'data mut [T],
}

impl<'data, T: Send> ChunksExactMut<'data, T> {
    pub fn new(chunk_size: usize, slice: &'data mut [T]) -> Self {
        let rem_len = slice.len() % chunk_size;
        let len = slice.len() - rem_len;
        let (slice, rem) = slice.split_at_mut(len);
        Self {
            chunk_size,
            slice,
            rem,
        }
    }

    /// Return the remainder of the original slice that is not going to be
    /// returned by the iterator. The returned slice has at most `chunk_size-1`
    /// elements.
    ///
    /// Note that this has to consume `self` to return the original lifetime of
    /// the data, which prevents this from actually being used as a parallel
    /// iterator since that also consumes. This method is provided for parity
    /// with `std::iter::ChunksExactMut`, but consider calling `remainder()` or
    /// `take_remainder()` as alternatives.
    pub fn into_remainder(self) -> &'data mut [T] {
        self.rem
    }

    /// Return the remainder of the original slice that is not going to be
    /// returned by the iterator. The returned slice has at most `chunk_size-1`
    /// elements.
    ///
    /// Consider `take_remainder()` if you need access to the data with its
    /// original lifetime, rather than borrowing through `&mut self` here.
    pub fn remainder(&mut self) -> &mut [T] {
        self.rem
    }

    /// Return the remainder of the original slice that is not going to be
    /// returned by the iterator. The returned slice has at most `chunk_size-1`
    /// elements. Subsequent calls will return an empty slice.
    pub fn take_remainder(&mut self) -> &'data mut [T] {
        std::mem::take(&mut self.rem)
    }
}

/// Parallel iterator over mutable non-overlapping chunks of a slice
#[derive(Debug)]
pub struct ChunksMut<'data, T: Send> {
    chunk_size: usize,
    slice: &'data mut [T],
}

impl<'data, T: Send> ChunksMut<'data, T> {
    pub fn new(chunk_size: usize, slice: &'data mut [T]) -> Self {
        Self { chunk_size, slice }
    }
}

pub trait TbSliceMut<T: Send> {
    /// Returns a plain mutable slice, which is used to implement the rest of
    /// the parallel methods.
    fn as_tb_slice_mut(&mut self) -> &mut [T];

    #[track_caller]
    fn tb_par_chunks_exact_mut(&mut self, chunk_size: usize) -> ChunksExactMut<'_, T> {
        assert!(chunk_size != 0, "chunk_size must not be zero");
        ChunksExactMut::new(chunk_size, self.as_tb_slice_mut())
    }

    #[track_caller]
    fn tb_par_chunks_mut(&mut self, chunk_size: usize) -> ChunksMut<'_, T> {
        assert!(chunk_size != 0, "chunk_size must not be zero");
        ChunksMut::new(chunk_size, self.as_tb_slice_mut())
    }
}

impl<T: Send> TbSliceMut<T> for [T] {
    #[inline]
    fn as_tb_slice_mut(&mut self) -> &mut [T] {
        self
    }
}

pub trait ParallelZonedIterator: Sized + Send {
    /// The type of item that this parallel iterator produces.
    /// For example, if you use the [`for_each`] method, this is the type of
    /// item that your closure will be invoked with.
    ///
    /// [`for_each`]: #method.for_each
    type Item: Send;

    fn for_each<'scope, F>(self, thread_pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(Self::Item) + Send + Sync;

    fn for_each_enumerated<'scope, F>(self, thread_pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync;
}

fn odd_rounding_div_ceil(rows_to_execute: usize, threads: usize, chunk_size: usize) -> usize {
    assert!(
        rows_to_execute > 0 && threads > 0 && chunk_size > 0,
        "All values must be not null"
    );

    let rows_per_thread = rows_to_execute.div_ceil(threads);

    let mut group_size = rows_per_thread.div_ceil(chunk_size) * chunk_size;

    let mut n_threads = threads;
    while group_size == 0 && n_threads > 2 {
        n_threads -= 1;
        let rows_per_thread = rows_to_execute.div_ceil(threads);
        group_size = rows_per_thread.div_ceil(chunk_size) * chunk_size;
    }

    if group_size == 0 {
        return rows_to_execute;
    }
    group_size
}

impl<'data, T: Send> ParallelZonedIterator for ChunksExactMut<'data, T> {
    type Item = &'data mut [T];

    fn for_each<'scope, F>(mut self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(Self::Item) + Send + Sync,
    {
        if pool.amount <= 1 {
            for chunk in self.slice.chunks_exact_mut(self.chunk_size) {
                f(chunk);
            }
            return;
        }
        let chunk_size = self.chunk_size;
        let slice = std::mem::take(&mut self.slice); // take ownership
        let total_chunks = slice.len() / chunk_size;

        let rows_to_execute = total_chunks * chunk_size;

        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);
        if group_size == rows_to_execute {
            for chunk in slice.chunks_exact_mut(chunk_size) {
                f(chunk);
            }
            return;
        }

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut chunks = slice.chunks_mut(group_size);
            let first_group = chunks.next();

            for chunk in chunks {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);

                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for chunk in chunk.chunks_exact_mut(self.chunk_size) {
                        job(chunk);
                    }
                });
            }

            if let Some(first_group) = first_group {
                for chunk in first_group.chunks_exact_mut(self.chunk_size) {
                    job(chunk);
                }
            }
        });
    }

    fn for_each_enumerated<'scope, F>(mut self, thread_pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
    {
        if thread_pool.amount <= 1 {
            for (i, chunk) in self.slice.chunks_exact_mut(self.chunk_size).enumerate() {
                f(i, chunk);
            }
            return;
        }
        let chunk_size = self.chunk_size;
        let slice = std::mem::take(&mut self.slice); // take ownership
        let total_chunks = slice.len() / chunk_size;

        let rows_to_execute = total_chunks * chunk_size;

        let group_size = odd_rounding_div_ceil(rows_to_execute, thread_pool.amount, chunk_size);
        if group_size == rows_to_execute {
            for (i, chunk) in slice.chunks_exact_mut(chunk_size).enumerate() {
                f(i, chunk);
            }
            return;
        }

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut chunks = slice.chunks_mut(group_size);
            let first_group = chunks.next();

            for (z, chunk) in chunks.enumerate() {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);

                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    let base_id = z + 1;
                    for (i, chunk) in chunk.chunks_exact_mut(chunk_size).enumerate() {
                        job(base_id + i, chunk);
                    }
                });
            }

            if let Some(first_group) = first_group {
                for (i, chunk) in first_group.chunks_exact_mut(chunk_size).enumerate() {
                    job(i, chunk);
                }
            }
        });
    }
}

impl<'data, T: Send> ParallelZonedIterator for ChunksMut<'data, T> {
    type Item = &'data mut [T];

    fn for_each<'scope, F>(mut self, pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(Self::Item) + Send + Sync,
    {
        if pool.amount <= 1 {
            for chunk in self.slice.chunks_mut(self.chunk_size) {
                f(chunk);
            }
            return;
        }
        let chunk_size = self.chunk_size;
        let slice = std::mem::take(&mut self.slice); // take ownership
        let total_chunks = slice.len() / chunk_size;

        let rows_to_execute = total_chunks * chunk_size;

        let group_size = odd_rounding_div_ceil(rows_to_execute, pool.amount, chunk_size);
        if group_size == rows_to_execute {
            for chunk in slice.chunks_mut(chunk_size) {
                f(chunk);
            }
            return;
        }

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut chunks = slice.chunks_mut(group_size);
            let first_group = chunks.next();

            for chunk in chunks {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);

                let job = Arc::clone(&job);
                let core = cores.get(offset % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    for chunk in chunk.chunks_mut(self.chunk_size) {
                        job(chunk);
                    }
                });
            }

            if let Some(first_group) = first_group {
                for chunk in first_group.chunks_mut(self.chunk_size) {
                    job(chunk);
                }
            }
        });
    }

    fn for_each_enumerated<'scope, F>(mut self, thread_pool: &ThreadPool, f: F)
    where
        F: 'scope + Fn(usize, Self::Item) + Send + Sync,
    {
        if thread_pool.amount <= 1 {
            for (i, chunk) in self.slice.chunks_mut(self.chunk_size).enumerate() {
                f(i, chunk);
            }
            return;
        }
        let chunk_size = self.chunk_size;
        let slice = std::mem::take(&mut self.slice); // take ownership
        let total_chunks = slice.len() / chunk_size;

        let rows_to_execute = total_chunks * chunk_size;

        let group_size = odd_rounding_div_ceil(rows_to_execute, thread_pool.amount, chunk_size);
        if group_size == rows_to_execute {
            for (i, chunk) in slice.chunks_mut(chunk_size).enumerate() {
                f(i, chunk);
            }
            return;
        }

        thread::scope(|s| {
            let job = Arc::new(f);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let cores_len = cores.len().max(1);

            let mut chunks = slice.chunks_mut(group_size);
            let first_group = chunks.next();

            for (z, chunk) in chunks.enumerate() {
                let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);

                let job = Arc::clone(&job);
                let core = cores.get((z.wrapping_add(offset)) % cores_len).cloned();

                s.spawn(move || {
                    if let Some(core_id) = core {
                        core_affinity::set_for_current(core_id);
                    }
                    let base_id = z + 1;
                    for (i, chunk) in chunk.chunks_mut(chunk_size).enumerate() {
                        job(base_id + i, chunk);
                    }
                });
            }

            if let Some(first_group) = first_group {
                for (i, chunk) in first_group.chunks_mut(chunk_size).enumerate() {
                    job(i, chunk);
                }
            }
        });
    }
}
