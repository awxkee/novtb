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
use crate::{CORE_RING_INDEX, ThreadPool, odd_rounding_div_ceil};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;

pub fn parallel_range<'scope, F>(thread_pool: &ThreadPool, cap: usize, f: F)
where
    F: 'scope + Fn(usize) + Send + Sync,
{
    if cap == 0 {
        return;
    }
    if thread_pool.amount <= 1 {
        (0..cap).for_each(f);
        return;
    }

    let group_size = odd_rounding_div_ceil(cap, thread_pool.amount, 1);
    if group_size >= cap {
        (0..cap).for_each(f);
        return;
    }

    thread::scope(|s| {
        let job = Arc::new(f);
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let cores_len = cores.len().max(1);

        let mut base_offset = 0usize;
        let mut remaining = cap;
        let mut first_group: Option<(usize, usize)> = None; // (start, end)

        let mut groups: Vec<(usize, usize)> = Vec::new();
        while remaining > 0 {
            let this_group = group_size.min(remaining);
            let end = base_offset + this_group;
            if first_group.is_none() {
                first_group = Some((base_offset, end));
            } else {
                groups.push((base_offset, end));
            }
            base_offset += this_group;
            remaining = remaining.saturating_sub(this_group);
        }

        for (start, end) in groups {
            let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
            let job = Arc::clone(&job);
            let core = cores.get(offset % cores_len).cloned();

            s.spawn(move || {
                if let Some(core_id) = core {
                    core_affinity::set_for_current(core_id);
                }
                (start..end).for_each(|i| job(i));
            });
        }

        // first group runs on calling thread
        if let Some((start, end)) = first_group {
            (start..end).for_each(|i| job(i));
        }
    });
}

pub fn parallel_range_with_context<'scope, F, C, CF>(
    thread_pool: &ThreadPool,
    cap: usize,
    make_context: CF,
    f: F,
) where
    F: 'scope + Fn(usize, &mut C) + Send + Sync,
    C: Send,
    CF: Fn() -> C + Send + Sync,
{
    if cap == 0 {
        return;
    }
    if thread_pool.amount <= 1 {
        let mut ctx = make_context();
        (0..cap).for_each(|i| f(i, &mut ctx));
        return;
    }

    let group_size = odd_rounding_div_ceil(cap, thread_pool.amount, 1);
    if group_size >= cap {
        let mut ctx = make_context();
        (0..cap).for_each(|i| f(i, &mut ctx));
        return;
    }

    thread::scope(|s| {
        let job = Arc::new(f);
        let make_context = Arc::new(make_context);
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let cores_len = cores.len().max(1);

        let mut base_offset = 0usize;
        let mut remaining = cap;
        let mut first_group: Option<(usize, usize)> = None;
        let mut groups: Vec<(usize, usize)> = Vec::new();

        while remaining > 0 {
            let this_group = group_size.min(remaining);
            let end = base_offset + this_group;
            if first_group.is_none() {
                first_group = Some((base_offset, end));
            } else {
                groups.push((base_offset, end));
            }
            base_offset += this_group;
            remaining = remaining.saturating_sub(this_group);
        }

        for (start, end) in groups {
            let offset = CORE_RING_INDEX.fetch_add(1, Ordering::Relaxed);
            let job = Arc::clone(&job);
            let make_context = Arc::clone(&make_context);
            let core = cores.get(offset % cores_len).cloned();

            s.spawn(move || {
                if let Some(core_id) = core {
                    core_affinity::set_for_current(core_id);
                }
                // Each thread allocates its context once and reuses it
                let mut ctx = make_context();
                (start..end).for_each(|i| job(i, &mut ctx));
            });
        }

        if let Some((start, end)) = first_group {
            let mut ctx = make_context();
            (start..end).for_each(|i| job(i, &mut ctx));
        }
    });
}
