# novtb – Simple Zoned Data-Parallelism with Core Affinity

novtb provides a lightweight, brute-force data-parallel execution model with support for core pinning (CPU affinity). It's useful for workloads where threads should be pinned to specific cores for improved cache locality or performance consistency.

# Features

- Zone-based parallel iteration over chunked data
- Explicit thread pool with core affinity
- Simple API without complex task graphs or scheduling overhead

# Some examples

```rust
let pool = novtb::ThreadPool::new(thread_count as usize);
pool.parallel_for(|thread_index| {
    // Perform thread-specific heavy work here
);
```

```rust
let pool = novtb::ThreadPool::new(thread_count as usize);
dst.tb_par_chunks_mut(dst_stride as usize * tile_size)
    .for_each_enumerated(&pool, |cy, dst_rows| {
    // Process a tile at row 'cy' in 'dst_rows'
    });
```

This project is licensed under either of

- BSD-3-Clause License (see [LICENSE](LICENSE.md))
- Apache License, Version 2.0 (see [LICENSE](LICENSE-APACHE.md))

at your option.
