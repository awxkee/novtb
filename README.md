# Simple Zoned Data-Parallelism with Core Affinity

Library does simple brute-force region data-parallelism with core pinning. 

# Some examples

```rust
let pool = novtb::ThreadPool::new(thread_count as usize);
pool.parallel_for(|thread_index| {
    // some heavy work
);
```

```rust
let pool = novtb::ThreadPool::new(thread_count as usize);
dst.tb_par_chunks_mut(dst_stride as usize * tile_size)
    .for_each_enumerated(&pool, |cy, dst_rows| {
        // some heavy work
    });
```

This project is licensed under either of

- BSD-3-Clause License (see [LICENSE](LICENSE.md))
- Apache License, Version 2.0 (see [LICENSE](LICENSE-APACHE.md))

at your option.
