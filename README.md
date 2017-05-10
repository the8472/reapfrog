[![Version](https://img.shields.io/crates/v/reapfrog.svg)](https://crates.io/crates/reapfrog)

# reapfrog

The library optimizes single-pass reading of many small files by taking a path
iterator as input and returning `Read` implementations for each file that automatically
schedule `posix_fadvise` readaheads for the following files to always keep a prefetch window
ahead of the current read position, even across files.

Can also perform dropbehind to avoid cluttering the disk caches, but this is optional since
it might interfere with other processes accessing those files at the same time.

