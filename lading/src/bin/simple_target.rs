//! A simple target for lading that runs forever

use jemallocator::Jemalloc;
use std::{thread, time};
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn main() {
    loop {
        thread::sleep(time::Duration::from_secs(60));
    }
}
