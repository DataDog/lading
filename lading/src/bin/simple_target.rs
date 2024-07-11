//! A simple target for lading that runs forever

use std::{thread, time};

pub fn main() {
    loop {
        thread::sleep(time::Duration::from_secs(60));
    }
}
