use fastrand::Rng;

#[inline]
pub fn fill_constant_buffer(_: &Rng, buffer: &mut [u8]) {
    buffer.iter_mut().for_each(|c| *c = b'A');
}

#[inline]
pub fn fill_ascii_buffer(rng: &Rng, buffer: &mut [u8]) {
    buffer.iter_mut().for_each(|c| *c = rng.u8(32..=126));
}
