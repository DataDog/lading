use rand::Rng;

#[inline]
pub fn fill_constant_buffer<R>(_: &mut R, buffer: &mut [u8])
where
    R: Rng + Sized,
{
    buffer.iter_mut().for_each(|c| *c = b'A');
}

const CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,/\\{}[];:'\"";
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[inline]
pub fn fill_ascii_buffer<R>(rng: &mut R, buffer: &mut [u8])
where
    R: Rng + Sized,
{
    rng.fill_bytes(buffer);
    buffer
        .iter_mut()
        .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
}
