pub(crate) mod smaps;
/// Memory metrics from /proc/[pid]/smaps_rollup.
pub mod smaps_rollup;

const BYTES_PER_KIBIBYTE: u64 = 1024;

fn next_token<'a>(
    source: &'a str,
    iter: &'_ mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> Option<&'a str> {
    while let Some((_, c)) = iter.peek() {
        if !c.is_whitespace() {
            break;
        }
        iter.next();
    }
    let start = iter.peek()?.0;
    while let Some((_, c)) = iter.peek() {
        if c.is_whitespace() {
            break;
        }
        iter.next();
    }
    let end = iter.peek().map_or_else(|| source.len(), |&(idx, _)| idx);
    Some(&source[start..end])
}
