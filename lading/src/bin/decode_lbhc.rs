//! Decode a `.lbhc` (Lading BlackHole Capture) file to human-readable output.
//!
//! Usage:
//!   decode_lbhc <path.lbhc> [--summary | --full | --json] [--out-dir <dir>]
//!
//! Modes:
//!   --summary (default): one line per record with metadata + a short payload preview.
//!   --full:              full payload printed after each record's metadata.
//!   --json:              JSON Lines, one object per record with payload as a
//!                        UTF-8 (lossy) string.
//!
//! Redirection:
//!   To capture the mode's output to a file, use shell redirection:
//!     decode_lbhc file.lbhc --full > out.txt
//!
//!   To dump each record's raw decoded payload to its own file (one file
//!   per record, named `NNNN.payload`), pass `--out-dir <dir>`. The
//!   selected mode still prints to stdout. Combine e.g.:
//!     decode_lbhc file.lbhc --summary --out-dir ./payloads > index.txt

use std::{
    env,
    fs::{self, File},
    io::{BufReader, ErrorKind, Read, Write},
    path::PathBuf,
    process,
};

/// Local copy of the proto struct so this bin doesn't need access to
/// `pub(crate)` proto module in the lading lib. Field tags and types
/// must match `proto/blackhole_capture.proto`.
#[derive(Clone, PartialEq, Eq, prost::Message)]
struct BlackholeCaptureRecord {
    #[prost(uint64, tag = "1")]
    relative_ms: u64,
    #[prost(uint64, tag = "2")]
    compressed_bytes: u64,
    #[prost(string, tag = "3")]
    content_type: String,
    #[prost(string, tag = "4")]
    request_path: String,
    #[prost(bytes = "vec", tag = "5")]
    payload: Vec<u8>,
}

#[derive(Copy, Clone)]
enum Mode {
    Summary,
    Full,
    Json,
}

fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!(
            "usage: decode_lbhc <path.lbhc> [--summary|--full|--json] [--out-dir <dir>]"
        );
        process::exit(2);
    }
    let path = args.remove(0);
    let mut mode = Mode::Summary;
    let mut out_dir: Option<PathBuf> = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--summary" => mode = Mode::Summary,
            "--full" => mode = Mode::Full,
            "--json" => mode = Mode::Json,
            "--out-dir" => match iter.next() {
                Some(d) => out_dir = Some(PathBuf::from(d)),
                None => {
                    eprintln!("--out-dir requires a directory argument");
                    process::exit(2);
                }
            },
            other => {
                eprintln!("unknown flag: {other}");
                process::exit(2);
            }
        }
    }
    if let Some(ref dir) = out_dir
        && let Err(e) = fs::create_dir_all(dir)
    {
        eprintln!("create --out-dir {}: {e}", dir.display());
        process::exit(1);
    }

    let f = File::open(&path).unwrap_or_else(|e| {
        eprintln!("open {path}: {e}");
        process::exit(1);
    });
    let mut reader = BufReader::new(f);

    // Prologue: 4 bytes magic "LBHC" + u16 LE version + u16 LE reserved.
    let mut prologue = [0u8; 8];
    if let Err(e) = reader.read_exact(&mut prologue) {
        eprintln!("read prologue: {e}");
        process::exit(1);
    }
    if &prologue[0..4] != b"LBHC" {
        eprintln!("bad magic: {:02x?}", &prologue[0..4]);
        process::exit(1);
    }
    let version = u16::from_le_bytes([prologue[4], prologue[5]]);
    if version != 1 {
        eprintln!("unsupported file version: {version}");
        process::exit(1);
    }
    eprintln!("# LBHC v{version}, decoding {path}");

    let mut seq: u64 = 0;
    loop {
        let bytes = match read_length_delimited(&mut reader) {
            Ok(Some(b)) => b,
            Ok(None) => break,
            Err(e) => {
                eprintln!("record {seq}: framing error: {e}");
                process::exit(1);
            }
        };
        let record: BlackholeCaptureRecord = match prost::Message::decode(&bytes[..]) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("record {seq}: decode error: {e}");
                process::exit(1);
            }
        };
        match mode {
            Mode::Summary => print_summary(seq, &record),
            Mode::Full => print_full(seq, &record),
            Mode::Json => print_json(seq, &record),
        }
        if let Some(ref dir) = out_dir {
            if let Err(e) = dump_payload(dir, seq, &record) {
                eprintln!("record {seq}: dump failed: {e}");
                process::exit(1);
            }
        }
        seq = seq.saturating_add(1);
    }
    eprintln!("# {seq} records total");
    if let Some(dir) = out_dir {
        eprintln!("# payloads written to {}", dir.display());
    }
}

/// Write one record's raw decoded payload to `<dir>/NNNN.payload`. If the
/// payload parses as JSON, write pretty-printed JSON to `NNNN.json`
/// instead so it's directly `jq`-able / browsable.
fn dump_payload(
    dir: &std::path::Path,
    seq: u64,
    r: &BlackholeCaptureRecord,
) -> std::io::Result<()> {
    let (ext, contents): (&str, Vec<u8>) =
        match serde_json::from_slice::<serde_json::Value>(&r.payload) {
            Ok(v) => {
                let pretty = serde_json::to_vec_pretty(&v).unwrap_or_else(|_| r.payload.clone());
                ("json", pretty)
            }
            Err(_) => ("payload", r.payload.clone()),
        };
    let name = format!("{seq:04}.{ext}");
    let mut path = dir.to_path_buf();
    path.push(name);
    let mut f = File::create(&path)?;
    f.write_all(&contents)?;
    Ok(())
}

/// Read one length-delimited proto message. `Ok(None)` at clean EOF or
/// truncated tail; `Err` on I/O failure or malformed varint.
fn read_length_delimited<R: Read>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut varint_buf = [0u8; 10];
    let mut varint_len = 0;
    loop {
        if varint_len >= varint_buf.len() {
            return Ok(None); // overlong varint => corrupt tail, stop
        }
        let mut byte = [0u8; 1];
        match reader.read(&mut byte) {
            Ok(0) if varint_len == 0 => return Ok(None), // clean EOF
            Ok(0) => return Ok(None),                    // truncated varint
            Ok(_) => {
                varint_buf[varint_len] = byte[0];
                varint_len += 1;
                if byte[0] & 0x80 == 0 {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    let mut slice = &varint_buf[..varint_len];
    let msg_len = match prost::encoding::decode_varint(&mut slice) {
        Ok(v) => v as usize,
        Err(e) => return Err(std::io::Error::new(ErrorKind::InvalidData, e)),
    };
    let mut buf = vec![0u8; msg_len];
    if !read_full(reader, &mut buf)? {
        return Ok(None); // truncated body
    }
    Ok(Some(buf))
}

fn read_full<R: Read>(reader: &mut R, mut buf: &mut [u8]) -> std::io::Result<bool> {
    while !buf.is_empty() {
        match reader.read(buf) {
            Ok(0) => return Ok(false),
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

fn preview(payload: &[u8], max: usize) -> String {
    let take = payload.len().min(max);
    let s = String::from_utf8_lossy(&payload[..take]).into_owned();
    if payload.len() > max {
        format!("{s}...[+{} bytes]", payload.len() - max)
    } else {
        s
    }
}

fn print_summary(seq: u64, r: &BlackholeCaptureRecord) {
    println!(
        "#{seq:04} t={:>7}ms compressed={:>7}B decoded={:>7}B path={} ctype={} payload={}",
        r.relative_ms,
        r.compressed_bytes,
        r.payload.len(),
        r.request_path,
        r.content_type,
        preview(&r.payload, 120),
    );
}

fn print_full(seq: u64, r: &BlackholeCaptureRecord) {
    println!(
        "#{seq:04} t={}ms compressed_bytes={} decoded_bytes={} path={} content_type={}",
        r.relative_ms,
        r.compressed_bytes,
        r.payload.len(),
        r.request_path,
        r.content_type,
    );
    // If payload parses as JSON, pretty-print. Otherwise lossy UTF-8.
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&r.payload) {
        let pretty = serde_json::to_string_pretty(&value).unwrap_or_default();
        println!("{pretty}");
    } else {
        println!("{}", String::from_utf8_lossy(&r.payload));
    }
    println!();
}

fn print_json(seq: u64, r: &BlackholeCaptureRecord) {
    // Preserve the raw payload string as-is (UTF-8 lossy) inside a JSON
    // string field so downstream tools can pipe this to jq.
    let obj = serde_json::json!({
        "seq": seq,
        "relative_ms": r.relative_ms,
        "compressed_bytes": r.compressed_bytes,
        "content_type": r.content_type,
        "request_path": r.request_path,
        "payload": String::from_utf8_lossy(&r.payload),
    });
    println!("{obj}");
}
