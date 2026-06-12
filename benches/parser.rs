use std::{env, fs, hint::black_box, path::PathBuf, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdbinsight::parser::{RDBFileParser, core::buffer::Buffer, error::NeedMoreData};

const DEFAULT_GENERATED_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_duration(name: &str, default_secs: u64) -> Duration {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(default_secs))
}

fn push_rdb_len(out: &mut Vec<u8>, len: usize) {
    match len {
        0..=0x3f => out.push(len as u8),
        0x40..=0x3fff => {
            out.push(((len >> 8) as u8) | 0x40);
            out.push(len as u8);
        }
        _ => {
            out.push(0x80);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
}

fn push_rdb_str(out: &mut Vec<u8>, value: &[u8]) {
    push_rdb_len(out, value.len());
    out.extend_from_slice(value);
}

fn synthetic_rdb(target_bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(target_bytes + 1024);
    out.extend_from_slice(b"REDIS0011");

    let value = vec![b'x'; 1024];
    let mut index = 0_u64;
    while out.len() < target_bytes {
        out.push(0x00); // String value type.
        let key = format!("bench:key:{index:016}");
        push_rdb_str(&mut out, key.as_bytes());
        push_rdb_str(&mut out, &value);
        index += 1;
    }

    out.push(0xff); // EOF.
    out.extend_from_slice(&0_u64.to_le_bytes()); // Checksum placeholder; parser does not validate it.
    out
}

fn benchmark_input() -> (String, Vec<u8>) {
    if let Ok(path) = env::var("RDBINSIGHT_BENCH_RDB") {
        let path = PathBuf::from(path);
        let label = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("custom-rdb")
            .to_owned();
        let data = fs::read(&path)
            .unwrap_or_else(|err| panic!("failed to read benchmark RDB {}: {err}", path.display()));
        return (label, data);
    }

    let target_bytes = env_usize("RDBINSIGHT_BENCH_GENERATED_BYTES", DEFAULT_GENERATED_BYTES);
    (
        format!("synthetic-string-records-{}MiB", target_bytes / 1024 / 1024),
        synthetic_rdb(target_bytes),
    )
}

fn parse_rdb(data: &[u8], chunk_size: usize, buffer_size: usize) -> usize {
    let mut parser = RDBFileParser::default();
    let mut buffer = Buffer::new(buffer_size);
    let mut offset = 0;
    let mut item_count = 0;

    loop {
        match parser.poll_next(&mut buffer) {
            Ok(Some(item)) => {
                black_box(item);
                item_count += 1;
            }
            Ok(None) => {
                assert_eq!(buffer.len(), 0, "parser finished with unconsumed bytes");
                return item_count;
            }
            Err(err) if err.is::<NeedMoreData>() => {
                if buffer.is_finished() {
                    panic!("parser requested more data after input was marked finished");
                }
                if offset == data.len() {
                    buffer.set_finished();
                    continue;
                }

                let read_len = chunk_size
                    .min(buffer.remain_capacity())
                    .min(data.len() - offset);
                assert!(
                    read_len > 0,
                    "parser buffer is full; increase RDBINSIGHT_BENCH_BUFFER_BYTES"
                );
                buffer
                    .extend(&data[offset..offset + read_len])
                    .expect("benchmark buffer should have enough capacity");
                offset += read_len;
            }
            Err(err) => panic!("parser failed: {err:#}"),
        }
    }
}

fn bench_parser(c: &mut Criterion) {
    let (label, data) = benchmark_input();
    let chunk_size = env_usize("RDBINSIGHT_BENCH_CHUNK_BYTES", DEFAULT_CHUNK_SIZE);
    let buffer_size = env_usize("RDBINSIGHT_BENCH_BUFFER_BYTES", DEFAULT_BUFFER_SIZE);

    let mut group = c.benchmark_group("parser");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("parse_rdb", label),
        &data,
        |bench, input| {
            bench.iter(|| parse_rdb(black_box(input), chunk_size, buffer_size));
        },
    );
    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(env_usize("RDBINSIGHT_BENCH_SAMPLE_SIZE", 10).max(10))
        .warm_up_time(env_duration("RDBINSIGHT_BENCH_WARM_UP_SECS", 1))
        .measurement_time(env_duration("RDBINSIGHT_BENCH_MEASUREMENT_SECS", 1))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_parser
}
criterion_main!(benches);
