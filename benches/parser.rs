use std::{env, fs, hint::black_box, path::PathBuf, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdbinsight::parser::{RDBFileParser, core::buffer::Buffer, error::NeedMoreData};

const DEFAULT_GENERATED_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_PROFILES: &[&str] = &["string", "list", "set", "hash", "zset", "zset2", "mixed"];

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

fn push_string_record(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    out.push(0x00); // RDB_TYPE_STRING.
    push_rdb_str(out, key);
    push_rdb_str(out, value);
}

fn push_list_record(out: &mut Vec<u8>, key: &[u8], values: &[&[u8]]) {
    out.push(0x01); // RDB_TYPE_LIST.
    push_rdb_str(out, key);
    push_rdb_len(out, values.len());
    for value in values {
        push_rdb_str(out, value);
    }
}

fn push_set_record(out: &mut Vec<u8>, key: &[u8], values: &[&[u8]]) {
    out.push(0x02); // RDB_TYPE_SET.
    push_rdb_str(out, key);
    push_rdb_len(out, values.len());
    for value in values {
        push_rdb_str(out, value);
    }
}

fn push_zset_record(out: &mut Vec<u8>, key: &[u8], values: &[(&[u8], &[u8])]) {
    out.push(0x03); // RDB_TYPE_ZSET.
    push_rdb_str(out, key);
    push_rdb_len(out, values.len());
    for (member, score) in values {
        push_rdb_str(out, member);
        push_rdb_str(out, score);
    }
}

fn push_zset2_record(out: &mut Vec<u8>, key: &[u8], values: &[(&[u8], f64)]) {
    out.push(0x05); // RDB_TYPE_ZSET_2.
    push_rdb_str(out, key);
    push_rdb_len(out, values.len());
    for (member, score) in values {
        push_rdb_str(out, member);
        out.extend_from_slice(&score.to_le_bytes());
    }
}

fn push_hash_record(out: &mut Vec<u8>, key: &[u8], pairs: &[(&[u8], &[u8])]) {
    out.push(0x04); // RDB_TYPE_HASH.
    push_rdb_str(out, key);
    push_rdb_len(out, pairs.len());
    for (field, value) in pairs {
        push_rdb_str(out, field);
        push_rdb_str(out, value);
    }
}

fn push_expire_ms(out: &mut Vec<u8>, expire_at_ms: u64) {
    out.push(0xfc); // RDB_OPCODE_EXPIRETIME_MS.
    out.extend_from_slice(&expire_at_ms.to_le_bytes());
}

fn push_select_db(out: &mut Vec<u8>, db: usize) {
    out.push(0xfe); // RDB_OPCODE_SELECTDB.
    push_rdb_len(out, db);
}

fn push_resize_db(out: &mut Vec<u8>, keys: usize, expires: usize) {
    out.push(0xfb); // RDB_OPCODE_RESIZEDB.
    push_rdb_len(out, keys);
    push_rdb_len(out, expires);
}

fn synthetic_string_rdb(target_bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(target_bytes + 1024);
    out.extend_from_slice(b"REDIS0011");

    let value = vec![b'x'; 1024];
    let mut index = 0_u64;
    while out.len() < target_bytes {
        let key = format!("bench:key:{index:016}");
        push_string_record(&mut out, key.as_bytes(), &value);
        index += 1;
    }

    out.push(0xff); // EOF.
    out.extend_from_slice(&0_u64.to_le_bytes()); // Checksum placeholder; parser does not validate it.
    out
}

fn synthetic_repeated_rdb<F>(target_bytes: usize, profile: &str, mut push_record: F) -> Vec<u8>
where F: FnMut(&mut Vec<u8>, &[u8], u64) {
    let mut out = Vec::with_capacity(target_bytes + 1024);
    out.extend_from_slice(b"REDIS0011");

    let mut index = 0_u64;
    while out.len() < target_bytes {
        let key = format!("bench:{profile}:{index:016}");
        push_record(&mut out, key.as_bytes(), index);
        index += 1;
    }

    out.push(0xff); // EOF.
    out.extend_from_slice(&0_u64.to_le_bytes()); // Checksum placeholder; parser does not validate it.
    out
}

fn synthetic_list_rdb(target_bytes: usize) -> Vec<u8> {
    let members: [&[u8]; 8] = [
        b"alpha", b"bravo", b"charlie", b"delta", b"echo", b"foxtrot", b"golf", b"hotel",
    ];
    synthetic_repeated_rdb(target_bytes, "list", |out, key, _| {
        push_list_record(out, key, &members);
    })
}

fn synthetic_set_rdb(target_bytes: usize) -> Vec<u8> {
    let members: [&[u8]; 8] = [
        b"alpha", b"bravo", b"charlie", b"delta", b"echo", b"foxtrot", b"golf", b"hotel",
    ];
    synthetic_repeated_rdb(target_bytes, "set", |out, key, _| {
        push_set_record(out, key, &members);
    })
}

fn synthetic_hash_rdb(target_bytes: usize) -> Vec<u8> {
    let pairs: [(&[u8], &[u8]); 8] = [
        (b"name", b"benchmark"),
        (b"region", b"ci"),
        (b"owner", b"parser"),
        (b"state", b"active"),
        (b"tier", b"hot"),
        (b"format", b"rdb"),
        (b"version", b"0011"),
        (b"profile", b"hash"),
    ];
    synthetic_repeated_rdb(target_bytes, "hash", |out, key, _| {
        push_hash_record(out, key, &pairs);
    })
}

fn synthetic_zset_rdb(target_bytes: usize) -> Vec<u8> {
    let scores: [(&[u8], &[u8]); 8] = [
        (b"alpha", b"1.25"),
        (b"bravo", b"2.50"),
        (b"charlie", b"3.75"),
        (b"delta", b"4.00"),
        (b"echo", b"5.25"),
        (b"foxtrot", b"6.50"),
        (b"golf", b"7.75"),
        (b"hotel", b"8.00"),
    ];
    synthetic_repeated_rdb(target_bytes, "zset", |out, key, _| {
        push_zset_record(out, key, &scores);
    })
}

fn synthetic_zset2_rdb(target_bytes: usize) -> Vec<u8> {
    let scores: [(&[u8], f64); 8] = [
        (b"alpha", 1.25),
        (b"bravo", 2.50),
        (b"charlie", 3.75),
        (b"delta", 4.00),
        (b"echo", 5.25),
        (b"foxtrot", 6.50),
        (b"golf", 7.75),
        (b"hotel", 8.00),
    ];
    synthetic_repeated_rdb(target_bytes, "zset2", |out, key, _| {
        push_zset2_record(out, key, &scores);
    })
}

fn synthetic_mixed_rdb(target_bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(target_bytes + 1024);
    out.extend_from_slice(b"REDIS0011");
    push_select_db(&mut out, 0);
    push_resize_db(&mut out, 1024, 128);

    let large_value = vec![b'x'; 1024];
    let members: [&[u8]; 8] = [
        b"alpha", b"bravo", b"charlie", b"delta", b"echo", b"foxtrot", b"golf", b"hotel",
    ];
    let zset_scores: [(&[u8], &[u8]); 8] = [
        (b"alpha", b"1.25"),
        (b"bravo", b"2.50"),
        (b"charlie", b"3.75"),
        (b"delta", b"4.00"),
        (b"echo", b"5.25"),
        (b"foxtrot", b"6.50"),
        (b"golf", b"7.75"),
        (b"hotel", b"8.00"),
    ];
    let zset2_scores: [(&[u8], f64); 8] = [
        (b"alpha", 1.25),
        (b"bravo", 2.50),
        (b"charlie", 3.75),
        (b"delta", 4.00),
        (b"echo", 5.25),
        (b"foxtrot", 6.50),
        (b"golf", 7.75),
        (b"hotel", 8.00),
    ];
    let hash_pairs: [(&[u8], &[u8]); 8] = [
        (b"name", b"benchmark"),
        (b"region", b"ci"),
        (b"owner", b"parser"),
        (b"state", b"active"),
        (b"tier", b"hot"),
        (b"format", b"rdb"),
        (b"version", b"0011"),
        (b"profile", b"mixed"),
    ];

    let mut index = 0_u64;
    while out.len() < target_bytes {
        let key = format!("bench:mixed:{index:016}");
        if index.is_multiple_of(10) {
            push_expire_ms(&mut out, 4_102_444_800_000);
        }

        match index % 6 {
            0 => push_string_record(&mut out, key.as_bytes(), &large_value),
            1 => push_list_record(&mut out, key.as_bytes(), &members),
            2 => push_set_record(&mut out, key.as_bytes(), &members),
            3 => push_zset_record(&mut out, key.as_bytes(), &zset_scores),
            4 => push_zset2_record(&mut out, key.as_bytes(), &zset2_scores),
            _ => push_hash_record(&mut out, key.as_bytes(), &hash_pairs),
        }
        index += 1;
    }

    out.push(0xff); // EOF.
    out.extend_from_slice(&0_u64.to_le_bytes()); // Checksum placeholder; parser does not validate it.
    out
}

fn generated_benchmark_input(target_bytes: usize, profile: &str) -> (String, Vec<u8>) {
    let target_mib = target_bytes / 1024 / 1024;

    match profile {
        "string" => (
            format!("synthetic-string-records-{target_mib}MiB"),
            synthetic_string_rdb(target_bytes),
        ),
        "list" => (
            format!("synthetic-list-records-{target_mib}MiB"),
            synthetic_list_rdb(target_bytes),
        ),
        "set" => (
            format!("synthetic-set-records-{target_mib}MiB"),
            synthetic_set_rdb(target_bytes),
        ),
        "hash" => (
            format!("synthetic-hash-records-{target_mib}MiB"),
            synthetic_hash_rdb(target_bytes),
        ),
        "zset" => (
            format!("synthetic-zset-records-{target_mib}MiB"),
            synthetic_zset_rdb(target_bytes),
        ),
        "zset2" => (
            format!("synthetic-zset2-records-{target_mib}MiB"),
            synthetic_zset2_rdb(target_bytes),
        ),
        "mixed" => (
            format!("synthetic-mixed-raw-types-{target_mib}MiB"),
            synthetic_mixed_rdb(target_bytes),
        ),
        other => {
            panic!(
                "unsupported RDBINSIGHT_BENCH_PROFILE {other:?}; expected one of {}",
                DEFAULT_PROFILES.join(", ")
            )
        }
    }
}

fn benchmark_inputs() -> Vec<(String, Vec<u8>)> {
    if let Ok(path) = env::var("RDBINSIGHT_BENCH_RDB") {
        let path = PathBuf::from(path);
        let label = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("custom-rdb")
            .to_owned();
        let data = fs::read(&path)
            .unwrap_or_else(|err| panic!("failed to read benchmark RDB {}: {err}", path.display()));
        return vec![(label, data)];
    }

    let target_bytes = env_usize("RDBINSIGHT_BENCH_GENERATED_BYTES", DEFAULT_GENERATED_BYTES);
    let profiles = env::var("RDBINSIGHT_BENCH_PROFILES")
        .or_else(|_| env::var("RDBINSIGHT_BENCH_PROFILE"))
        .ok()
        .map(|profiles| {
            profiles
                .split(',')
                .map(str::trim)
                .filter(|profile| !profile.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|profiles| !profiles.is_empty())
        .unwrap_or_else(|| {
            DEFAULT_PROFILES
                .iter()
                .map(|profile| (*profile).to_owned())
                .collect()
        });

    profiles
        .iter()
        .map(|profile| generated_benchmark_input(target_bytes, profile))
        .collect()
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
    let inputs = benchmark_inputs();
    let chunk_size = env_usize("RDBINSIGHT_BENCH_CHUNK_BYTES", DEFAULT_CHUNK_SIZE);
    let buffer_size = env_usize("RDBINSIGHT_BENCH_BUFFER_BYTES", DEFAULT_BUFFER_SIZE);

    let mut group = c.benchmark_group("parser");
    for (label, data) in &inputs {
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("parse_rdb", label),
            data,
            |bench, input| {
                bench.iter(|| parse_rdb(black_box(input), chunk_size, buffer_size));
            },
        );
    }
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
