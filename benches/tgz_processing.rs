//! Benchmarks for TGZ processing pipeline stages.
//!
//! Run with: cargo bench
//!
//! For comparing backends, modify Cargo.toml flate2 features and re-run.

use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, Read},
    thread,
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use quick_xml::de::Deserializer;
use rayon::prelude::*;
use serde::Deserialize;
use tar::Archive;

// Minimal structs for XML parsing benchmark (mirrors lib.rs)
#[derive(Debug, Default, Deserialize)]
struct Identifier {
    #[serde(rename = "uri")]
    _uri: String,
    #[serde(rename = "path")]
    _path: String,
}

#[derive(Debug, Default, Deserialize)]
struct PersonName {
    #[serde(rename = "given-names")]
    _given_names: Option<String>,
    #[serde(rename = "family-name")]
    _family_name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Person {
    _name: PersonName,
}

#[derive(Debug, Default, Deserialize)]
struct OrgIdentifier {
    #[serde(alias = "disambiguated-organization-identifier")]
    _identifier: String,
    #[serde(alias = "disambiguation-source")]
    _source: String,
}

#[derive(Debug, Default, Deserialize)]
struct Organization {
    _name: String,
    #[serde(alias = "disambiguated-organization")]
    _identifier: Option<OrgIdentifier>,
}

#[derive(Debug, Default, Deserialize)]
struct Employment {
    #[serde(alias = "end-date")]
    _end: Option<()>,
    _organization: Organization,
}

#[derive(Debug, Default, Deserialize)]
struct AffiliationGroup {
    #[serde(alias = "employment-summary")]
    _employment: Employment,
}

#[derive(Debug, Default, Deserialize)]
struct Employments {
    #[serde(alias = "affiliation-group")]
    _employment: Option<Vec<AffiliationGroup>>,
}

#[derive(Debug, Default, Deserialize)]
struct Activities {
    _employments: Employments,
}

#[derive(Debug, Default, Deserialize)]
struct Record {
    #[serde(alias = "orcid-identifier")]
    _identifier: Identifier,
    _person: Person,
    #[serde(alias = "activities-summary")]
    _activities: Activities,
}

/// Find test TGZ file. Uses the committed fixture by default for reproducibility.
fn find_test_tgz() -> Option<std::path::PathBuf> {
    let candidates = [
        "tests/data/bench-fixture.tar.gz", // ~10MB, committed fixture for CI
        "var/summaries-2025-large.tar.gz", // ~320MB, for thorough local testing
        "var/summaries-2025-partial.tar.gz", // ~26MB
    ];

    for candidate in candidates {
        let path = std::path::PathBuf::from(candidate);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Benchmark gzip decompression only.
fn bench_gzip_decompression(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found in var/. Skipping decompression benchmark.");
        return;
    };

    let file_size = std::fs::metadata(&tgz_path).unwrap().len();
    let mut group = c.benchmark_group("gzip_decompression");
    group.throughput(Throughput::Bytes(file_size));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "decompress",
            format!("{:.0}MB", file_size as f64 / 1024.0 / 1024.0),
        ),
        &tgz_path,
        |b, path| {
            b.iter(|| {
                let file = File::open(path).unwrap();
                let mut decoder = GzDecoder::new(BufReader::new(file));
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).unwrap();
                black_box(decompressed.len())
            });
        },
    );

    group.finish();
}

/// Benchmark tar iteration and XML reading (combined, since streaming).
fn bench_tar_iteration(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found in var/. Skipping tar iteration benchmark.");
        return;
    };

    let file_size = std::fs::metadata(&tgz_path).unwrap().len();
    let mut group = c.benchmark_group("tar_iteration");
    group.throughput(Throughput::Bytes(file_size));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "iterate_and_read",
            format!("{:.0}MB", file_size as f64 / 1024.0 / 1024.0),
        ),
        &tgz_path,
        |b, path| {
            b.iter(|| {
                let file = File::open(path).unwrap();
                let mut archive = Archive::new(GzDecoder::new(BufReader::new(file)));
                let mut xml_count = 0usize;
                let mut xml_bytes = 0usize;

                for entry_result in archive.entries().unwrap() {
                    let Ok(mut entry) = entry_result else {
                        continue;
                    };
                    let Ok(entry_path) = entry.path() else {
                        continue;
                    };

                    if entry_path.extension().and_then(OsStr::to_str) == Some("xml") {
                        let mut content = String::new();
                        if entry.read_to_string(&mut content).is_ok() {
                            xml_bytes += content.len();
                            xml_count += 1;
                        }
                    }
                }
                black_box((xml_count, xml_bytes))
            });
        },
    );

    group.finish();
}

/// Pre-load XML contents from test TGZ.
fn load_xml_contents() -> Option<(Vec<String>, usize, usize)> {
    let tgz_path = find_test_tgz()?;
    let file = File::open(&tgz_path).unwrap();
    let mut archive = Archive::new(GzDecoder::new(BufReader::new(file)));
    let mut xml_contents: Vec<String> = Vec::new();

    for entry_result in archive.entries().unwrap() {
        let Ok(mut entry) = entry_result else {
            continue;
        };
        let Ok(entry_path) = entry.path() else {
            continue;
        };

        if entry_path.extension().and_then(OsStr::to_str) == Some("xml") {
            let mut content = String::new();
            if entry.read_to_string(&mut content).is_ok() {
                xml_contents.push(content);
            }
        }
    }

    let total_bytes: usize = xml_contents.iter().map(|s| s.len()).sum();
    let xml_count = xml_contents.len();
    Some((xml_contents, total_bytes, xml_count))
}

/// Parse a single XML string into a Record.
fn parse_xml(xml: &str) -> Option<Record> {
    let rd = &mut Deserializer::from_str(xml);
    serde_path_to_error::deserialize(rd).ok()
}

/// Benchmark XML parsing from pre-loaded strings (sequential).
fn bench_xml_parsing_sequential(c: &mut Criterion) {
    let Some((xml_contents, total_bytes, xml_count)) = load_xml_contents() else {
        eprintln!("No test TGZ file found. Skipping XML parsing benchmark.");
        return;
    };

    let mut group = c.benchmark_group("xml_parsing");
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "sequential",
            format!(
                "{}files_{:.0}MB",
                xml_count,
                total_bytes as f64 / 1024.0 / 1024.0
            ),
        ),
        &xml_contents,
        |b, contents| {
            b.iter(|| {
                let parsed: usize = contents.iter().filter_map(|xml| parse_xml(xml)).count();
                black_box(parsed)
            });
        },
    );

    group.finish();
}

/// Benchmark XML parsing from pre-loaded strings (parallel with rayon).
fn bench_xml_parsing_parallel(c: &mut Criterion) {
    let Some((xml_contents, total_bytes, xml_count)) = load_xml_contents() else {
        eprintln!("No test TGZ file found. Skipping XML parsing benchmark.");
        return;
    };

    let mut group = c.benchmark_group("xml_parsing");
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "parallel",
            format!(
                "{}files_{:.0}MB",
                xml_count,
                total_bytes as f64 / 1024.0 / 1024.0
            ),
        ),
        &xml_contents,
        |b, contents| {
            b.iter(|| {
                let parsed: usize = contents
                    .par_iter()
                    .filter_map(|xml| parse_xml(xml))
                    .count();
                black_box(parsed)
            });
        },
    );

    group.finish();
}

/// Benchmark full pipeline with parallel parsing via channel + rayon.
fn bench_full_pipeline_parallel(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found. Skipping parallel pipeline benchmark.");
        return;
    };

    let file_size = std::fs::metadata(&tgz_path).unwrap().len();
    let mut group = c.benchmark_group("full_pipeline");
    group.throughput(Throughput::Bytes(file_size));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "parallel",
            format!("{:.0}MB", file_size as f64 / 1024.0 / 1024.0),
        ),
        &tgz_path,
        |b, path| {
            b.iter(|| {
                let (tx, rx) = bounded::<String>(1024);
                let path = path.clone();

                let producer = thread::spawn(move || {
                    let file = File::open(&path).unwrap();
                    let mut archive = Archive::new(GzDecoder::new(BufReader::new(file)));

                    for entry_result in archive.entries().unwrap() {
                        let Ok(mut entry) = entry_result else {
                            continue;
                        };
                        let Ok(entry_path) = entry.path() else {
                            continue;
                        };

                        if entry_path.extension().and_then(OsStr::to_str) == Some("xml") {
                            let mut content = String::new();
                            if entry.read_to_string(&mut content).is_ok() {
                                if tx.send(content).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                let parsed: usize = rx
                    .into_iter()
                    .par_bridge()
                    .filter_map(|xml| parse_xml(&xml))
                    .count();

                producer.join().unwrap();
                black_box(parsed)
            });
        },
    );

    group.finish();
}

/// Benchmark full pipeline (decompress + tar + parse) - sequential.
fn bench_full_pipeline_sequential(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found. Skipping full pipeline benchmark.");
        return;
    };

    let file_size = std::fs::metadata(&tgz_path).unwrap().len();
    let mut group = c.benchmark_group("full_pipeline");
    group.throughput(Throughput::Bytes(file_size));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "sequential",
            format!("{:.0}MB", file_size as f64 / 1024.0 / 1024.0),
        ),
        &tgz_path,
        |b, path| {
            b.iter(|| {
                let file = File::open(path).unwrap();
                let mut archive = Archive::new(GzDecoder::new(BufReader::new(file)));
                let mut parsed = 0usize;

                for entry_result in archive.entries().unwrap() {
                    let Ok(mut entry) = entry_result else {
                        continue;
                    };
                    let Ok(entry_path) = entry.path() else {
                        continue;
                    };

                    if entry_path.extension().and_then(OsStr::to_str) == Some("xml") {
                        let mut content = String::new();
                        if entry.read_to_string(&mut content).is_ok() {
                            if parse_xml(&content).is_some() {
                                parsed += 1;
                            }
                        }
                    }
                }
                black_box(parsed)
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_gzip_decompression,
    bench_tar_iteration,
    bench_xml_parsing_sequential,
    bench_xml_parsing_parallel,
    bench_full_pipeline_sequential,
    bench_full_pipeline_parallel,
);
criterion_main!(benches);
