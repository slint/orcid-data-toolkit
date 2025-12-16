//! Benchmarks for TGZ processing pipeline stages.
//!
//! Run with: cargo bench
//!
//! For comparing backends, modify Cargo.toml flate2 features and re-run.

use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, Read},
};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use flate2::read::GzDecoder;
use quick_xml::de::Deserializer;
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

/// Find test TGZ file, preferring larger files for more accurate benchmarks.
fn find_test_tgz() -> Option<std::path::PathBuf> {
    let candidates = [
        "var/summaries-2025-large.tar.gz",   // ~320MB, best for benchmarks
        "var/summaries-2025-partial.tar.gz", // ~26MB
        "var/summaries-2025.tar.gz",         // full file if available
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

/// Benchmark XML parsing from pre-loaded strings.
fn bench_xml_parsing(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found in var/. Skipping XML parsing benchmark.");
        return;
    };

    // Pre-load XML contents
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

    let mut group = c.benchmark_group("xml_parsing");
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "parse",
            format!(
                "{}files_{:.0}MB",
                xml_count,
                total_bytes as f64 / 1024.0 / 1024.0
            ),
        ),
        &xml_contents,
        |b, contents| {
            b.iter(|| {
                let mut parsed = 0usize;
                for xml in contents {
                    let rd = &mut Deserializer::from_str(xml);
                    let result: Result<Record, _> = serde_path_to_error::deserialize(rd);
                    if result.is_ok() {
                        parsed += 1;
                    }
                }
                black_box(parsed)
            });
        },
    );

    group.finish();
}

/// Benchmark full pipeline (decompress + tar + parse).
fn bench_full_pipeline(c: &mut Criterion) {
    let Some(tgz_path) = find_test_tgz() else {
        eprintln!("No test TGZ file found in var/. Skipping full pipeline benchmark.");
        return;
    };

    let file_size = std::fs::metadata(&tgz_path).unwrap().len();
    let mut group = c.benchmark_group("full_pipeline");
    group.throughput(Throughput::Bytes(file_size));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new(
            "end_to_end",
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
                            let rd = &mut Deserializer::from_str(&content);
                            let result: Result<Record, _> = serde_path_to_error::deserialize(rd);
                            if result.is_ok() {
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
    bench_xml_parsing,
    bench_full_pipeline,
);
criterion_main!(benches);
