use anyhow::{bail, Result};
use chrono::Utc;
use flate2::read::GzDecoder;
use std::{
    ffi::OsStr,
    fs::{self, File},
    io::{stdout, Read},
    path::{Path, PathBuf},
};
use tar::Archive;

use uuid::Uuid;

use quick_xml::de::Deserializer;
use serde::Deserialize;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Identifier {
    #[serde(rename = "uri")]
    uri: String,
    #[serde(rename = "path")]
    path: String,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct PersonName {
    #[serde(rename = "given-names")]
    given_names: Option<String>,
    #[serde(rename = "family-name")]
    family_name: Option<String>,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Person {
    name: PersonName,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct OrgIdentifier {
    #[serde(alias = "disambiguated-organization-identifier")]
    identifier: String,
    #[serde(alias = "disambiguation-source")]
    source: String,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Organization {
    name: String,
    #[serde(alias = "disambiguated-organization")]
    identifier: Option<OrgIdentifier>,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Employment {
    #[serde(alias = "end-date")]
    end: Option<()>,
    organization: Organization,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct AffiliationGroup {
    #[serde(alias = "employment-summary")]
    employment: Employment,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Employments {
    #[serde(alias = "affiliation-group")]
    employment: Option<Vec<AffiliationGroup>>,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Activities {
    employments: Employments,
}

#[derive(Debug, PartialEq, Default, Deserialize)]
struct Record {
    #[serde(alias = "orcid-identifier")]
    identifier: Identifier,
    person: Person,
    #[serde(alias = "activities-summary")]
    activities: Activities,
}

#[derive(Debug, serde::Serialize)]
struct NameIdentifier {
    scheme: String,
    identifier: String,
}

#[derive(Debug, serde::Serialize)]
struct NameAffiliation {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "$schema", rename = "local://names/name-v1.0.0.json")]
struct NameJson {
    given_name: String,
    family_name: String,
    name: String,
    identifiers: Vec<NameIdentifier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    affiliations: Option<Vec<NameAffiliation>>,
}

#[derive(serde::Serialize)]
struct Row {
    created: String,
    updated: String,
    id: String,
    json: String,
    version_id: u8,
    pid: String,
}

fn csv_line_from_record(record: &Record) -> Result<Row> {
    let now = Utc::now().to_rfc3339();
    let name_json = record_to_json(record)?;
    Ok(Row {
        created: String::from(now.as_str()),
        updated: String::from(now.as_str()),
        id: Uuid::new_v4().to_string(),
        pid: String::from(record.identifier.path.as_str()),
        version_id: 1,
        json: serde_json::to_string(&name_json)?,
    })
}

fn record_to_json(record: &Record) -> Result<NameJson> {
    let mut affiliations: Vec<NameAffiliation> = vec![];
    let employments = record.activities.employments.employment.as_ref();
    if let Some(_employments) = employments {
        _employments
            .iter()
            .filter_map(|a| match a.employment.end {
                Some(_) => None,
                None => {
                    let ror_id = match &a.employment.organization.identifier {
                        Some(identifier) if identifier.source == "ROR" => Some(
                            identifier
                                .identifier
                                .as_str()
                                .rsplit_once('/')?
                                .1
                                .to_string(),
                        ),
                        _ => None,
                    };
                    Some(NameAffiliation {
                        name: a.employment.organization.name.clone(),
                        id: ror_id,
                    })
                }
            })
            .for_each(|n| affiliations.push(n));
    }

    let (given_name, family_name, name) = match &record.person.name {
        PersonName {
            given_names: Some(name),
            family_name: None,
        }
        | PersonName {
            given_names: None,
            family_name: Some(name),
        } => {
            if name.trim().len() > 0 {
                (String::new(), name.clone(), name.clone())
            } else {
                bail!("Can't determine person name")
            }
        }

        // If both values are present, combine them
        PersonName {
            given_names: Some(given_names),
            family_name: Some(family_name),
        } => (
            given_names.clone(),
            family_name.clone(),
            format!("{}, {}", family_name, given_names),
        ),
        PersonName {
            given_names: None,
            family_name: None,
        } => bail!("Can't determine person name"),
    };

    Ok(NameJson {
        given_name,
        family_name,
        name,
        identifiers: vec![NameIdentifier {
            scheme: "orcid".to_string(),
            identifier: record.identifier.path.clone(),
        }],
        affiliations: (!affiliations.is_empty()).then_some(affiliations),
    })
}

fn iter_records<R: Read>(entries: tar::Entries<R>) -> impl Iterator<Item = Record> + use<'_, R> {
    entries
        .filter_map(|e| match e {
            Ok(entry) => {
                let path = match entry.path() {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Error getting entry path: {}", e);
                        return None;
                    }
                };
                match path.extension().and_then(OsStr::to_str) {
                    Some("xml") => Some(entry),
                    _ => None,
                }
            }
            Err(e) => {
                eprintln!("Error reading archive entry: {}", e);
                None
            }
        })
        .filter_map(|mut entry| -> Option<Record> {
            let mut xml_content = String::new();
            let path = entry
                .path()
                .ok()
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            if let Err(e) = entry.read_to_string(&mut xml_content) {
                eprintln!("Error reading XML content for {}: {}", path, e);
                return None;
            }
            let rd = &mut Deserializer::from_str(&xml_content);
            match serde_path_to_error::deserialize(rd) {
                Ok(record) => Some(record),
                Err(err) => {
                    eprintln!(
                        "Error parsing XML content for {}: {}",
                        path,
                        err.path().to_string()
                    );
                    None
                }
            }
        })
}

fn convert_tgz(input_file: &PathBuf, output_file: &PathBuf, format: &ConvertFormat) {
    // Open the input .tar.gz
    let file = File::open(input_file)
        .map_err(|e| {
            eprintln!("Error opening file {}: {}", input_file.display(), e);
            e
        })
        .unwrap();
    let mut archive = Archive::new(GzDecoder::new(file));
    let records = iter_records(archive.entries().unwrap());

    // Open the output CSV writer
    let mut out_stream = match output_file.to_str() {
        Some("-") => Box::new(stdout()) as Box<dyn std::io::Write>,
        _ => Box::new(
            File::create(output_file)
                .map_err(|e| {
                    eprintln!(
                        "Error creating output file {}: {}",
                        output_file.display(),
                        e
                    );
                    e
                })
                .unwrap(),
        ),
    };

    match format {
        ConvertFormat::NDJSON => {
            for r in records {
                let json = match record_to_json(&r) {
                    Ok(record) => record,
                    Err(e) => {
                        eprintln!("Error converting record to JSON: {}", e);
                        continue;
                    }
                };

                if let Err(e) = serde_json::to_writer(&mut out_stream, &json) {
                    eprintln!("Error writing JSON: {}", e);
                }
            }
            return;
        }
        ConvertFormat::InvenioRDMNames => {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(out_stream);

            // Convert and write the records to CSV
            records.for_each(|r| {
                let row = match csv_line_from_record(&r) {
                    Ok(record) => record,
                    Err(e) => {
                        eprintln!("Error converting record to CSV: {}", e);
                        return;
                    }
                };
                if let Err(e) = writer.serialize(row) {
                    eprintln!("Error writing CSV line: {}", e);
                }
            })
        }
    };
}

fn convert_xml(input_file: &PathBuf, format: &ConvertFormat) {
    let record = {
        let xml = fs::read_to_string(input_file).expect("Failed to read XML file");
        let rd = &mut Deserializer::from_str(&xml);
        match serde_path_to_error::deserialize(rd) {
            Ok(record) => Ok::<Record, anyhow::Error>(record),
            Err(err) => {
                let err_path = err.path().to_string();
                dbg!(err_path);
                Err(err.into())
            }
        }
    }
    .expect("Failed to parse XML");

    match format {
        ConvertFormat::InvenioRDMNames => {
            let row = csv_line_from_record(&record).expect("Failed to convert to CSV");
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(stdout());
            writer.serialize(row).unwrap()
        }
        ConvertFormat::NDJSON => {
            let name_json = record_to_json(&record).expect("Failed to convert to JSON");
            println!("{}", serde_json::to_string_pretty(&name_json).unwrap());
        }
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ConvertFormat {
    InvenioRDMNames,
    NDJSON,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ExtractFormat {
    RINGGOLD,
}

#[derive(Subcommand)]
enum Commands {
    Convert {
        #[arg(
            short,
            long,
            required = true,
            help = "Path to the ORCiD public data file"
        )]
        input_file: PathBuf,

        #[arg(
            short,
            long,
            help = "Path to where to output the converted file",
            default_value = "-"
        )]
        output_file: PathBuf,

        #[arg(value_enum, short, long, help = "Output format", default_value_t=ConvertFormat::InvenioRDMNames)]
        format: ConvertFormat,
    },

    Extract {
        #[arg(
            short,
            long,
            required = true,
            help = "Path to the ORCiD public data file"
        )]
        input_file: PathBuf,

        #[arg(
            short,
            long,
            help = "Path to where to output the extracted file",
            default_value = "-"
        )]
        output_file: PathBuf,

        #[arg(value_enum, short, long, help = "Extract format", default_value_t=ExtractFormat::RINGGOLD)]
        format: ExtractFormat,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Convert {
            input_file,
            output_file,
            format,
        }) => {
            if output_file != Path::new("-") {
                eprintln!("Can only output to stdout for now");
                return;
            }
            match input_file.extension().and_then(OsStr::to_str) {
                Some("xml") => convert_xml(input_file, format),
                Some("gz") => convert_tgz(input_file, output_file, format),
                _ => eprintln!("Unsupported file extension"),
            };
        }
        Some(Commands::Extract {
            input_file: _,
            output_file,
            format: _,
        }) => {
            if output_file != Path::new("-") {
                eprintln!("Can only output to stdout for now");
                return;
            }
        }
        None => {}
    }
}
