use anyhow::{bail, Context, Result};
use chrono::Utc;
use flate2::read::GzDecoder;
use std::{
    ffi::OsStr,
    fs::{self, File},
    io::{stdout, Read},
    path::PathBuf,
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

fn record_to_row(record: &Record) -> Result<Row> {
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
                // Past employment (i.e. end date is present)
                Some(_) => None,
                // Active employment (i.e. no end date)
                None => {
                    // Check for ROR ID
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
            if !name.trim().is_empty() {
                (String::new(), name.clone(), name.clone())
            } else {
                bail!(
                    "Can't determine person name from {:?}, {:?}",
                    record.person.name.given_names,
                    record.person.name.family_name,
                )
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
        } => bail!(
            "Can't determine person name from {:?}, {:?}",
            record.person.name.given_names,
            record.person.name.family_name,
        ),
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
        .filter_map(|entry_result| {
            let entry = entry_result.ok()?;
            let path = entry.path().ok()?;
            if path.extension().and_then(OsStr::to_str) == Some("xml") {
                Some(entry)
            } else {
                None
            }
        })
        .filter_map(|mut entry| -> Option<Record> {
            let mut xml_content = String::new();
            entry.read_to_string(&mut xml_content).ok()?;
            let rd = &mut Deserializer::from_str(&xml_content);
            match serde_path_to_error::deserialize(rd) {
                Ok(record) => Some(record),
                Err(err) => {
                    eprintln!(
                        "Error parsing XML content for {}: {}",
                        entry.path().unwrap().display(),
                        err.path().to_string()
                    );
                    None
                }
            }
        })
}

fn convert_tgz(input_file: &PathBuf, output_file: &PathBuf, format: &ConvertFormat) -> Result<()> {
    // Open the input .tar.gz
    let file = File::open(input_file)
        .with_context(|| format!("Error opening file {}", input_file.display()))?;
    let mut archive = Archive::new(GzDecoder::new(file));
    let records = iter_records(archive.entries().unwrap());

    // Open the output CSV writer
    let mut out_stream = match output_file.to_str() {
        Some("-") => Box::new(stdout()) as Box<dyn std::io::Write>,
        _ => Box::new(
            File::create(output_file)
                .with_context(|| format!("Error opening file {}", input_file.display()))?,
        ),
    };

    match format {
        ConvertFormat::JSON => {
            for r in records {
                let json = record_to_json(&r);
                // Log the error and continue to the next record
                if let Err(e) = json {
                    eprintln!("Error converting record to JSON: {}", e);
                    continue;
                }
                serde_json::to_writer(&mut out_stream, &json.unwrap())
                    .with_context(|| format!("Error writing JSON"))?;
            }
        }
        ConvertFormat::InvenioRDMNames => {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(out_stream);

            // Convert and write the records to CSV
            for r in records {
                let row = record_to_row(&r);
                if let Err(e) = row {
                    eprintln!("Error converting record to JSON: {}", e);
                    continue;
                }
                writer
                    .serialize(row.unwrap())
                    .with_context(|| format!("Error writing CSV line"))?;
            }
        }
    };
    Ok(())
}

fn convert_xml(input_file: &PathBuf, output_file: &PathBuf, format: &ConvertFormat) -> Result<()> {
    let xml = fs::read_to_string(input_file).expect("Failed to read XML file");
    let rd = &mut Deserializer::from_str(&xml);
    let record = serde_path_to_error::deserialize(rd)
        .with_context(|| format!("Error parsing XML content"))?;

    let mut out_stream = match output_file.to_str() {
        Some("-") => Box::new(stdout()) as Box<dyn std::io::Write>,
        _ => Box::new(
            File::create(output_file)
                .with_context(|| format!("Error opening file {}", input_file.display()))?,
        ),
    };

    match format {
        ConvertFormat::InvenioRDMNames => {
            let row = record_to_row(&record).expect("Failed to convert to CSV");
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(out_stream);
            writer.serialize(row).unwrap()
        }
        ConvertFormat::JSON => {
            let json = record_to_json(&record).expect("Failed to convert to JSON");
            serde_json::to_writer_pretty(&mut out_stream, &json)
                .with_context(|| format!("Error writing JSON"))?;
        }
    };
    Ok(())
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ConvertFormat {
    InvenioRDMNames,
    JSON,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ExtractFormat {
    RINGGOLD,
}

#[derive(Subcommand)]
enum Commands {
    Convert {
        /// Path to the ORCiD public data file
        #[arg(short, long)]
        input_file: PathBuf,

        /// Path to where to output the converted file,
        #[arg(short, long, default_value = "-")]
        output_file: PathBuf,

        /// Output format
        #[arg(short, long, value_enum, default_value_t=ConvertFormat::InvenioRDMNames)]
        format: ConvertFormat,
    },

    Extract {
        /// Path to the ORCiD public data file
        #[arg(short, long)]
        input_file: PathBuf,

        /// Path to where to output the extracted file,
        #[arg(short, long, default_value = "-")]
        output_file: PathBuf,

        /// Extract format
        #[arg(value_enum, short, long, default_value_t=ExtractFormat::RINGGOLD)]
        format: ExtractFormat,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Convert {
            input_file,
            output_file,
            format,
        } => match input_file.extension().and_then(OsStr::to_str) {
            Some("xml") => convert_xml(input_file, output_file, format),
            Some("gz") => convert_tgz(input_file, output_file, format),
            _ => bail!("Unsupported file extension"),
        },
        Commands::Extract {
            input_file: _,
            output_file: _,
            format: _,
        } => Ok(()),
    }
}
