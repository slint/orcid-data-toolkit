use anyhow::{bail, Context, Result};
use chrono::Utc;
use flate2::read::GzDecoder;
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{self, File},
    io::{stdout, Read},
    path::PathBuf,
};
use tar::Archive;

use uuid::Uuid;

use quick_xml::de::Deserializer;
use serde::Deserialize;

use clap::ValueEnum;

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

fn record_to_row(record: &Record, org_map: &OrgMap, created_dt: &str) -> Result<Row> {
    let name_json = record_to_json(record, org_map)?;
    Ok(Row {
        created: String::from(created_dt),
        updated: String::from(created_dt),
        id: Uuid::new_v4().to_string(),
        pid: String::from(record.identifier.path.as_str()),
        version_id: 1,
        json: serde_json::to_string(&name_json)?,
    })
}

fn record_to_json(record: &Record, org_map: &OrgMap) -> Result<NameJson> {
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
                        Some(identifier) if identifier.source == "ROR" => identifier
                            .identifier
                            .as_str()
                            .rsplit_once('/')
                            .map(|(_, id)| id.to_string()),
                        // Check for ROR ID in the org_map
                        Some(identifer) => {
                            let normalized_id = match identifer.source.as_str() {
                                // Keep last part of FUNDREF, similar to ROR
                                "FUNDREF" => identifer
                                    .identifier
                                    .rsplit_once('/')
                                    .map(|(_, id)| id.to_string()),
                                _ => Some(identifer.identifier.clone()),
                            };
                            normalized_id.and_then(|id| {
                                org_map
                                    .get(&ExtractedIdentifier {
                                        scheme: identifer.source.clone(),
                                        identifier: id,
                                    })
                                    .cloned()
                            })
                        }
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
        // If either value is present, use it
        PersonName {
            given_names: Some(name),
            family_name: None,
        }
        | PersonName {
            given_names: None,
            family_name: Some(name),
        } if !name.trim().is_empty() => (String::new(), name.clone(), name.clone()),

        // If both values are present, combine them
        PersonName {
            given_names: Some(given_names),
            family_name: Some(family_name),
        } if !given_names.trim().is_empty() && !family_name.trim().is_empty() => (
            given_names.clone(),
            family_name.clone(),
            format!("{}, {}", family_name, given_names),
        ),
        _ => bail!("Can't determine person name from {:?}", record.person.name,),
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

fn iter_records<R: Read>(entries: tar::Entries<'_, R>) -> impl Iterator<Item = Record> + '_ {
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
                        err.path()
                    );
                    None
                }
            }
        })
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ConvertFormat {
    InvenioRDMNames,
    JSON,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ExtractFormat {
    OrgIDs,
}

type OrgMap = HashMap<ExtractedIdentifier, String>;

pub fn convert_tgz(
    input_file: &PathBuf,
    output_file: &PathBuf,
    orgs_mappings_file: &Option<PathBuf>,
    format: &ConvertFormat,
) -> Result<()> {
    let org_map = read_org_ids(orgs_mappings_file);
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
                let json = record_to_json(&r, &org_map);
                // Log the error and continue to the next record
                if let Err(e) = json {
                    eprintln!("Error converting record to JSON: {}", e);
                    continue;
                }
                serde_json::to_writer(&mut out_stream, &json.unwrap())
                    .with_context(|| "Error writing JSON".to_string())?;
            }
        }
        ConvertFormat::InvenioRDMNames => {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(out_stream);
            let now = Utc::now().to_rfc3339();

            // Convert and write the records to CSV
            for r in records {
                let row = record_to_row(&r, &org_map, &now);
                if let Err(e) = row {
                    eprintln!("Error converting record to JSON: {}", e);
                    continue;
                }
                writer
                    .serialize(row.unwrap())
                    .with_context(|| "Error writing CSV line".to_string())?;
            }
        }
    };
    Ok(())
}

pub fn convert_xml(
    input_file: &PathBuf,
    output_file: &PathBuf,
    orgs_mappings_file: &Option<PathBuf>,
    format: &ConvertFormat,
) -> Result<()> {
    let org_map = read_org_ids(orgs_mappings_file);
    let xml = fs::read_to_string(input_file).expect("Failed to read XML file");
    let rd = &mut Deserializer::from_str(&xml);
    let record = serde_path_to_error::deserialize(rd)
        .with_context(|| "Error parsing XML content".to_string())?;

    let mut out_stream = match output_file.to_str() {
        Some("-") => Box::new(stdout()) as Box<dyn std::io::Write>,
        _ => Box::new(
            File::create(output_file)
                .with_context(|| format!("Error opening file {}", input_file.display()))?,
        ),
    };

    match format {
        ConvertFormat::InvenioRDMNames => {
            let now = Utc::now().to_rfc3339();
            let row = record_to_row(&record, &org_map, &now).expect("Failed to convert to CSV");
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(out_stream);
            writer.serialize(row).unwrap()
        }
        ConvertFormat::JSON => {
            let json = record_to_json(&record, &org_map).expect("Failed to convert to JSON");
            serde_json::to_writer_pretty(&mut out_stream, &json)
                .with_context(|| "Error writing JSON".to_string())?;
        }
    };
    Ok(())
}

fn read_org_ids(orgs_mappings_file: &Option<PathBuf>) -> OrgMap {
    let mut org_map = OrgMap::new();
    if let Some(orgs_mappings_file) = orgs_mappings_file {
        if let Ok(file) = File::open(orgs_mappings_file) {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(file);
            for result in reader.deserialize() {
                let (scheme, identifier, ror_id): (String, String, String) =
                    result.expect("Failed to parse org IDs file");
                org_map.insert(ExtractedIdentifier { scheme, identifier }, ror_id);
            }
        }
    }
    org_map
}

#[derive(Debug, Hash, Eq, PartialEq, serde::Serialize)]
struct ExtractedIdentifier {
    scheme: String,
    identifier: String,
}

fn collect_org_ids(record: Record) -> HashSet<ExtractedIdentifier> {
    record
        .activities
        .employments
        .employment
        .unwrap_or_default()
        .iter()
        .filter_map(|a| {
            a.employment
                .organization
                .identifier
                .as_ref()
                .map(|id| ExtractedIdentifier {
                    scheme: id.source.to_string(),
                    identifier: id.identifier.to_string(),
                })
        })
        .collect()
}

pub fn extract_xml(
    input_file: &PathBuf,
    output_file: &PathBuf,
    format: &ExtractFormat,
) -> Result<()> {
    let xml = fs::read_to_string(input_file).expect("Failed to read XML file");
    let rd = &mut Deserializer::from_str(&xml);
    let record: Record = serde_path_to_error::deserialize(rd)
        .with_context(|| "Error parsing XML content".to_string())?;

    let mut out_stream = match output_file.to_str() {
        Some("-") => Box::new(stdout()) as Box<dyn std::io::Write>,
        _ => Box::new(
            File::create(output_file)
                .with_context(|| format!("Error opening file {}", input_file.display()))?,
        ),
    };

    match format {
        ExtractFormat::OrgIDs => {
            let identifiers = collect_org_ids(record);
            writeln!(
                out_stream,
                "{}",
                serde_json::to_string_pretty(&identifiers)?
            )
            .with_context(|| "Error writing JSON".to_string())?;
        }
    }
    Ok(())
}

pub fn extract_tgz(
    input_file: &PathBuf,
    output_file: &PathBuf,
    format: &ExtractFormat,
) -> Result<()> {
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
        ExtractFormat::OrgIDs => {
            let mut identifiers = HashSet::<ExtractedIdentifier>::new();
            for r in records {
                let org_ids = collect_org_ids(r);
                // Write the org IDs that are not already in the set
                for i in &org_ids {
                    if !identifiers.contains(i) {
                        writeln!(out_stream, "{}", serde_json::to_string(i)?)
                            .with_context(|| "Error writing JSON".to_string())?;
                    }
                }
                identifiers.extend(org_ids);
            }
        }
    }

    Ok(())
}
