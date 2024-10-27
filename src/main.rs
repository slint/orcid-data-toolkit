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
struct NameIdentifier<'a> {
    scheme: &'a str,
    identifier: &'a str,
}

#[derive(Debug, serde::Serialize)]
struct NameAffiliation<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    name: &'a str,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "$schema", rename = "local://names/name-v1.0.0.json")]
struct NameJson<'a> {
    given_name: &'a str,
    family_name: &'a str,
    name: &'a str,
    identifiers: Vec<NameIdentifier<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    affiliations: Option<Vec<NameAffiliation<'a>>>,
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
    let mut affiliations: Vec<NameAffiliation> = vec![];
    let employments = record.activities.employments.employment.as_ref();
    if let Some(_employments) = employments {
        _employments
            .iter()
            .filter_map(|a| match a.employment.end {
                Some(_) => None,
                None => {
                    let ror_id = match &a.employment.organization.identifier {
                        Some(identifier) if identifier.source == "ROR" => {
                            Some(identifier.identifier.as_str().rsplit_once('/')?.1)
                        }
                        _ => None,
                    };
                    Some(NameAffiliation {
                        name: &a.employment.organization.name,
                        id: ror_id,
                    })
                }
            })
            .for_each(|n| affiliations.push(n));
    }

    let (given_name, family_name, name) = match &record.person.name {
        PersonName {
            given_names: Some(given_names),
            family_name: None,
        } => {
            if given_names.trim().len() > 0 {
                (String::from(""), given_names.clone(), given_names.clone())
            } else {
                bail!("Can't determine person name")
            }
        }
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
            family_name: Some(family_name),
        } => {
            if family_name.trim().len() > 0 {
                (String::from(""), family_name.clone(), family_name.clone())
            } else {
                bail!("Can't determine person name")
            }
        }
        PersonName {
            given_names: None,
            family_name: None,
        } => bail!("Can't determine person name"),
    };

    let name_json = NameJson {
        given_name: given_name.as_str(),
        family_name: family_name.as_str(),
        name: name.as_str(),
        identifiers: vec![NameIdentifier {
            scheme: "orcid",
            identifier: &record.identifier.path,
        }],
        affiliations: if affiliations.is_empty() {
            None
        } else {
            Some(affiliations)
        },
    };
    Ok(Row {
        created: String::from(now.as_str()),
        updated: String::from(now.as_str()),
        id: Uuid::new_v4().to_string(),
        pid: String::from(record.identifier.path.as_str()),
        version_id: 1,
        json: serde_json::to_string(&name_json)?,
    })
}

fn parse_xml(xml_path: &Path) -> Result<Record> {
    let xml: String = fs::read_to_string(xml_path)?.parse()?;
    let rd = &mut Deserializer::from_str(&xml);
    match serde_path_to_error::deserialize(rd) {
        Ok(record) => Ok(record),
        Err(err) => {
            let err_path = err.path().to_string();
            dbg!(err_path);
            Err(err.into())
        }
    }
}

fn parse_tgz(tgz_path: &Path) -> Result<()> {
    // TODO: Probably return a stream of records instead of writing directly to stdout
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(stdout());

    let file = File::open(tgz_path).map_err(|e| {
        eprintln!("Error opening file {}: {}", tgz_path.display(), e);
        e
    })?;
    let mut archive = Archive::new(GzDecoder::new(file));
    archive
        .entries()
        .map_err(|e| {
            eprintln!("Error reading archive entries: {}", e);
            e
        })?
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
        .for_each(|r| {
            let record = match csv_line_from_record(&r) {
                Ok(record) => record,
                Err(e) => {
                    eprintln!("Error converting record to CSV: {}", e);
                    return;
                }
            };
            if let Err(e) = writer.serialize(record) {
                eprintln!("Error writing CSV line: {}", e);
            }
        });
    Ok(())
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Format {
    InvenioRDMNames,
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

        #[arg(value_enum, short, long, help = "Output format", default_value_t=Format::InvenioRDMNames)]
        format: Format,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Convert {
            input_file,
            output_file,
            format: _,
        }) => {
            if output_file != Path::new("-") {
                eprintln!("Can only output to stdout for now");
                return;
            }
            if input_file.ends_with(".xml") {
                let record = parse_xml(input_file).expect("Failed to parse XML");
                let line = csv_line_from_record(&record).expect("Failed to convert to CSV");
                let mut writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(stdout());
                writer.serialize(line).unwrap()
            } else {
                let _ = parse_tgz(input_file);
            }
        }
        None => {}
    }
}
