use anyhow::Result;
use chrono::Utc;
use flate2::read::GzDecoder;
use std::fs;
use std::io::{stdout, Read};
use std::path::{Path, PathBuf};
use std::{ffi::OsStr, fs::File};
use tar::Archive;

use uuid::Uuid;

use quick_xml::de::from_str;
use serde::Deserialize;

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
    given_names: String,
    #[serde(rename = "family-name")]
    family_name: String,
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
    identifier: OrgIdentifier,
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
                    let ror_id = match a.employment.organization.identifier.source.as_str() {
                        "ROR" => Some(
                            a.employment
                                .organization
                                .identifier
                                .identifier
                                .as_str()
                                .rsplit_once('/')?
                                .1,
                        ),
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
    let name_json = NameJson {
        given_name: &record.person.name.given_names,
        family_name: &record.person.name.family_name,
        name: &format!(
            "{}, {}",
            record.person.name.family_name, record.person.name.given_names
        ),
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
    let record: Record = from_str(&xml)?;
    Ok(record)
}

fn parse_tgz(tgz_path: &Path) -> Result<()> {
    // TODO: Probably return a stream of records instead of writing directly to stdout
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(stdout());

    let file = File::open(tgz_path).unwrap();
    let mut archive = Archive::new(GzDecoder::new(file));
    archive
        .entries()?
        .filter_map(|e| {
            let entry = e.unwrap();
            let path = entry.path().expect("No entry path");
            match path.extension().and_then(OsStr::to_str) {
                Some("xml") => Some(entry),
                _ => None,
            }
        })
        .filter_map(|mut entry| -> Option<Record> {
            let mut xml_content = String::new();
            entry.read_to_string(&mut xml_content).unwrap();
            from_str(&xml_content).ok()
        })
        .for_each(|r| writer.serialize(csv_line_from_record(&r).unwrap()).unwrap());
    Ok(())
}

fn main() {
    let path = std::env::args().nth(1).expect("No ORCiD dump path given.");
    if path.ends_with(".xml") {
        let record = parse_xml(&PathBuf::from(path)).expect("Failed to parse XML");
        let line = csv_line_from_record(&record).expect("Failed to convert to CSV");
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(stdout());
        writer.serialize(line).unwrap()
    } else {
        let _ = parse_tgz(&PathBuf::from(path));
    }
}
