use anyhow::Result;
use assert_cmd::prelude::*;
use std::process::Command;

#[test]
fn convert_xml() -> Result<()> {
    let mut cmd = Command::cargo_bin("orcid-data-toolkit")?;

    let pred = r#"{
  "$schema": "local://names/name-v1.0.0.json",
  "given_name": "Alex",
  "family_name": "Ioannidis",
  "name": "Ioannidis, Alex",
  "identifiers": [
    {
      "scheme": "orcid",
      "identifier": "0000-0002-5082-6404"
    }
  ],
  "affiliations": [
    {
      "id": "01ggx4157",
      "name": "European Organization for Nuclear Research"
    }
  ]
}"#;
    cmd.arg("convert")
        .arg("--input-file")
        .arg("tests/data/alex.xml")
        .arg("--format")
        .arg("json")
        .assert()
        .success()
        .stdout(pred);

    Ok(())
}
