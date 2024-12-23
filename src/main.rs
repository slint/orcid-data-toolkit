use anyhow::{bail, Result};
use orcid_data_toolkit::{
    convert_tgz, convert_xml, extract_tgz, extract_xml, ConvertFormat, ExtractFormat,
};
use std::{ffi::OsStr, path::PathBuf};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

const DEFAULT_NAME_FILTER_REGEX: &str = r"^[\p{L} ,.'’`´\-\(\)]+$";

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

        /// Path to Organization ID CSV mappings file
        #[arg(long = "orgs-mapping")]
        orgs_mappings_file: Option<PathBuf>,

        #[arg(long = "filter-name", default_value=DEFAULT_NAME_FILTER_REGEX)]
        filter_name: Option<String>,
    },

    Extract {
        /// Path to the ORCiD public data file
        #[arg(short, long)]
        input_file: PathBuf,

        /// Path to where to output the extracted file,
        #[arg(short, long, default_value = "-")]
        output_file: PathBuf,

        /// Extract format
        #[arg(value_enum, short, long, default_value_t=ExtractFormat::OrgIDs)]
        format: ExtractFormat,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Convert {
            input_file,
            output_file,
            orgs_mappings_file,
            filter_name,
            format,
        } => match input_file.extension().and_then(OsStr::to_str) {
            Some("xml") => convert_xml(input_file, output_file, orgs_mappings_file, format),
            Some("gz") => convert_tgz(
                input_file,
                output_file,
                orgs_mappings_file,
                filter_name,
                format,
            ),
            _ => bail!("Unsupported file extension"),
        },
        Commands::Extract {
            input_file,
            output_file,
            format,
        } => match input_file.extension().and_then(OsStr::to_str) {
            Some("xml") => extract_xml(input_file, output_file, format),
            Some("gz") => extract_tgz(input_file, output_file, format),
            _ => bail!("Unsupported file extension"),
        },
    }
}
