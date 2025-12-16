# ORCiD Data Toolkit

A CLI tool to work with the [annual ORCiD Public Data
file](https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/).

## Features

- [ ] Convert the ORCiD Public Data Summaries file to different formats:
  - [x] [InvenioRDM Names](https://inveniordm.docs.cern.ch/customize/vocabularies/names/), ready
    for import in a `names_metadata` PostgreSQL table via `COPY`
  - [ ] CSV
  - [x] NDJSON
- [x] Enhance the converted files from other sources
  - [x] Translate employment affiliations to ROR IDs
- [ ] Download ORCiD Public Data files

## Installation

You can install the `orcid-data-toolkit` using `cargo`:

```bash
cargo install orcid-data-toolkit
```

Or download the binary from the [releases
page](https://github.com/inveniosoftware/orcid-data-toolkit/releases).

> [!NOTE]
> In the future we might also provide a PyPI package installable via `pipx` or
> `uv tool`.

## Usage

First download the latest ORCiD Public Data Summaries file from
[FigShare](https://doi.org/10.23640/07243.27151305.v1):

```bash
wget "https://orcid.figshare.com/ndownloader/files/49560102" \
  -O ORCID_2024_10_summaries.tar.gz
```

Then convert the file to the InvenioRDM Names format:

```bash
orcid-data-toolkit convert \
  --input-file ORCID_2024_10_summaries.tar.gz \
  --format invenio-rdm-names \
  --output-file names.csv
```

For development/debug purposes you can also pass an individual ORCiD Summmary
XML file into JSON:

```bash
# Will output the converted data to stdout
orcid-data-toolkit convert --format json --input-file samples/alex.xml
```

## Development

To run tests locally, you can use the following command:

```bash
cargo test
```

To run benchmarks:

```bash
cargo bench
```

### Test Data

The test fixtures in `tests/data/` contain real records from the [ORCID Public
Data File](https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/),
which is released under the [CC0 1.0 Public Domain Dedication](https://creativecommons.org/publicdomain/zero/1.0/).
This data is already publicly available and published by ORCID annually.

### Working with the ORCiD Public Data Summaries file

When working with the ORCiD Public Data Summary files, one might wish to extract
individual files from the `.tar.gz.` file. Since `.tar.gz` does not support
efficient random access, you can either:

- extract individual files using `tar -xzf <archive> <path-in-archive>`, which
  is slow since it has to go through the whole archive to find the file
- extract the whole archive and then access the individual file, which is faster
  but requires more disk space

A better solution is to use
[`ratarmount`](https://github.com/mxmlnkn/ratarmount), which allows you to mount
the `.tar.gz` file as a FUSE filesystem and access the files as if they were in
your filesystem but without all of them taking up disk space. You can do that
like so:

```bash
# Install ratarmount
uv tool install ratarmount

# Mount the archive. This will first generate an index file for the archive
# which will take some time (~15min)
ratarmount ORCID_2024_10_summaries.tar.gz orcid_summaries

cd orcid_summaries/ORCID_2024_10_summaries
ls -l

total 550
drwxrwxr-x. 1 6001 6001 0 Sep 23 17:21 000
drwxrwxr-x. 1 6001 6001 0 Sep 23 17:22 001
drwxrwxr-x. 1 6001 6001 0 Sep 23 17:23 002
drwxrwxr-x. 1 6001 6001 0 Sep 23 17:24 003
drwxrwxr-x. 1 6001 6001 0 Sep 23 17:25 004
...

# Copy a single ORCiD summary file out of the archive
cp 000/0000-0002-5082-6404.xml ~/tmp/
```

## FAQ

### Why do I need this tool?

The annual ORCiD Public Data file is a huge file (the 2024 file is **36.28GB**)
that contains all public information about ORCiD records. It is packaged as a
GZIP-compressed TAR archive containing XML files for each ORCiD record.
Processing this file can be challenging since:

- extracting it expands to a huge amount of data
- the XML format is not very usable for further processing with tools like
  `awk`, `sed`, `grep`, `jq`, etc.

This tool helps to convert this file into a more usable format, e.g. CSV or
NDJSON. In InvenioRDM specifically, we expose an "author search" feature, which
sources entries from a PostgreSQL table. This tool efficiently converts the
ORCiD Public Data Summaries file to a CSV file that can be quickly imported into
the `names_metadata` table using PostgreSQL's `COPY` command.

### Why not use the official [ORCID Conversion Libary](https://github.com/ORCID/orcid-conversion-lib)?

The official ORCID Conversion Library is a Java library that can be used to
convert the ORCiD Public Data files into other formats. Compared to Java,
distributing a Rust binary is much easier and lightweight.
