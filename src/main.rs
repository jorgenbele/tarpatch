use clap::{Parser, Subcommand};
use tokio::fs;
use std::io::Read;
use std::io::copy;

use serde::{Deserialize, Serialize};

use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet};

use anyhow::{Result, Context};

use filetime::FileTime;

use tar::{Archive, Builder, Header};
use flate2::read::GzDecoder;

use sha1::{Sha1, Digest};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // Verbose mode
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    // Enable gzip
    #[arg(short = 'c', long, default_value_t = false)]
    gzip: bool,

    // Output file
    #[arg(short, long)]
    out: PathBuf,

    // The command to execute
    #[command(subcommand)]
    command: Commands,
}


#[derive(Debug, Subcommand)]
enum Commands {
    /// Creates a diff tar file containing the changed files
    Diff { old: PathBuf, new: PathBuf },

    /// Applies the diff tar file to an existing tar file
    Apply { old: PathBuf, diff: PathBuf },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DiffMetadata {
    changed: Vec<PathBuf>,
    added: Vec<PathBuf>,
    removed: Vec<PathBuf>,
}

fn open_tar(path: &Path, gzip: bool) -> Result<Archive<std::fs::File>> {
    let file = std::fs::File::open(path)?;
    Ok(Archive::new(file))

    // if gzip {
    //     let tar = GzDecoder::new(file);
    //     Ok(Archive::new(tar))
    // } else {
    // }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexValue {
    sha1: [u8; 20],
    cksum: u32,
}

const DELTA_METADATA_FILE: &'static str = "__delta_metadata.json";

async fn create_index(archive: &mut Archive<std::fs::File>) -> Result<HashMap<PathBuf, IndexValue>> {
    let mut index = HashMap::new();
    let mut entries = archive.entries()?;
    for entry in entries {
        match entry.with_context(|| format!("corrupt tar")) {
            Ok(mut entry) => {
                let path = entry.path()?.to_path_buf();
                let cksum = entry.header().cksum().with_context(|| format!("invalid cksum"))?;

                // create a Sha1 object
                let mut hasher = Sha1::new();
                copy(&mut entry, &mut hasher).with_context(|| format!("sha1 hashing failed"))?;
                let sha1 = hasher.finalize();
                let value = IndexValue { cksum, sha1: sha1.into() };
                index.insert(path, value);
            },
            Err(err) => {
                return Err(err);
            }
        }
        // dbg!(&entry)
    }
    dbg!(&index);
    Ok(index)
}

fn entry_has_changed(a: &IndexValue, b: &IndexValue) -> bool {
    a != b
}

async fn create_delta_archive(changed: HashSet<PathBuf>, mut new_tar: &Path,  metadata: &DiffMetadata, out: &Path) -> Result<()> {
    // We now create a new tar file that consists of
    // a metadata file and the other files.
    // It will be structured like this:
    // *: all files
    // __delta_metadata.json: the json file

    // TODO: this can be done in parallel
    let mut file = std::fs::File::create(out)?;
    let mut builder = Builder::new(file);

    let mut new_tar = open_tar(new_tar, false)?;

    let mut entries = new_tar.entries_with_seek()?;
    for result_entry in entries {
        let mut entry = result_entry.with_context(|| format!("corrupt tar"))?;

        let mut path = entry.path()?.to_path_buf();
        if changed.contains(&path) {
            let mut header = entry.header().clone();
            builder.append_data(&mut header, path, &mut entry);
        }
    }

    // Write the metadata file to the tar archive
    let metadata_path = PathBuf::from(DELTA_METADATA_FILE);
    let mut metadata_header = Header::new_old();

    let mut metadata_bytes: Vec<u8> = Vec::new();
    serde_json::to_writer(&mut metadata_bytes, &metadata).unwrap();
    metadata_header.set_size(metadata_bytes.len() as u64);
    builder.append_data(&mut metadata_header, metadata_path, &metadata_bytes[..]);

    Ok(())
}

async fn diff(old: &Path, new: &Path, gzip: bool, out: &Path) -> Result<()> {
    let mut old_tar = open_tar(old, gzip)?;
    let mut new_tar = open_tar(new, gzip)?;

    let (old_index, new_index) = tokio::join!(
        create_index(&mut old_tar),
        create_index(&mut new_tar)
    );

    let old_index = old_index?;
    let new_index = new_index?;

    dbg!(&old_index);
    dbg!(&new_index);

    // do the computation of the diff
    let mut changed = HashSet::new();
    let mut added = HashSet::new();
    let mut removed = HashSet::new();

    // TODO: deal with removed files
    for (path, new_value) in new_index.iter() {
        if let Some(old_value) = old_index.get(path) {
            if entry_has_changed(new_value, old_value) {
                changed.insert(path.clone());
            }
        } else {
            added.insert(path.clone());
        }
    }

    for path in old_index.keys() {
        if !new_index.contains_key(path) {
            removed.insert(path.clone());
        }
    }

    let changed_vec = Vec::from_iter(changed.clone().into_iter());
    let added_vec = Vec::from_iter(added.clone().into_iter());

    changed.extend(added);

    let metadata = DiffMetadata {
        changed: changed_vec,
        added: added_vec,
        removed: Vec::from_iter(removed.into_iter()),
    };
    dbg!(&metadata);

    dbg!(&changed);

    create_delta_archive(changed, new, &metadata, out).await?;

    Ok(())
}

async fn apply(old: &Path, diff: &Path) -> Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    dbg!(&args);

    match &args.command {
        Commands::Diff { old, new } => diff(&old, &new, args.gzip, &args.out).await?,
        Commands::Apply { old, diff } => apply(&old, &diff).await?,
    }
    Ok(())
}
