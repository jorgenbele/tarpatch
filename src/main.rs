use clap::{Parser, Subcommand};


use std::io::copy;

use serde::{Deserialize, Serialize};

use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet};

use anyhow::{Result, Context};
use anyhow::bail;



use tar::{Archive, Builder, Header};


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

    // The command to execute
    #[command(subcommand)]
    command: Commands,
}


#[derive(Debug, Subcommand)]
enum Commands {
    /// Creates a diff tar file containing the changed files
    Diff { old: PathBuf, new: PathBuf, out: PathBuf },

    /// Applies the diff tar file to an existing tar file
    Apply { old: PathBuf, diff: PathBuf, out: PathBuf },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DiffMetadata {
    changed: Vec<PathBuf>,
    added: Vec<PathBuf>,
    removed: Vec<PathBuf>,
}

fn open_tar(path: &Path, _gzip: bool) -> Result<Archive<std::fs::File>> {
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

const DELTA_METADATA_FILE: &str = "__delta_metadata.json";

async fn create_index(archive: &mut Archive<std::fs::File>) -> Result<HashMap<PathBuf, IndexValue>> {
    let mut index = HashMap::new();
    let entries = archive.entries()?;
    for entry in entries {
        match entry.with_context(|| "corrupt tar".to_string()) {
            Ok(mut entry) => {
                let path = entry.path()?.to_path_buf();
                let cksum = entry.header().cksum().with_context(|| "invalid cksum".to_string())?;

                // create a Sha1 object
                let mut hasher = Sha1::new();
                copy(&mut entry, &mut hasher).with_context(|| "sha1 hashing failed".to_string())?;
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

async fn create_delta_archive(changed: HashSet<PathBuf>, new_tar: &Path,  metadata: &DiffMetadata, out: &Path) -> Result<()> {
    // We now create a new tar file that consists of
    // a metadata file and the other files.
    // It will be structured like this:
    // *: all files
    // __delta_metadata.json: the json file

    // TODO: this can be done in parallel
    let file = std::fs::File::create(out)?;
    let mut builder = Builder::new(file);

    // Write the metadata file to the tar archive AS FIRST FILE
    let metadata_path = PathBuf::from(DELTA_METADATA_FILE);
    let mut metadata_header = Header::new_old();

    let mut metadata_bytes: Vec<u8> = Vec::new();
    serde_json::to_writer(&mut metadata_bytes, &metadata).unwrap();
    metadata_header.set_size(metadata_bytes.len() as u64);
    builder.append_data(&mut metadata_header, metadata_path, &metadata_bytes[..]).with_context(|| "unable to add metadata".to_string())?;

    let mut new_tar = open_tar(new_tar, false)?;

    let entries = new_tar.entries_with_seek()?;
    for result_entry in entries {
        let mut entry = result_entry.with_context(|| "corrupt tar".to_string())?;

        let path = entry.path()?.to_path_buf();
        if changed.contains(&path) {
            let mut header = entry.header().clone();
            builder.append_data(&mut header, path, &mut entry).with_context(|| "unable to add file".to_string())?;
        }
    }
    builder.finish().with_context(|| "failed to create delta archive".to_string())
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

async fn apply_delta_archive(old: &Path, diff: &Path, out: &Path) -> Result<()> {
    let file = std::fs::File::create(out)?;
    let mut builder = Builder::new(file);


    let mut diff_tar = open_tar(diff, false)?;
    let mut entries = diff_tar.entries_with_seek()?;

    // read the metadata entry (should be first entry)
    let metadata: DiffMetadata = {
        let first = entries.next();
        if first.is_none() {
            bail!("empty delta file");
        }
        let mut entry = first.unwrap().with_context(|| "corrupt tar missing metadata".to_string())?;
        let path = entry.path()?.to_path_buf();
        if path != PathBuf::from(DELTA_METADATA_FILE) {
            bail!("delta file is missing metadata file as first entry");
        }
        serde_json::from_reader(&mut entry).with_context(|| "invalid metadata file".to_string())?
    };

    let changed: HashSet<&PathBuf> = HashSet::from_iter(metadata.changed.iter());
    let added: HashSet<&PathBuf> = HashSet::from_iter(metadata.added.iter());
    let removed: HashSet<&PathBuf> = HashSet::from_iter(metadata.removed.iter());

    // add old entries
    println!("Adding old entries..");
    let mut old_tar = open_tar(old, false)?;
    let old_entries = old_tar.entries_with_seek()?;
    for result_entry in old_entries {
        let mut entry = result_entry.with_context(|| "corrupt tar".to_string())?;

        let path = entry.path()?.to_path_buf();
        if !removed.contains(&path) && !changed.contains(&path) {
            let mut header = entry.header().clone();
            builder.append_data(&mut header, path, &mut entry).with_context(|| "unable to add old entry".to_string())?;
        }
    }

    // apply diff
    println!("Applying diff..");
    for result_entry in entries {
        let mut entry = result_entry.with_context(|| "corrupt tar".to_string())?;

        let path = entry.path()?.to_path_buf();
        if changed.contains(&path) || added.contains(&path) {
            let mut header = entry.header().clone();
            builder.append_data(&mut header, path, &mut entry).with_context(|| "unable to add diff change".to_string())?;
        }
    }

    builder.finish().with_context(|| "failed to apply delta archive".to_string())?;

    dbg!(&metadata);

    Ok(())
}

async fn apply(old: &Path, diff: &Path, out: &Path) -> Result<()> {
    apply_delta_archive(old, diff, out).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    dbg!(&args);

    match &args.command {
        Commands::Diff { old, new, out } => diff(old, new, args.gzip, &out).await?,
        Commands::Apply { old, diff, out } => apply(old, diff, &out).await?,
    }
    Ok(())
}
