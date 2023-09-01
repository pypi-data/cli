use std::borrow::Cow;
use std::path::{Path, PathBuf};

use bloomfilter::Bloom;
use git2::{BranchType, TreeWalkMode};
use glob::Pattern;
use rayon::prelude::*;
use regex::bytes::Regex;
use tracing::info;

use crate::output::{OutputDriver, OutputMode, Payload};

thread_local! {
    pub static NEWLINE_PATTERN: Regex = Regex::new(r"(\r\n|\r)").unwrap();
}
#[inline]
pub fn normalize(item: &[u8]) -> Cow<[u8]> {
    NEWLINE_PATTERN.with(|pattern| pattern.replace_all(item, b"\n"))
}

#[derive(Clone)]
pub enum SearchType {
    // Regex(Regex),
    Glob(Pattern),
}

impl SearchType {
    #[inline(always)]
    pub fn matches_path(&self, file_path: &Path) -> bool {
        match self {
            // SearchType::Regex(r) => r.is_match(file_path.to_str().unwrap().as_bytes()),
            SearchType::Glob(g) => g.matches_path(file_path),
        }
    }
}

pub fn parse_files(
    data_dir: PathBuf,
    search_type: SearchType,
    search_contents: Option<Regex>,
    output: OutputMode,
) -> anyhow::Result<()> {
    let repositories: Vec<_> = std::fs::read_dir(data_dir).unwrap().flatten().collect();
    git2::opts::enable_caching(false);
    git2::opts::strict_hash_verification(false);

    repositories.into_par_iter().try_for_each(|r| {
        let search_type = search_type.clone();
        let search_contents = search_contents.clone();

        let repo = git2::Repository::open(r.path())?;
        let odb = repo.odb()?;
        let code_branch = repo.find_branch("origin/code", BranchType::Remote)?;
        let tree = code_branch.into_reference().peel_to_tree()?;
        let mut total_items = 0;
        odb.foreach(|_item| {
            total_items += 1;
            true
        })?;

        let mut output = OutputDriver::new(output.clone());

        let fp_rate = 0.001;

        let mut bloom = Bloom::new_for_fp_rate(total_items, fp_rate);
        info!(
            "Total items: {total_items}. Bloom size: {} kb",
            bloom.bitmap().len() / 1024
        );

        tree.walk(TreeWalkMode::PostOrder, |directory, entry| {
            let name = match entry.name() {
                Some(n) => n,
                None => {
                    return git2::TreeWalkResult::Ok;
                }
            };
            let path = Path::new(&directory).join(name);

            if search_type.matches_path(&path) && !bloom.check_and_set(entry.id().as_bytes()) {
                let oid = entry.id();

                let contents = match odb.read(oid) {
                    Ok(d) => d,
                    Err(_) => {
                        return git2::TreeWalkResult::Ok;
                    }
                };
                let contents = contents.data();

                let does_match = match &search_contents {
                    Some(r) => r.is_match(contents),
                    None => true,
                };

                if does_match {
                    let contents = normalize(contents);
                    let source = String::from_utf8_lossy(&contents);
                    let payload = Payload {
                        oid: oid.to_string(),
                        path,
                        contents: source,
                    };
                    if output.push(payload).is_err() {
                        return git2::TreeWalkResult::Abort;
                    }
                }
            }
            git2::TreeWalkResult::Ok
        })?;
        output.flush()?;

        info!("Done {}", r.path().display());
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
