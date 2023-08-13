use duct::cmd;
use git2::{ErrorCode, Repository};

use std::path::Path;

pub fn init_repo(path: &Path) -> Repository {
    match Repository::open(path) {
        Ok(r) => r,
        Err(e) => match e.code() {
            ErrorCode::NotFound => {
                println!("Creating new repository at {}", path.display());
                Repository::init(path).unwrap()
            }
            _ => {
                panic!("Failed to open repository at {}: {e}", path.display());
            }
        },
    }
}

pub fn set_upstreams(repo: &mut Repository) -> Vec<String> {
    repo.config()
        .unwrap()
        .set_bool("gc.autodetach", false)
        .unwrap();
    let response = ureq::get(
        "https://raw.githubusercontent.com/pypi-data/data/main/links/repositories_ssh.txt",
    )
    .call()
    .unwrap();
    let mut names = vec![];
    for (idx, line) in response.into_string().unwrap().lines().enumerate() {
        let name = format!("remote-{idx}", idx = idx + 1);
        let _command = cmd!(
            "git",
            "-C",
            repo.path(),
            "remote",
            "add",
            &name,
            &line,
            "--no-tags",
            "-t",
            "code"
        )
        .run()
        .unwrap();
        let _command = cmd!(
            "git",
            "-C",
            repo.path(),
            "config",
            "--bool",
            format!("remote.{name}.promisor"),
            "true"
        )
        .run()
        .unwrap();
        // let command = cmd!("git", "-C", repo.path(), "config", format!("remote.{name}.partialclonefilter"), "blob:none").run().unwrap();
        // git config --add remote.all.url
        // repo.remote_with_fetch(&name, line, "code").unwrap();
        names.push(name);
    }
    let _command = cmd!(
        "git",
        "-C",
        repo.path(),
        "config",
        "--add",
        "remotes.mirrors",
        names.join(" ")
    )
    .run()
    .unwrap();
    let _command = cmd!(
        "git",
        "-C",
        repo.path(),
        "config",
        "--bool",
        "gc.auto",
        "false"
    )
    .run()
    .unwrap();
    let _command = cmd!(
        "git",
        "-C",
        repo.path(),
        "config",
        "--bool",
        "maintenance.auto",
        "false"
    )
    .run()
    .unwrap();
    names
}

pub fn fetch_trees(repo: &mut Repository) {
    // println!("Fetching {name}...");
    let _command = cmd!(
        "git",
        "-C",
        repo.path(),
        "fetch",
        "--multiple",
        "--jobs",
        "4",
        "mirrors",
        "--depth=1",
        "--progress"
    )
    .run()
    .unwrap();
    // let command = cmd!("git", "-C", repo.path(), "fetch", "--jobs", "8", "remote-1", "--depth=1", "--filter=blob:none").run().unwrap();

    // let mut cb = RemoteCallbacks::new();
    // let mut remote = repo
    //     .find_remote(name).unwrap();
    // cb.sideband_progress(|data| {
    //     print!("remote: {}", std::str::from_utf8(data).unwrap());
    //     io::stdout().flush().unwrap();
    //     true
    // });
    // cb.transfer_progress(|stats| {
    //     if stats.received_objects() == stats.total_objects() {
    //         print!(
    //             "Resolving deltas {}/{}\r",
    //             stats.indexed_deltas(),
    //             stats.total_deltas()
    //         );
    //     } else if stats.total_objects() > 0 {
    //         print!(
    //             "Received {}/{} objects ({}) in {} bytes\r",
    //             stats.received_objects(),
    //             stats.total_objects(),
    //             stats.indexed_objects(),
    //             stats.received_bytes()
    //         );
    //     }
    //     io::stdout().flush().unwrap();
    //     true
    // });
    // let mut fo = FetchOptions::new();
    // fo.remote_callbacks(cb);
    //
    // remote.download(&[] as &[&str], Some(&mut fo)).unwrap();
    //
    // {
    //     // If there are local objects (we got a thin pack), then tell the user
    //     // how many objects we saved from having to cross the network.
    //     let stats = remote.stats();
    //     if stats.local_objects() > 0 {
    //         println!(
    //             "\rReceived {}/{} objects in {} bytes (used {} local \
    //              objects)",
    //             stats.indexed_objects(),
    //             stats.total_objects(),
    //             stats.received_bytes(),
    //             stats.local_objects()
    //         );
    //     } else {
    //         println!(
    //             "\rReceived {}/{} objects in {} bytes",
    //             stats.indexed_objects(),
    //             stats.total_objects(),
    //             stats.received_bytes()
    //         );
    //     }
    // }
    //
    // remote.disconnect().unwrap();
    //
    // remote.update_tips(None, true, AutotagOption::Unspecified, None).unwrap();
}
