import fnmatch
import functools
import multiprocessing
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated, List

import pygit2
import tqdm
import typer

from pypi_data_cli.matcher import GlobMatcher, LiteralMatcher, RegexMatcher, MatcherGroup
from pypi_data_cli.repo import RepoToParse, iter_repos

app = typer.Typer()


@app.command()
def bootstrap(input_directory: Path, output_directory: Path):
    stems = {p.name.split(".")[0] for p in input_directory.iterdir() if p.is_file()}
    for stem in stems:
        commits = input_directory / f"{stem}.commits.txt"
        idx = input_directory / f"{stem}.idx"
        pack = input_directory / f"{stem}.pack"
        rev = input_directory / f"{stem}.rev"

        output_dir = output_directory / stem
        output_dir.mkdir(parents=True, exist_ok=False)

        subprocess.check_call(["git", "init", "--bare", str(output_dir / ".git")])

        pack_dir = output_dir / ".git" / "objects" / "pack"
        pack_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(idx, pack_dir / idx.name)
        shutil.copy(pack, pack_dir / pack.name)
        shutil.copy(rev, pack_dir / rev.name)
        shutil.copy(commits, output_dir / "commits.txt")


@app.command()
def parse(
        glob: Annotated[List[GlobMatcher], typer.Option(parser=GlobMatcher.parse)] = None,
        literal: Annotated[List[LiteralMatcher], typer.Option(parser=LiteralMatcher.parse)] = None,
        regex: Annotated[List[RegexMatcher], typer.Option(parser=RegexMatcher.parse)] = None,
):
    base = Path("~/tmp/repos/").expanduser()
    matcher = MatcherGroup(globs=glob or [], literals=literal or [], regex=regex or [])

    with multiprocessing.Pool() as executor:
        # for idx, v in enumerate(ref.keys()):
        # print_repo(base / v, idx, pattern, glob, literal, extract)
        input_data: List[RepoToParse] = []
        for i, (repo, commit) in enumerate(iter_repos(base)):
            input_data.append(RepoToParse(repo, commit, i))
        import random, json
        mapper = executor.imap(functools.partial(print_repo, matcher), random.sample(input_data, 50))
        print(json.dumps(list(mapper)))


def print_repo(
        matcher: MatcherGroup,
        repo: RepoToParse,
):
    position = multiprocessing.current_process()._identity[0]
    settings = pygit2.Settings()
    settings.enable_caching(True)
    settings.cache_max_size(1024 * 1024 * 1024)
    settings.cache_object_limit(pygit2.GIT_OBJ_BLOB, 1024 * 1024 * 1024)
    settings.cache_object_limit(pygit2.GIT_OBJ_TREE, 1024 * 1024 * 1024)

    blobs = repo.count_tree_blobs()
    matched_oids = set()
    non_matched_oids = set()
    total_seen = 0
    total_excluded = 0
    for git_obj, path in tqdm.tqdm(
            repo.walk_tree(),
            total=blobs,
            position=position,
            mininterval=1,
    ):
        if matcher.is_path_matched(path.encode()):
            total_seen += 1
            if git_obj.oid not in matched_oids:
                matched_oids.add(git_obj.oid)
        else:
            total_excluded += 1
            non_matched_oids.add(git_obj.oid)

    return {
        "path": str(repo.repo_path),
        "matched": len(matched_oids),
        "total_seen": total_seen,
        "percent_seen": (len(matched_oids) / total_seen) * 100,

        "total_excluded": total_excluded,
        "non_matched": len(non_matched_oids),
        "percent_excluded": (len(non_matched_oids) / total_excluded) * 100,
    }

    # for tick_idx, (item, path) in enumerate(
    #         pbar := tqdm.tqdm(
    #             walk_tree(tree, total_items),
    #             desc=f"{name} - {commit_id[:6]} - {idx}/{total}",
    #             total=total_items,
    #             position=position,
    #             leave=True,
    #             mininterval=1,
    #             bar_format="{desc}: {percentage:3.0f}% | {n_fmt}/{total_fmt} [{elapsed}<{remaining} {rate_fmt}{postfix}]",
    #         )
    # ):
    #     if tick_idx % 50000 == 0:
    #         cached_mem, total_mem = settings.cached_memory
    #         pbar.set_postfix(
    #             {"cache": pbar.format_sizeof(cached_mem), "total": pbar.format_sizeof(total_mem)},
    #             refresh=False,
    #         )
    #     is_fn_match = fnmatch.fnmatch(path, glob) if glob else True
    #     if is_fn_match:
    #         data: bytes = odb.read(item.id)[1]
    #         # if b"stackoverflow" not in data and b"stackexchange" not in data:
    #         #     continue
    #         m = _is_match(data)
    #         if m:
    #             # path_bytes = path.encode()
    #             project_name = path.split("/")[2].encode()
    #             if isinstance(m, list):
    #                 sys.stdout.buffer.writelines(
    #                     project_name + b"\t" + match + b"\n" for match in m
    #                 )
    #             else:
    #                 sys.stdout.buffer.write(data)


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    app()
