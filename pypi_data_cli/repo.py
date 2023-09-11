import dataclasses
from functools import cached_property
from pathlib import Path
from typing import Iterator, Tuple, cast

import pygit2


def iter_repos(base: Path) -> Iterator[Tuple[Path, str]]:
    for repo in base.iterdir():
        commits = (repo / "commits.txt").read_text().splitlines(keepends=False)
        for commit in commits:
            yield repo, commit


@dataclasses.dataclass(frozen=True)
class RepoToParse:
    repo_path: Path
    commit_oid: str
    job_index: int

    @cached_property
    def git_repo(self) -> pygit2.Repository:
        return pygit2.Repository(self.repo_path)

    @cached_property
    def git_commit(self) -> pygit2.Commit:
        return self.git_repo.get(self.commit_oid)

    @cached_property
    def git_tree(self) -> pygit2.Tree:
        return self.git_commit.tree

    def count_tree_blobs(self) -> int:
        total_items = 0
        for item, _ in _walk_tree(self.git_tree, ""):
            total_items += 1
        return total_items

    def walk_tree(self) -> Iterator[Tuple[pygit2.Blob, str]]:
        yield from _walk_tree(self.git_tree, "")


def _walk_tree(
        tree: pygit2.Tree, root_path: str
) -> Iterator[Tuple[pygit2.Blob, str]]:
    for item in tree:
        if item.type == pygit2.GIT_OBJ_TREE:
            # if not tree_bloom_filter.add_if_not_contains(item.oid.raw):
            yield from _walk_tree(cast(pygit2.Tree, item), f"{root_path}/{item.name}")
        elif item.type == pygit2.GIT_OBJ_BLOB:
            # if not bloom_filter_blobs.add_if_not_contains(item.oid.raw):
            yield item, f"{root_path}/{item.name}"
