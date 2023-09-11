import dataclasses
import fnmatch
import re
from typing import List


@dataclasses.dataclass(frozen=True)
class Matcher:
    # negate: bool

    def match(self, value: bytes) -> bool:
        raise NotImplementedError()

    @classmethod
    def parse(cls, value: str) -> 'Matcher':
        raise NotImplementedError()


@dataclasses.dataclass(frozen=True)
class PathMatcher(Matcher):
    def match_path(self, value: bytes) -> bool:
        return self.match(value)


@dataclasses.dataclass(frozen=True)
class GlobMatcher(PathMatcher):
    pattern: bytes

    def match_path(self, value: bytes) -> bool:
        return fnmatch.fnmatch(value, self.pattern)

    @classmethod
    def parse(cls, value: str) -> 'PathMatcher':
        return GlobMatcher(value.encode())


@dataclasses.dataclass(frozen=True)
class RegexMatcher(Matcher):
    pattern: re.Pattern

    def match(self, value: bytes) -> bool:
        return bool(self.pattern.search(value))

    @classmethod
    def parse(cls, value: str) -> 'Matcher':
        return RegexMatcher(pattern=re.compile(value))


@dataclasses.dataclass(frozen=True)
class LiteralMatcher(Matcher):
    needle: bytes

    def match(self, value: bytes) -> bool:
        return self.needle in value

    @classmethod
    def parse(cls, value: str) -> 'Matcher':
        return LiteralMatcher(needle=value.encode())


@dataclasses.dataclass(frozen=True)
class MatcherGroup:
    globs: List[GlobMatcher]
    literals: List[LiteralMatcher]
    regex: List[RegexMatcher]

    def is_path_matched(self, value: bytes) -> bool:
        if not self.globs:
            return True
        return any(m.match_path(value) for m in self.globs)

    def is_contents_matched(self, value: bytes) -> bool:
        if not self.literals and not self.regex:
            return True

        # prune literals first
        if self.literals:
            if not any(m.match(value) for m in self.literals):
                return False

        if self.regex:
            if not any(m.match(value) for m in self.regex):
                return False

        return True
