#!/usr/bin/env python3
"""Split clients/client-php into a standalone history for the php-client mirror.

The split keeps every source commit, but drops assistant co-author trailers from
the messages so the public mirror credits only the humans who wrote the package.
The rewrite is deterministic: the same source commit always produces the same
mirror commit, so the mirror stays fast-forwardable across runs.

The package used to live at clients/client-laravel. `git subtree split` follows a
path and not a rename, so splitting only the current prefix would drop every
commit made before the move and diverge from the published mirror. This script
therefore splits both prefixes and grafts the current history onto the legacy
one. Commits before the move rewrite to exactly the same mirror commits as
before, so the mirror still fast-forwards.
"""

import os
import re
import subprocess
import sys

PREFIX = "clients/client-php"
LEGACY_PREFIX = "clients/client-laravel"
ASSISTANT_TRAILER = re.compile(
    r"^\s*Co-Authored-By:\s*Claude\b", re.IGNORECASE
)


def git(*args, **kwargs):
    return subprocess.run(
        ("git",) + args, check=True, capture_output=True, **kwargs
    ).stdout


def strip_assistant_trailers(message):
    kept = [
        line
        for line in message.decode("utf-8", "replace").split("\n")
        if not ASSISTANT_TRAILER.match(line)
    ]
    collapsed = re.sub(r"\n{3,}", "\n\n", "\n".join(kept))
    return (collapsed.rstrip("\n") + "\n").encode()


def split(prefix, source):
    output = subprocess.run(
        ("git", "subtree", "split", "--prefix=" + prefix, source),
        check=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    ).stdout
    return output.decode().split()[-1]


def legacy_tip(source):
    """Newest ancestor of `source` whose tree still carries the pre-rename path.

    Not the rename commit's first parent: that is the branch point. Anything
    that touched the legacy path on master after the branch was cut arrives
    through the merge's *second* parent, and taking the first one drops those
    commits from the graft, so the mirror diverges from history it already has.
    """
    for revision in git("rev-list", "--topo-order", source).decode().split():
        probe = subprocess.run(
            ("git", "rev-parse", "--verify", "-q", revision + ":" + LEGACY_PREFIX),
            capture_output=True,
        )
        if probe.returncode == 0:
            return revision
    return None


def rewrite(split_commit, graft=None, rewritten=None):
    rewritten = {} if rewritten is None else rewritten
    revisions = git(
        "rev-list", "--reverse", "--topo-order", split_commit
    ).decode().split()
    for revision in revisions:
        fields = git(
            "show", "--no-patch", "--format=%T%n%P%n%an%n%ae%n%aI%n%cn%n%ce%n%cI",
            revision,
        ).decode().split("\n")
        tree, parents = fields[0], fields[1].split()
        environment = {
            "GIT_AUTHOR_NAME": fields[2],
            "GIT_AUTHOR_EMAIL": fields[3],
            "GIT_AUTHOR_DATE": fields[4],
            "GIT_COMMITTER_NAME": fields[5],
            "GIT_COMMITTER_EMAIL": fields[6],
            "GIT_COMMITTER_DATE": fields[7],
        }
        arguments = ["commit-tree", tree]
        for parent in parents:
            arguments += ["-p", rewritten[parent]]
        if not parents and graft is not None:
            arguments += ["-p", graft]
        message = strip_assistant_trailers(git("show", "--no-patch", "--format=%B", revision))
        rewritten[revision] = subprocess.run(
            ("git",) + tuple(arguments),
            check=True,
            input=message,
            capture_output=True,
            env={**os.environ, **environment},
        ).stdout.decode().strip()
    return rewritten[revisions[-1]]


def main():
    if len(sys.argv) != 2:
        print("usage: split-php-client.py <source-commit>", file=sys.stderr)
        return 2
    source = sys.argv[1]
    legacy = legacy_tip(source)
    graft = rewrite(split(LEGACY_PREFIX, legacy)) if legacy else None
    mirror_commit = rewrite(split(PREFIX, source), graft=graft)
    expected_tree = git("rev-parse", source + ":" + PREFIX).decode().strip()
    actual_tree = git("rev-parse", mirror_commit + "^{tree}").decode().strip()
    if expected_tree != actual_tree:
        print("split tree does not match the source package", file=sys.stderr)
        return 1
    print(mirror_commit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
