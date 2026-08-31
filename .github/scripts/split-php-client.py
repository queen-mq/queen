#!/usr/bin/env python3
"""Split clients/client-laravel into a standalone history for the php-client mirror.

The split keeps every source commit, but drops assistant co-author trailers from
the messages so the public mirror credits only the humans who wrote the package.
The rewrite is deterministic: the same source commit always produces the same
mirror commit, so the mirror stays fast-forwardable across runs.
"""

import os
import re
import subprocess
import sys

PREFIX = "clients/client-laravel"
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


def split(source):
    output = subprocess.run(
        ("git", "subtree", "split", "--prefix=" + PREFIX, source),
        check=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    ).stdout
    return output.decode().split()[-1]


def rewrite(split_commit):
    rewritten = {}
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
    mirror_commit = rewrite(split(source))
    expected_tree = git("rev-parse", source + ":" + PREFIX).decode().strip()
    actual_tree = git("rev-parse", mirror_commit + "^{tree}").decode().strip()
    if expected_tree != actual_tree:
        print("split tree does not match the source package", file=sys.stderr)
        return 1
    print(mirror_commit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
