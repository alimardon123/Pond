#!/usr/bin/env python3
"""
Pond Git — CLI application.

A real Git replacement built on the Pond kernel.
Tests the full developer experience for version control.

Usage:
  python3 app.py init                        # initialize repo
  python3 app.py add <path> <content>        # stage a file
  python3 app.py commit <message>            # commit staged changes
  python3 app.py log                         # show history
  python3 app.py ls                          # list files
  python3 app.py cat <path>                  # read a file
  python3 app.py branch <name>               # create a branch
  python3 app.py checkout <branch>           # switch to a branch
  python3 app.py merge <branch>              # merge a branch
  python3 app.py diff <a> <b>                # diff two commits
  python3 app.py rm <path>                   # remove a file
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_git import PondGit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
from kernel import PondMinimal


def get_repo():
    repo_dir = os.environ.get("POND_GIT_DIR", ".pond_git")
    kernel = PondMinimal(repo_dir)
    git = PondGit(kernel, "repo")
    # Load staging area from temp file
    staging_file = os.path.join(repo_dir, "staging.json")
    if os.path.exists(staging_file):
        with open(staging_file) as f:
            state = json.load(f)
        git._staged = {k: bytes.fromhex(v) for k, v in state.get("staged", {}).items()}
        git._deleted = set(state.get("deleted", []))
    return kernel, git


def save_staging(kernel, git):
    repo_dir = os.environ.get("POND_GIT_DIR", ".pond_git")
    staging_file = os.path.join(repo_dir, "staging.json")
    state = {
        "staged": {k: v.hex() for k, v in git._staged.items()},
        "deleted": list(git._deleted),
    }
    with open(staging_file, "w") as f:
        json.dump(state, f)


def cmd_init(args):
    kernel, git = get_repo()
    h = git.init()
    print(f"Initialized Pond Git repo. Initial commit: {h[:12]}")
    kernel.close()


def cmd_add(args):
    if len(args) < 2:
        print("Usage: add <path> <content>")
        sys.exit(1)
    path, content = args[0], args[1]
    kernel, git = get_repo()
    git.add(path, content.encode())
    save_staging(kernel, git)
    print(f"Staged '{path}'")
    kernel.close()


def cmd_commit(args):
    if not args:
        print("Usage: commit <message>")
        sys.exit(1)
    message = " ".join(args)
    kernel, git = get_repo()
    h = git.commit(message)
    # Clear staging after commit
    save_staging(kernel, git)
    print(f"[{h[:12]}] {message}")
    kernel.close()


def cmd_log(args):
    kernel, git = get_repo()
    for entry in git.log():
        print(f"  {entry['commit']}  {entry['message']}")
    kernel.close()


def cmd_ls(args):
    kernel, git = get_repo()
    files = git.ls()
    for f in files:
        print(f"  {f}")
    print(f"\n{len(files)} file(s)")
    kernel.close()


def cmd_cat(args):
    if not args:
        print("Usage: cat <path>")
        sys.exit(1)
    kernel, git = get_repo()
    content = git.cat(args[0])
    print(content.decode())
    kernel.close()


def cmd_branch(args):
    if not args:
        kernel, git = get_repo()
        branches = git.branches()
        if not branches:
            print("(no branches)")
        for b in branches:
            print(f"  {b}")
        kernel.close()
    else:
        kernel, git = get_repo()
        git.branch(args[0])
        print(f"Created branch '{args[0]}'")
        kernel.close()


def cmd_checkout(args):
    if not args:
        print("Usage: checkout <branch>")
        sys.exit(1)
    kernel, git = get_repo()
    git.checkout(args[0])
    print(f"Switched to branch '{args[0]}'")
    kernel.close()


def cmd_merge(args):
    if not args:
        print("Usage: merge <branch>")
        sys.exit(1)
    kernel, git = get_repo()
    h = git.merge(args[0])
    print(f"Merged '{args[0]}' -> [{h[:12]}]")
    kernel.close()


def cmd_diff(args):
    if len(args) < 2:
        print("Usage: diff <commit_a> <commit_b>")
        sys.exit(1)
    kernel, git = get_repo()
    d = git.diff(args[0], args[1])
    print(f"Added: {list(d['added'].keys())}")
    print(f"Removed: {list(d['removed'].keys())}")
    print(f"Modified: {list(d['modified'].keys())}")
    kernel.close()


def cmd_rm(args):
    if not args:
        print("Usage: rm <path>")
        sys.exit(1)
    kernel, git = get_repo()
    git.rm(args[0])
    save_staging(kernel, git)
    print(f"Removed '{args[0]}'")
    kernel.close()


COMMANDS = {
    "init": cmd_init,
    "add": cmd_add,
    "commit": cmd_commit,
    "log": cmd_log,
    "ls": cmd_ls,
    "cat": cmd_cat,
    "branch": cmd_branch,
    "checkout": cmd_checkout,
    "merge": cmd_merge,
    "diff": cmd_diff,
    "rm": cmd_rm,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[sys.argv[1]](sys.argv[2:])


if __name__ == "__main__":
    main()
