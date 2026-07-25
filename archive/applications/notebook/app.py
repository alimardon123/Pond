#!/usr/bin/env python3
"""
Pond Notebook — CLI application.

A real, usable notebook built on the Pond kernel.
Tests the full developer experience: is it pleasant or painful?

Usage:
  python3 app.py init                    # initialize notebook
  python3 app.py create <path> <title>   # create a page
  python3 app.py read <path>             # read a page
  python3 app.py edit <path>             # edit a page (body via stdin)
  python3 app.py delete <path>           # delete a page
  python3 app.py list                    # list all pages
  python3 app.py search <query>          # search pages
  python3 app.py history                 # show commit history
  python3 app.py branch <name>           # create a branch
  python3 app.py checkout <branch>       # switch to a branch
  python3 app.py undo [N]                # undo N commits (default 1)
  python3 app.py diff <a> <b>            # diff two commits
  python3 app.py attach <filename>       # add attachment from stdin
  python3 app.py get-attach <filename>   # retrieve attachment to stdout
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from notebook import NotebookLens

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
from kernel import PondMinimal


def get_kernel_and_notebook(notebook_dir=None):
    notebook_dir = notebook_dir or os.environ.get("POND_NOTEBOOK_DIR", ".pond_notebook")
    kernel = PondMinimal(notebook_dir)
    nb = NotebookLens(kernel, "notebook")
    return kernel, nb


def cmd_init(args):
    kernel, nb = get_kernel_and_notebook()
    # Create initial empty commit
    tree_h = kernel.write(b'{"type":"tree","entries":{}}')
    # write_commit is in notebook.py
    from notebook import write_commit
    commit_h = write_commit(kernel, tree_h, None, "initialize notebook")
    kernel.reference("notebook", commit_h)
    print(f"Notebook initialized at {os.path.abspath(os.environ.get('POND_NOTEBOOK_DIR', '.pond_notebook'))}")
    print(f"Initial commit: {commit_h[:12]}")
    kernel.close()


def cmd_create(args):
    if len(args) < 2:
        print("Usage: create <path> <title> [body]")
        sys.exit(1)
    path, title = args[0], args[1]
    body = args[2] if len(args) > 2 else ""
    kernel, nb = get_kernel_and_notebook()
    page = nb.create_page(path, title, body)
    nb.commit(f"create '{title}' at {path}")
    print(f"Created page '{path}' with title '{title}'")
    kernel.close()


def cmd_read(args):
    if not args:
        print("Usage: read <path>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    page = nb.read_page(args[0])
    if page is None:
        print(f"Page '{args[0]}' not found")
        sys.exit(1)
    print(f"# {page.title}")
    print(f"Path: {args[0]}")
    print(f"Tags: {', '.join(page.tags) if page.tags else '(none)'}")
    print(f"Updated: {time.ctime(page.updated_at)}")
    print()
    print(page.body)
    kernel.close()


def cmd_edit(args):
    if not args:
        print("Usage: edit <path> [title]")
        sys.exit(1)
    path = args[0]
    title = args[1] if len(args) > 1 else None
    kernel, nb = get_kernel_and_notebook()
    # Read body from stdin
    print("Enter body (Ctrl+D to finish):")
    body = sys.stdin.read().strip()
    nb.update_page(path, title=title, body=body)
    nb.commit(f"edit '{path}'")
    print(f"Updated page '{path}'")
    kernel.close()


def cmd_delete(args):
    if not args:
        print("Usage: delete <path>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    nb.delete_page(args[0])
    nb.commit(f"delete '{args[0]}'")
    print(f"Deleted page '{args[0]}'")
    kernel.close()


def cmd_list(args):
    kernel, nb = get_kernel_and_notebook()
    pages = nb.list_pages()
    if not pages:
        print("(no pages)")
    for p in pages:
        tags = f" [{', '.join(p['tags'])}]" if p["tags"] else ""
        print(f"  {p['path']:<30} {p['title']}{tags}")
    print(f"\n{len(pages)} page(s)")
    kernel.close()


def cmd_search(args):
    if not args:
        print("Usage: search <query>")
        sys.exit(1)
    query = " ".join(args)
    kernel, nb = get_kernel_and_notebook()
    results = nb.search(query)
    if not results:
        print(f"No results for '{query}'")
    for r in results:
        print(f"  {r['path']:<30} {r['title']}")
        if r["context"]:
            print(f"    {r['context']}")
    print(f"\n{len(results)} result(s)")
    kernel.close()


def cmd_history(args):
    kernel, nb = get_kernel_and_notebook()
    history = nb.history()
    if not history:
        print("(no history)")
    for h in history:
        print(f"  {h['commit']}  {time.ctime(h['timestamp'])}  {h['message']}")
    kernel.close()


def cmd_branch(args):
    if not args:
        print("Usage: branch <name>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    full_name = nb.create_branch(args[0])
    print(f"Created branch '{args[0]}' (commit: {kernel.resolve(full_name)[:12]})")
    kernel.close()


def cmd_checkout(args):
    if not args:
        print("Usage: checkout <branch>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    nb.checkout_branch(args[0])
    print(f"Switched to branch '{args[0]}'")
    kernel.close()


def cmd_undo(args):
    steps = int(args[0]) if args else 1
    kernel, nb = get_kernel_and_notebook()
    commit = nb.undo(steps)
    print(f"Undid {steps} commit(s). Now at {commit}")
    kernel.close()


def cmd_diff(args):
    if len(args) < 2:
        print("Usage: diff <commit_a> <commit_b>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    d = nb.diff(args[0], args[1])
    print(f"Added: {list(d['added'].keys())}")
    print(f"Removed: {list(d['removed'].keys())}")
    print(f"Modified: {list(d['modified'].keys())}")
    kernel.close()


def cmd_attach(args):
    if not args:
        print("Usage: attach <filename>")
        sys.exit(1)
    data = sys.stdin.buffer.read()
    kernel, nb = get_kernel_and_notebook()
    h = nb.add_attachment(args[0], data)
    nb.commit(f"attach '{args[0]}'")
    print(f"Attached '{args[0]}' ({len(data)} bytes, hash {h[:12]})")
    kernel.close()


def cmd_get_attach(args):
    if not args:
        print("Usage: get-attach <filename>")
        sys.exit(1)
    kernel, nb = get_kernel_and_notebook()
    data = nb.get_attachment(args[0])
    sys.stdout.buffer.write(data)
    kernel.close()


COMMANDS = {
    "init": cmd_init,
    "create": cmd_create,
    "read": cmd_read,
    "edit": cmd_edit,
    "delete": cmd_delete,
    "list": cmd_list,
    "search": cmd_search,
    "history": cmd_history,
    "branch": cmd_branch,
    "checkout": cmd_checkout,
    "undo": cmd_undo,
    "diff": cmd_diff,
    "attach": cmd_attach,
    "get-attach": cmd_get_attach,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    args = sys.argv[2:]
    COMMANDS[cmd](args)


if __name__ == "__main__":
    main()
