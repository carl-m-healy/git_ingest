#!/usr/bin/env python3
"""query_github.py

Command-line utility to list all repositories and their branches for a
specified GitHub user.

Usage:
  python query_github.py carl-m-healy            # unauthenticated (public repos)
  python query_github.py carl-m-healy --json     # pretty-print JSON
  python query_github.py carl-m-healy --token <PERSONAL_ACCESS_TOKEN>

The token can also be provided via the GITHUB_TOKEN environment variable.
Providing a token increases the rate-limit to 5,000 requests per hour and
allows access to private repositories owned by the user.
"""
import argparse
import json
import os
import re
import sys
from typing import Dict, List

import requests
from dotenv import load_dotenv
from pathlib import Path

API_BASE = "https://api.github.com"
load_dotenv()


def _github_get(url: str, headers: dict) -> requests.Response:
    """Perform a GET request and raise helpful errors on failure."""
    resp = requests.get(url, headers=headers)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        msg = f"GitHub API error {resp.status_code}: {resp.text} ({url})"
        raise SystemExit(msg) from exc
    return resp


def _sanitize(name: str) -> str:
    """Return filesystem-safe version of *name* (replace / with __)."""
    return name.replace("/", "__")


def fetch_repos(username: str, headers: dict) -> List[dict]:
    """Return the list of repository JSON objects for *username*.
    If an authentication token is present in *headers*, we use the
    `/user/repos` endpoint so private repos owned by *username* are
    included (GitHub only returns public repos via `/users/{user}/repos`).
    """
    repos: List[dict] = []
    page = 1

    authed = "Authorization" in headers
    while True:
        if authed:
            url = (
                f"{API_BASE}/user/repos?per_page=100&affiliation=owner&visibility=all&page={page}"
            )
        else:
            url = f"{API_BASE}/users/{username}/repos?per_page=100&type=all&page={page}"

        data = _github_get(url, headers=headers).json()
        if not data:
            break

        # If authenticated, filter so we only keep repos actually owned by the
        # *username* we are interested in (token may have access to many repos).
        if authed:
            data = [repo for repo in data if repo.get("owner", {}).get("login") == username]

        repos.extend(data)
        page += 1
    return repos


def fetch_branches(owner: str, repo: str, headers: dict) -> List[str]:
    """Return a list of branch names for the given repository."""
    branches: List[str] = []
    page = 1
    while True:
        url = f"{API_BASE}/repos/{owner}/{repo}/branches?per_page=100&page={page}"
        data = _github_get(url, headers=headers).json()
        if not data:
            break
        branches.extend([b["name"] for b in data])
        page += 1
    return branches


def fetch_branches_full(owner: str, repo: str, headers: dict) -> List[dict]:
    """Return list of full branch JSON objects for repository.

    Uses the summary list endpoint then queries each branch individually
    via `/repos/{owner}/{repo}/branches/{branch}` to capture full commit
    metadata (author, committer, message, protection, etc.). If the
    detailed request fails (e.g., branch deleted between calls), falls
    back to the summary object.
    """
    detailed: List[dict] = []
    page = 1
    while True:
        url = f"{API_BASE}/repos/{owner}/{repo}/branches?per_page=100&page={page}"
        summaries = _github_get(url, headers=headers).json()
        if not summaries:
            break
        for summary in summaries:
            name = summary.get("name")
            if not name:
                detailed.append(summary)
                continue
            detail_url = f"{API_BASE}/repos/{owner}/{repo}/branches/{name}"
            try:
                detail = _github_get(detail_url, headers=headers).json()
                # Enrich with full commit object
                commit_url = detail.get("commit", {}).get("url")
                if commit_url:
                    try:
                        commit_detail = _github_get(commit_url, headers=headers).json()
                        detail["commit_full"] = commit_detail
                    except SystemExit:
                        pass
            except SystemExit:
                detail = summary  # fallback
            detailed.append(detail)
        page += 1
    return detailed


def fetch_tags_full(owner: str, repo: str, headers: dict) -> List[dict]:
    """Return list of full tag JSON objects for repository.

    For each tag retrieved from `/tags`, we also fetch its commit object
    to embed full commit metadata under `commit_full`.
    """
    tags: List[dict] = []
    page = 1
    while True:
        url = f"{API_BASE}/repos/{owner}/{repo}/tags?per_page=100&page={page}"
        summaries = _github_get(url, headers=headers).json()
        if not summaries:
            break
        for tag in summaries:
            commit_url = tag.get("commit", {}).get("url")
            if commit_url:
                try:
                    commit_detail = _github_get(commit_url, headers=headers).json()
                    tag["commit_full"] = commit_detail
                except SystemExit:
                    pass
            tags.append(tag)
        page += 1
    return tags


def list_repos_branches(username: str, token: str | None = None) -> Dict[str, List[str]]:
    """Return mapping {repo_name: [branch, ...]} for *username*."""
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    repos = fetch_repos(username, headers)
    if not repos:
        print("No repositories found for", username)
        return {}
    result: Dict[str, List[str]] = {}
    for repo in repos:
        name = repo["name"]
        try:
            result[name] = fetch_branches(username, name, headers)
        except SystemExit as err:
            print(f"[warn] Skipping {name}: {err}", file=sys.stderr)
    return result


def persist_branches(repo_br_map: Dict[str, List[str]], out_dir: str) -> None:
    """Write each repo's branches to a file inside *out_dir*.

    For every repository in *repo_br_map*, create (or update) a file
    named `<repo>.txt` containing one branch per line, sorted
    alphabetically. If the file already exists, compare old vs new
    content and print a brief summary of additions/removals.
    """
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)

    for repo, branches in repo_br_map.items():
        file = p / f"{repo}.txt"
        new_set = set(branches)
        added: set[str] = set()
        removed: set[str] = set()
        if file.exists():
            old_set = {line.strip() for line in file.read_text().splitlines() if line.strip()}
            added = new_set - old_set
            removed = old_set - new_set
        file.write_text("\n".join(sorted(new_set)) + "\n")
        if added or removed:
            print(f"{repo}: +{len(added)} -{len(removed)} branches updated")


def persist_repo_json(repo_data: Dict[str, dict], out_dir: str) -> None:
    """Persist each repo's full JSON data to `<out_dir>/<repo>.json`."""
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    for name, payload in repo_data.items():
        file = p / f"{name}.json"
        serialized = json.dumps(payload, indent=2, sort_keys=True)
        changed = True
        if file.exists() and file.read_text() == serialized:
            changed = False
        file.write_text(serialized + "\n")
        if changed:
            print(f"{name}: JSON updated (size {len(serialized)} bytes)")


def persist_branch_json(branch_map: Dict[str, List[dict]], out_dir: str) -> None:
    """Persist each branch JSON to `<out_dir>/<repo>/<branch>.json`."""
    base = Path(out_dir)
    for repo, branches in branch_map.items():
        repo_dir = base / repo
        repo_dir.mkdir(parents=True, exist_ok=True)
        for br in branches:
            fname = _sanitize(br.get("name", "unnamed")) + ".json"
            file = repo_dir / fname
            serialized = json.dumps(br, indent=2, sort_keys=True)
            changed = True
            if file.exists() and file.read_text() == serialized:
                changed = False
            file.write_text(serialized + "\n")
            if changed:
                print(f"{repo}/{br.get('name')}: branch JSON updated")


def persist_tag_json(tag_map: Dict[str, List[dict]], out_dir: str) -> None:
    """Persist each tag JSON to `<out_dir>/<repo>/<tag>.json`."""
    base = Path(out_dir)
    for repo, tags in tag_map.items():
        repo_dir = base / repo
        repo_dir.mkdir(parents=True, exist_ok=True)
        for tg in tags:
            name = tg.get("name", "unnamed")
            fname = _sanitize(name) + ".json"
            file = repo_dir / fname
            serialized = json.dumps(tg, indent=2, sort_keys=True)
            changed = True
            if file.exists() and file.read_text() == serialized:
                changed = False
            file.write_text(serialized + "\n")
            if changed:
                print(f"{repo}/{name}: tag JSON updated")


def cli() -> None:
    parser = argparse.ArgumentParser(description="List all GitHub repos and branches for a user.")
    parser.add_argument("username", help="GitHub username to query")
    parser.add_argument("--token", help="GitHub Personal Access Token (or set GITHUB_TOKEN env var)")
    parser.add_argument("--json", action="store_true", help="Output result as JSON")
    parser.add_argument("--save-dir", help="Directory to persist per-repo branch files (overwrites old state)")
    parser.add_argument("--save-json-dir", help="Directory to persist full repo + branches JSON file")
    parser.add_argument("--save-branch-json-dir", help="Directory to persist each branch JSON individually")
    parser.add_argument("--save-tag-json-dir", help="Directory to persist each tag JSON individually")
    args = parser.parse_args()

    token = args.token or os.getenv("GITHUB_TOKEN")
    data = list_repos_branches(args.username, token)

    if args.json:
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        for repo, branches in data.items():
            print(repo)
            for br in branches:
                print(f"  └─ {br}")

    if args.save_dir:
        persist_branches(data, args.save_dir)

    if args.save_json_dir or args.save_branch_json_dir or args.save_tag_json_dir:
        full_map: Dict[str, dict] = {}
        branch_map: Dict[str, List[dict]] = {}
        tag_map: Dict[str, List[dict]] = {}
        headers = {"Accept": "application/vnd.github+json"}
        if token:
            headers["Authorization"] = f"token {token}"
        for repo in fetch_repos(args.username, headers):
            name = repo["name"]
            branches_json = fetch_branches_full(args.username, name, headers)
            tags_json = fetch_tags_full(args.username, name, headers) if args.save_tag_json_dir or args.save_json_dir else []
            if args.save_json_dir:
                full_map[name] = {"repo": repo, "branches": branches_json, "tags": tags_json}
            branch_map[name] = branches_json
            tag_map[name] = tags_json

        if args.save_json_dir:
            persist_repo_json(full_map, args.save_json_dir)

        if args.save_branch_json_dir:
            persist_branch_json(branch_map, args.save_branch_json_dir)

        if args.save_tag_json_dir:
            persist_tag_json(tag_map, args.save_tag_json_dir)


if __name__ == "__main__":
    cli()
