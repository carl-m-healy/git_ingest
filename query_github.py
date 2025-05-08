#!/usr/bin/env python3
from __future__ import annotations
import os  # placed before requests import to affect SSL verification

# TEMPORARY: bypass SSL certificate verification for testing only.
# Remove or comment out for production use.
os.environ.setdefault("PYTHONHTTPSVERIFY", "0")

"""query_github.py

Command-line utility to list all repositories and their branches for a
specified GitHub user or organization.

Usage:
  python query_github.py carl-m-healy            # unauthenticated (public repos)
  python query_github.py carl-m-healys --json     # pretty-print JSON
  python query_github.py carl-m-healy --token <PERSONAL_ACCESS_TOKEN>
  python query_github.py carl-m-healy --graphql  # use GitHub GraphQL API (v4)
  python query_github.py my-org --org           # query organization instead of user

The token can also be provided via the GITHUB_TOKEN environment variable.
Providing a token increases the rate-limit to 5,000 requests per hour and
allows access to private repositories owned by the user or organization.
"""
import argparse
import json
import logging
import sys
import textwrap
from datetime import datetime
from typing import Dict, List
import requests
from dotenv import load_dotenv
from pathlib import Path
import time

# Load environment variables from .env located next to this script (project root).
# Use override=True so values in the file replace any existing environment
# variables (e.g. a blank or outdated GITHUB_TOKEN already present in the
# shell), ensuring the token in `.env` is actually picked up.
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

API_BASE = "https://api.github.com"
GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
API_CALL_COUNT = 0

# Request / proxy resilience settings
REQUEST_TIMEOUT = (5, 30)  # (connect_timeout, read_timeout) in seconds
MAX_RETRIES = 3

# Paging sizes tunable via environment (smaller sizes help stay under strict proxy timeouts)
REST_PAGE_SIZE = int(os.getenv("GITHUB_REST_PAGE_SIZE", "10"))  # default 50 items
GRAPHQL_PAGE_SIZE = int(os.getenv("GITHUB_GRAPHQL_PAGE_SIZE", "10"))  # default 50 nodes

# Module-level logger so library functions are usable without invoking the
# `cli()` entry-point (which reconfigures logging). Tests import the module
# directly and call helpers, therefore we need a default logger instance to
# avoid NameError.
logger = logging.getLogger(__name__)

def _github_get(url: str, headers: dict, *, timeout: tuple = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES) -> requests.Response:
    """Perform a GET request with retry/backoff, specifically retrying on 504.

    We retry on:
        * HTTP 504 Gateway Timeout returned by the proxy
        * Low-level connection / read timeouts raised by *requests*
    A simple exponential back-off (2^attempt seconds) is used between tries.
    """
    for attempt in range(1, max_retries + 1):
        global API_CALL_COUNT
        API_CALL_COUNT += 1
        start = time.perf_counter()
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            duration = time.perf_counter() - start
            logger.debug("GET %s completed in %.3fs", url, duration)
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code
            # Only retry on 504; all other errors break immediately.
            if status != 504 or attempt == max_retries:
                duration = time.perf_counter() - start
                logger.warning("GET %s failed after %.3fs: %s", url, duration, exc)
                raise SystemExit(f"GitHub API error {status}: {exc.response.text} ({url})") from exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            # Retry connection/timeout errors up to *max_retries*
            if attempt == max_retries:
                duration = time.perf_counter() - start
                logger.warning("GET %s connection error after %.3fs: %s", url, duration, exc)
                raise SystemExit(f"GitHub connection error: {exc}") from exc
        # Exponential back-off before the next attempt
        sleep_s = 2 ** attempt
        logger.info("Retrying GET %s in %ds (attempt %d/%d)...", url, sleep_s, attempt, max_retries)
        time.sleep(sleep_s)


def _github_graphql(query: str, variables: dict, headers: dict, *, timeout: tuple = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES) -> dict:
    """Execute a GraphQL query with retry/backoff logic mirroring *_github_get*."""
    for attempt in range(1, max_retries + 1):
        global API_CALL_COUNT
        API_CALL_COUNT += 1
        start = time.perf_counter()
        try:
            resp = requests.post(
                GRAPHQL_ENDPOINT,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            duration = time.perf_counter() - start
            logger.debug("GraphQL request completed in %.3fs", duration)
            payload = resp.json()
            if payload.get("errors"):
                raise SystemExit(f"GitHub GraphQL errors: {payload['errors']}")
            return payload.get("data", {})
        except requests.HTTPError as exc:
            status = exc.response.status_code
            if status != 504 or attempt == max_retries:
                duration = time.perf_counter() - start
                logger.warning("GraphQL request failed after %.3fs: %s", duration, exc)
                raise SystemExit(
                    f"GitHub GraphQL error {status}: {exc.response.text}"
                ) from exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_retries:
                duration = time.perf_counter() - start
                logger.warning("GraphQL connection error after %.3fs: %s", duration, exc)
                raise SystemExit(f"GitHub connection error: {exc}") from exc
        sleep_s = 2 ** attempt
        logger.info(
            "Retrying GraphQL request in %ds (attempt %d/%d)...", sleep_s, attempt, max_retries
        )
        time.sleep(sleep_s)


def _sanitize(name: str) -> str:
    """Return filesystem-safe version of *name* (replace / with __)."""
    return name.replace("/", "__")


def fetch_repos(username: str, headers: dict, is_org: bool = False) -> List[dict]:
    """Return the list of repository JSON objects for *username*.
    If an authentication token is present in *headers*, we use the
    `/user/repos` endpoint so private repos owned by *username* are
    included (GitHub only returns public repos via `/users/{user}/repos`).
    """
    repos: List[dict] = []
    page = 1

    authed = "Authorization" in headers
    while True:
        if is_org:
            # Organization repositories endpoint (public + private with proper permissions)
            url = f"{API_BASE}/orgs/{username}/repos?per_page={REST_PAGE_SIZE}&type=all&page={page}"
        elif authed:
            url = (
                f"{API_BASE}/user/repos?per_page={REST_PAGE_SIZE}&affiliation=owner&visibility=all&page={page}"
            )
        else:
            url = f"{API_BASE}/users/{username}/repos?per_page={REST_PAGE_SIZE}&type=all&page={page}"

        data = _github_get(url, headers=headers).json()
        if not data:
            break

        # If authenticated, filter so we only keep repos actually owned by the
        # *username* we are interested in (token may have access to many repos).
        if authed and not is_org:
            # For user endpoint we may receive repos not owned by *username*; filter them.
            data = [repo for repo in data if repo.get("owner", {}).get("login") == username]

        repos.extend(data)
        page += 1
    return repos


def fetch_branches(owner: str, repo: str, headers: dict) -> List[str]:
    """Return a list of branch names for the given repository."""
    branches: List[str] = []
    page = 1
    while True:
        url = f"{API_BASE}/repos/{owner}/{repo}/branches?per_page={REST_PAGE_SIZE}&page={page}"
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
        url = f"{API_BASE}/repos/{owner}/{repo}/branches?per_page={REST_PAGE_SIZE}&page={page}"
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
        url = f"{API_BASE}/repos/{owner}/{repo}/tags?per_page={REST_PAGE_SIZE}&page={page}"
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


def list_repos_branches(username: str, token: str | None = None, is_org: bool = False) -> Dict[str, List[str]]:
    """Return mapping {repo_name: [branch, ...]} for *username*."""
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    repos = fetch_repos(username, headers, is_org)
    if not repos:
        logger.warning("No repositories found for %s", username)
        print("No repositories found for", username)
        return {}
    result: Dict[str, List[str]] = {}
    for repo in repos:
        name = repo["name"]
        try:
            result[name] = fetch_branches(username, name, headers)
        except SystemExit as err:
            logger.warning("Skipping %s: %s", name, err)
            print(f"[warn] Skipping {name}: {err}", file=sys.stderr)
    return result


def list_repos_branches_graphql(username: str, token: str, is_org: bool = False) -> Dict[str, List[str]]:
    """Return mapping {repo_name: [branch, ...]} using GitHub GraphQL v4.

    Retrieves repositories in batches of 100 and, for each repository, grabs
    up to 100 branches per request (paginating when necessary). Because the
    GraphQL API allows nested data to be fetched in a single call, this
    approach typically requires an order-of-magnitude fewer HTTP requests
    compared to the REST fallback above.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"bearer {token}",
    }

    result: Dict[str, List[str]] = {}
    repo_after: str | None = None

    root_field = "organization" if is_org else "user"
    while True:
        repo_query = textwrap.dedent(
            f"""
            query($login: String!, $after: String) {{
              {root_field}(login: $login) {{
                repositories(first: {GRAPHQL_PAGE_SIZE}, after: $after, ownerAffiliations: OWNER) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    name
                    refs(refPrefix: \"refs/heads/\", first: {GRAPHQL_PAGE_SIZE}) {{
                      pageInfo {{ hasNextPage endCursor }}
                      nodes {{ name }}
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        data = _github_graphql(repo_query, {"login": username, "after": repo_after}, headers)
        root_obj = data.get("organization" if is_org else "user") or {}
        repos_conn = root_obj.get("repositories") or {}

        for repo_node in repos_conn.get("nodes", []):
            repo_name = repo_node["name"]
            branches: List[str] = [ref["name"] for ref in repo_node["refs"]["nodes"]]

            # Paginate branches if more than GRAPHQL_PAGE_SIZE exist
            br_after = repo_node["refs"]["pageInfo"].get("endCursor")
            br_has_next = repo_node["refs"]["pageInfo"].get("hasNextPage")
            while br_has_next:
                branch_query = textwrap.dedent(
                    """
                    query($owner: String!, $name: String!, $after: String!) {
                      repository(owner: $owner, name: $name) {
                        refs(refPrefix: \"refs/heads/\", first: {GRAPHQL_PAGE_SIZE}, after: $after) {
                          pageInfo { hasNextPage endCursor }
                          nodes { name }
                        }
                      }
                    }
                    """
                )
                br_data = _github_graphql(
                    branch_query,
                    {"owner": username, "name": repo_name, "after": br_after},
                    headers,
                )
                refs = br_data["repository"]["refs"]
                branches.extend([ref["name"] for ref in refs["nodes"]])
                br_after = refs["pageInfo"].get("endCursor")
                br_has_next = refs["pageInfo"].get("hasNextPage")

            result[repo_name] = branches

        if not repos_conn.get("pageInfo", {}).get("hasNextPage"):
            break
        repo_after = repos_conn["pageInfo"].get("endCursor")

    return result


# ---------------------------------------------------------------------------
# GraphQL full-detail helpers ------------------------------------------------
# ---------------------------------------------------------------------------

def _paginate_refs_graphql(owner: str, repo: str, after: str, ref_prefix: str, headers: dict) -> list[dict]:
    """Return additional ref nodes (>GRAPHQL_PAGE_SIZE) for *repo* after *cursor*.

    *ref_prefix* must be either "refs/heads/" or "refs/tags/". Each call
    returns up to GRAPHQL_PAGE_SIZE nodes; we loop internally until the pages are
    exhausted so the caller receives a complete list.
    """
    all_nodes: list[dict] = []
    while after:
        query = textwrap.dedent(
            f"""
            query($owner: String!, $name: String!, $after: String!) {{
              repository(owner: $owner, name: $name) {{
                refs(refPrefix: \"{ref_prefix}\", first: {GRAPHQL_PAGE_SIZE}, after: $after) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    name
                    target {{
                      ... on Commit {{
                        oid
                        committedDate
                        messageHeadline
                        author {{ name email date }}
                        committer {{ name email date }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        data = _github_graphql(query, {"owner": owner, "name": repo, "after": after}, headers)
        refs = data["repository"]["refs"]
        all_nodes.extend(refs["nodes"])
        if refs["pageInfo"]["hasNextPage"]:
            after = refs["pageInfo"]["endCursor"]
        else:
            after = None
    return all_nodes


def fetch_repos_full_graphql(username: str, token: str, include_tags: bool, is_org: bool = False) -> tuple[dict, dict, dict]:
    """Return (repo_map, branches_map, tags_map) with full details via GraphQL.

    *repo_map*   : repo_name → raw repository dictionary (includes first GRAPHQL_PAGE_SIZE refs)
    *branches_map*: repo_name → list of branch dictionaries (each with commit data)
    *tags_map*   : repo_name → list of tag dictionaries
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"bearer {token}",
    }

    repo_after: str | None = None
    repos: dict[str, dict] = {}
    branch_map: dict[str, list[dict]] = {}
    tag_map: dict[str, list[dict]] = {}

    root_field = "organization" if is_org else "user"
    while True:
        repo_query = textwrap.dedent(
            f"""
            query($login: String!, $after: String) {{
              {root_field}(login: $login) {{
                repositories(first: {GRAPHQL_PAGE_SIZE}, after: $after, ownerAffiliations: OWNER) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    name
                    url
                    description
                    isPrivate
                    isFork
                    defaultBranchRef {{ name }}
                    refs(refPrefix: \"refs/heads/\", first: {GRAPHQL_PAGE_SIZE}) {{
                      pageInfo {{ hasNextPage endCursor }}
                      nodes {{
                        name
                        target {{
                          ... on Commit {{
                            oid
                            committedDate
                            messageHeadline
                            author {{ name email date }}
                            committer {{ name email date }}
                          }}
                        }}
                      }}
                    }}
                    tagRefs: refs(refPrefix: \"refs/tags/\", first: {GRAPHQL_PAGE_SIZE}) {{
                      pageInfo {{ hasNextPage endCursor }}
                      nodes {{
                        name
                        target {{
                          ... on Commit {{
                            oid
                            committedDate
                            messageHeadline
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """
        )

        data = _github_graphql(repo_query, {"login": username, "after": repo_after}, headers)
        repos_conn = data[root_field]["repositories"]

        for node in repos_conn.get("nodes", []):
            name: str = node["name"]
            repos[name] = node

            # Normalize branch nodes to REST-like structure
            def _norm_branch(n: dict) -> dict:
                tgt = n.pop("target", {})
                n["commit_full"] = tgt
                n["commit"] = {"sha": tgt.get("oid"), "message": tgt.get("messageHeadline")}
                return n

            branches = [_norm_branch(b) for b in node["refs"]["nodes"]]
            if node["refs"]["pageInfo"]["hasNextPage"]:
                after = node["refs"]["pageInfo"]["endCursor"]
                extra = _paginate_refs_graphql(username, name, after, "refs/heads/", headers)
                branches.extend([_norm_branch(b) for b in extra])
            # REST enrichment removed – GraphQL data already includes necessary commit details
            branch_map[name] = branches

            # Tags (optional)
            if include_tags:
                tags = list(node["tagRefs"]["nodes"])
                if node["tagRefs"]["pageInfo"]["hasNextPage"]:
                    after_t = node["tagRefs"]["pageInfo"]["endCursor"]
                    tags.extend(_paginate_refs_graphql(username, name, after_t, "refs/tags/", headers))
                # Normalize tag commit objects using GraphQL data only
                for tg in tags:
                    if isinstance(tg.get("target"), dict):
                        commit = tg.pop("target")
                        tg["commit_full"] = commit
                        tg["commit"] = {"sha": commit.get("oid"), "message": commit.get("messageHeadline")}
                tag_map[name] = tags

        if not repos_conn.get("pageInfo", {}).get("hasNextPage"):
            break
        repo_after = repos_conn["pageInfo"].get("endCursor")

    return repos, branch_map, tag_map


def _apply_gql_page_size(query: str) -> str:
    """Replace hardcoded 'first: 100' with the configured GRAPHQL_PAGE_SIZE."""
    return query.replace("first: 100", f"first: {GRAPHQL_PAGE_SIZE}")


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
            logger.info("%s: +%d -%d branches updated", repo, len(added), len(removed))
            print(f"{repo}: +{len(added)} -{len(removed)} branches updated")


def persist_repo_json(repo_data: Dict[str, dict], out_dir: str) -> None:
    """Persist each repo's full JSON data to `<out_dir>/<repo>.json`."""
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    for name, payload in repo_data.items():
        file = p / f"{name}.json"
        serialized = json.dumps(payload, indent=2, sort_keys=True)
        new_content = serialized + "\n"
        changed = True
        if file.exists() and file.read_text() == new_content:
            changed = False
        file.write_text(new_content)
        if changed:
            logger.info("%s: JSON updated (size %d bytes)", name, len(new_content))
            print(f"{name}: JSON updated (size {len(new_content)} bytes)")


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
            new_content = serialized + "\n"
            changed = True
            if file.exists() and file.read_text() == new_content:
                changed = False
            file.write_text(new_content)
            if changed:
                logger.info("%s/%s: branch JSON updated", repo, br.get("name"))
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
            new_content = serialized + "\n"
            changed = True
            if file.exists() and file.read_text() == new_content:
                changed = False
            file.write_text(new_content)
            if changed:
                logger.info("%s/%s: tag JSON updated", repo, name)
                print(f"{repo}/{name}: tag JSON updated")


def cli() -> None:
    parser = argparse.ArgumentParser(description="List all GitHub repos and branches for a user or organization.")
    parser.add_argument("login", help="GitHub username or organization to query")
    parser.add_argument("--token", help="GitHub Personal Access Token (or set GITHUB_TOKEN env var)")
    parser.add_argument("--json", action="store_true", help="Output result as JSON")
    parser.add_argument("--graphql", action="store_true", help="Use GitHub GraphQL API (v4) to minimise HTTP requests")
    parser.add_argument("--org", action="store_true", help="Treat <login> as organization rather than user")
    parser.add_argument("--save-dir", help="Directory to persist per-repo branch files (overwrites old state)")
    parser.add_argument("--save-json-dir", help="Directory to persist full repo + branches JSON file")
    parser.add_argument("--save-branch-json-dir", help="Directory to persist each branch JSON individually")
    parser.add_argument("--save-tag-json-dir", help="Directory to persist each tag JSON individually")
    parser.add_argument("--log-dir", default="logs", help="Directory to save timestamped logs")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    args = parser.parse_args()

    global logger
    start_time = time.perf_counter()
    debug_log_dir = Path(args.log_dir)
    debug_log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    log_file = debug_log_dir / f"{Path(sys.argv[0]).stem}_{timestamp}.log"
    logger = logging.getLogger()
    # Capture all levels in root logger so file handler can store DEBUG logs
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # store everything to file
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler prints INFO by default, DEBUG if --verbose specified
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    entity = "organization" if args.org else "user"
    logger.info("Starting query_github run for %s: %s", entity, args.login)

    token = args.token or os.getenv("GITHUB_TOKEN")
    if args.graphql:
        if not token:
            parser.error("--graphql requires --token or GITHUB_TOKEN environment variable")

        # When persisting additional JSON we need full detail.
        need_full = bool(
            args.save_json_dir or args.save_branch_json_dir or args.save_tag_json_dir
        )

        if need_full:
            include_tags = bool(args.save_tag_json_dir or args.save_json_dir)
            repo_map, branch_map, tag_map = fetch_repos_full_graphql(args.login, token, include_tags, is_org=args.org)

            data = {name: [br["name"] for br in branches] for name, branches in branch_map.items()}

            if args.save_dir:
                persist_branches(data, args.save_dir)

            if args.save_json_dir:
                full_map = {
                    name: {
                        "repo": repo_map[name],
                        "branches": branch_map[name],
                        "tags": tag_map.get(name, []),
                    }
                    for name in repo_map
                }
                persist_repo_json(full_map, args.save_json_dir)

            if args.save_branch_json_dir:
                persist_branch_json(branch_map, args.save_branch_json_dir)

            if args.save_tag_json_dir and include_tags:
                persist_tag_json(tag_map, args.save_tag_json_dir)

        else:
            # Simple branch-name listing only
            data = list_repos_branches_graphql(args.login, token, is_org=args.org)

    else:
        data = list_repos_branches(args.login, token, is_org=args.org)

        # REST persistence (branches / full JSON / tags)
        if args.save_dir:
            persist_branches(data, args.save_dir)

        if args.save_json_dir or args.save_branch_json_dir or args.save_tag_json_dir:
            full_map: Dict[str, dict] = {}
            branch_map: Dict[str, List[dict]] = {}
            tag_map: Dict[str, List[dict]] = {}

            headers = {"Accept": "application/vnd.github+json"}
            if token:
                headers["Authorization"] = f"token {token}"

            for repo in fetch_repos(args.login, headers, is_org=args.org):
                name = repo["name"]
                branches_json = fetch_branches_full(args.login, name, headers)
                tags_json = (
                    fetch_tags_full(args.login, name, headers)
                    if args.save_tag_json_dir or args.save_json_dir
                    else []
                )

                if args.save_json_dir:
                    full_map[name] = {
                        "repo": repo,
                        "branches": branches_json,
                        "tags": tags_json,
                    }

                branch_map[name] = branches_json
                tag_map[name] = tags_json

            if args.save_json_dir:
                persist_repo_json(full_map, args.save_json_dir)

            if args.save_branch_json_dir:
                persist_branch_json(branch_map, args.save_branch_json_dir)

            if args.save_tag_json_dir:
                persist_tag_json(tag_map, args.save_tag_json_dir)

    if args.json:
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        for repo, branches in data.items():
            print(repo)
            for br in branches:
                print(f"  └─ {br}")

    logger.info("Total GitHub API calls: %d", API_CALL_COUNT)
    logger.info("Total runtime: %.2f seconds", time.perf_counter() - start_time)


if __name__ == "__main__":
    cli()
