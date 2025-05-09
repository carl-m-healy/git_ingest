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
from typing import Dict, List, Callable, Tuple, Optional, Any
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

GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
API_CALL_COUNT = 0

# Request / proxy resilience settings
REQUEST_TIMEOUT = (5, 30)  # (connect_timeout, read_timeout) in seconds
MAX_RETRIES = 3

# Paging sizes tunable via environment (smaller sizes help stay under strict proxy timeouts)
GRAPHQL_PAGE_SIZE = int(os.getenv("GITHUB_GRAPHQL_PAGE_SIZE", "10"))  # default 50 nodes

# Module-level logger so library functions are usable without invoking the
# `cli()` entry-point (which reconfigures logging). Tests import the module
# directly and call helpers, therefore we need a default logger instance to
# avoid NameError.
logger = logging.getLogger(__name__)
API_CALL_COUNT = 0


# --- GraphQL Helper Functions --- #

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


def _fetch_paginated_gql_data(
    build_query_and_vars_func: Callable[[Optional[str]], Tuple[str, Dict[str, Any]]],
    extract_nodes_and_pageinfo_func: Callable[[Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]],
    headers: Dict[str, str],
    start_cursor: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches all nodes for a paginated GraphQL query.

    Args:
        build_query_and_vars_func: A function that takes an 'after' cursor (or None)
                                   and returns a tuple of (query_string, variables).
        extract_nodes_and_pageinfo_func: A function that takes the GraphQL response data
                                         and returns a tuple of (nodes_list, pageInfo_dict).
        headers: The HTTP headers for the GraphQL request.
        start_cursor: The cursor to start pagination from. Defaults to None.

    Returns:
        A list containing all fetched nodes.
    """
    all_nodes: List[Dict[str, Any]] = []
    after_cursor: Optional[str] = start_cursor
    has_next_page = True

    while has_next_page:
        query, variables = build_query_and_vars_func(after_cursor)
        response_data = _github_graphql(query, variables, headers)
        
        nodes, page_info = extract_nodes_and_pageinfo_func(response_data)
        all_nodes.extend(nodes)
        
        has_next_page = page_info.get("hasNextPage", False)
        after_cursor = page_info.get("endCursor")
        
        if not has_next_page:
            break
            
    return all_nodes


def list_repos_branches_graphql(username: str, token: str, is_org: bool = False) -> Dict[str, List[str]]:
    """Return mapping {repo_name: [branch, ...]} using GitHub GraphQL v4.

    Retrieves repositories in batches and, for each repository, grabs
    branches, paginating when necessary.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"bearer {token}",
    }
    result: Dict[str, List[str]] = {}
    root_field = "organization" if is_org else "user"

    # Define how to build the repository query and extract its data
    def build_repo_query_and_vars(after_cursor: Optional[str]) -> Tuple[str, Dict[str, Any]]:
        query = textwrap.dedent(
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
        return query, {"login": username, "after": after_cursor}

    def extract_repo_nodes_and_pageinfo(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        root_obj = data.get(root_field) or {}
        repos_conn = root_obj.get("repositories") or {}
        return repos_conn.get("nodes", []), repos_conn.get("pageInfo", {})

    # Fetch all repository nodes
    all_repo_nodes = _fetch_paginated_gql_data(
        build_repo_query_and_vars,
        extract_repo_nodes_and_pageinfo,
        headers
    )

    for repo_node in all_repo_nodes:
        repo_name = repo_node["name"]
        initial_branch_refs = repo_node.get("refs", {})
        branches: List[str] = [ref["name"] for ref in initial_branch_refs.get("nodes", [])]
        
        branch_page_info = initial_branch_refs.get("pageInfo", {})
        
        if branch_page_info.get("hasNextPage"):
            # Define how to build the branch query and extract its data for *this* repo
            def build_branch_query_and_vars_for_repo(after_cursor: Optional[str]) -> Tuple[str, Dict[str, Any]]:
                query = textwrap.dedent(
                    f"""
                    query($owner: String!, $name: String!, $after: String!) {{
                      repository(owner: $owner, name: $name) {{
                        refs(refPrefix: \"refs/heads/\", first: {GRAPHQL_PAGE_SIZE}, after: $after) {{
                          pageInfo {{ hasNextPage endCursor }}
                          nodes {{ name }}
                        }}
                      }}
                    }}
                    """
                )
                return query, {"owner": username, "name": repo_name, "after": after_cursor, "refPrefix": "refs/heads/"}

            def extract_branch_nodes_and_pageinfo(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
                refs_conn = data.get("repository", {}).get("refs", {})
                return refs_conn.get("nodes", []), refs_conn.get("pageInfo", {})

            remaining_branch_nodes = _fetch_paginated_gql_data(
                build_branch_query_and_vars_for_repo,
                extract_branch_nodes_and_pageinfo,
                headers,
                start_cursor=branch_page_info.get("endCursor")
            )
            branches.extend([ref["name"] for ref in remaining_branch_nodes])
            
        result[repo_name] = sorted(list(set(branches))) # Ensure unique and sorted branches

    return result


# ---------------------------------------------------------------------------
# GraphQL full-detail helpers ------------------------------------------------
# ---------------------------------------------------------------------------

def fetch_repos_full_graphql(username: str, token: str, include_tags: bool, is_org: bool = False) -> tuple[dict, dict, dict]:
    """Return (repo_map, branches_map, tags_map) with full details via GraphQL.

    *repo_map*   : repo_name → raw repository dictionary
    *branches_map*: repo_name → list of branch dictionaries (each with commit data)
    *tags_map*   : repo_name → list of tag dictionaries
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"bearer {token}",
    }

    repo_map: dict[str, dict] = {}
    branch_map: dict[str, list[dict]] = {}
    tag_map: dict[str, list[dict]] = {}

    root_field = "organization" if is_org else "user"

    # --- Repository Pagination --- 
    def build_repo_query_and_vars(after_cursor: Optional[str]) -> Tuple[str, Dict[str, Any]]:
        tags_fragment = """
            tags: refs(refPrefix: \"refs/tags/\", first: $refsPageSize, orderBy: {field: TAG_COMMIT_DATE, direction: DESC}) {
              pageInfo { hasNextPage endCursor }
              nodes {
                name
                target {
                  ... on Commit {
                    oid
                    committedDate
                    messageHeadline
                    message
                    author { name email date }
                    committer { name email date }
                  }
                  ... on Tag {
                    oid
                    message
                    tagger { name email date }
                    target {
                      ... on Commit {
                        oid
                        committedDate
                        messageHeadline
                        message
                        author { name email date }
                        committer { name email date }
                      }
                    }
                  }
                }
              }
            }
        """ if include_tags else ""

        query = textwrap.dedent(
            f"""
            query($login: String!, $after: String, $refsPageSize: Int!) {{
              {root_field}(login: $login) {{
                repositories(first: {GRAPHQL_PAGE_SIZE}, after: $after, ownerAffiliations: OWNER) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    name
                    nameWithOwner
                    description
                    url
                    sshUrl
                    homepageUrl
                    createdAt
                    updatedAt
                    pushedAt
                    diskUsage
                    forkCount
                    stargazerCount
                    watchers {{ totalCount }}
                    languages(first: 10) {{ totalCount edges {{ size node {{ name color }} }} }}
                    repositoryTopics(first: 10) {{ edges {{ node {{ topic {{ name }} }} }} }}
                    collaborators(first: 10) {{ totalCount edges {{ node {{ login name url }} }} }}
                    visibility
                    licenseInfo {{ name spdxId url }}
                    isPrivate
                    isFork
                    isTemplate
                    hasIssuesEnabled
                    hasWikiEnabled
                    hasProjectsEnabled
                    hasDiscussionsEnabled
                    isMirror
                    mirrorUrl
                    openGraphImageUrl
                    webCommitSignoffRequired
                    autoMergeAllowed
                    deleteBranchOnMerge
                    mergeCommitAllowed
                    rebaseMergeAllowed
                    squashMergeAllowed
                    parent {{ nameWithOwner url }}
                    owner {{ login url __typename }}
                    openIssues: issues(states: OPEN) {{ totalCount }}
                    closedIssues: issues(states: CLOSED) {{ totalCount }}
                    openPRs: pullRequests(states: OPEN) {{ totalCount }}
                    closedPRs: pullRequests(states: CLOSED) {{ totalCount }}
                    mergedPRs: pullRequests(states: MERGED) {{ totalCount }}
                    releases {{ totalCount }}
                    deployments {{ totalCount }}
                    codeOfConduct {{ name key url }}
                    securityPolicyUrl
                    vulnerabilityAlerts {{ totalCount }}
                    defaultBranchRef {{ name }}
                    refs(refPrefix: \"refs/heads/\", first: $refsPageSize, orderBy: {{field: ALPHABETICAL, direction: ASC}}) {{
                      pageInfo {{ hasNextPage endCursor }}
                      nodes {{
                        name
                        prefix
                        associatedPullRequests(first: 5) {{
                          nodes {{
                            title
                            number
                            state
                            merged
                            mergedAt
                            url
                          }}
                        }}
                        target {{
                          ... on Commit {{
                            oid
                            committedDate
                            messageHeadline
                            message
                            author {{ name email date }}
                            committer {{ name email date }}
                            parents(first: 5) {{
                              nodes {{ oid }}
                            }}
                            pushedDate
                            status {{ state }}
                            checkSuites(first: 5) {{
                              nodes {{
                                conclusion
                                status
                                app {{ name }}
                              }}
                            }}
                          }}
                        }}
                        branchProtectionRule {{
                          pattern
                          requiresApprovingReviews
                          requiredApprovingReviewCount
                          dismissesStaleReviews
                          isAdminEnforced
                          requiresCodeOwnerReviews
                          requiresCommitSignatures
                          requiresConversationResolution
                          requiresLinearHistory
                          restrictsPushes
                          restrictsReviewDismissals
                        }}
                      }}
                    }}
                    {tags_fragment}
                  }}
                }}
              }}
            }}
            """
        )
        return query, {"login": username, "after": after_cursor, "refsPageSize": GRAPHQL_PAGE_SIZE}

    def extract_repo_nodes_and_pageinfo(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        root_obj = data.get(root_field) or {}
        repos_conn = root_obj.get("repositories") or {}
        return repos_conn.get("nodes", []), repos_conn.get("pageInfo", {})

    all_repo_nodes = _fetch_paginated_gql_data(
        build_repo_query_and_vars,
        extract_repo_nodes_and_pageinfo,
        headers
    )

    for repo_node in all_repo_nodes:
        repo_name = repo_node["name"]
        repo_map[repo_name] = repo_node

        # --- Branch Pagination for current repo --- 
        initial_branch_refs_conn = repo_node.get("refs", {})
        current_repo_branches: List[Dict[str, Any]] = list(initial_branch_refs_conn.get("nodes", []))
        branch_page_info = initial_branch_refs_conn.get("pageInfo", {})

        if branch_page_info.get("hasNextPage"):
            def build_branch_query_and_vars_for_repo(after_cursor: Optional[str]) -> Tuple[str, Dict[str, Any]]:
                query = textwrap.dedent(
                    f"""
                    query($owner: String!, $repoName: String!, $after: String, $refsPageSize: Int!) {{
                      repository(owner: $owner, name: $repoName) {{
                        refs(refPrefix: \"refs/heads/\", first: $refsPageSize, after: $after, orderBy: {{field: ALPHABETICAL, direction: ASC}}) {{
                          pageInfo {{ hasNextPage endCursor }}
                          nodes {{
                            name
                            prefix
                            associatedPullRequests(first: 5) {{
                              nodes {{
                                title
                                number
                                state
                                merged
                                mergedAt
                                url
                              }}
                            }}
                            target {{
                              ... on Commit {{
                                oid
                                committedDate
                                messageHeadline
                                message
                                author {{ name email date }}
                                committer {{ name email date }}
                              }}
                            }}
                            branchProtectionRule {{
                              pattern
                              requiresApprovingReviews
                              requiredApprovingReviewCount
                              dismissesStaleReviews
                              isAdminEnforced
                              requiresCodeOwnerReviews
                              requiresCommitSignatures
                              requiresConversationResolution
                              requiresLinearHistory
                              restrictsPushes
                              restrictsReviewDismissals
                            }}
                          }}
                        }}
                      }}
                    }}
                    """
                )
                return query, {"owner": username, "repoName": repo_name, "after": after_cursor, "refsPageSize": GRAPHQL_PAGE_SIZE, "refPrefix": "refs/heads/"}

            def extract_branch_nodes_and_pageinfo(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
                refs_conn = data.get("repository", {}).get("refs", {})
                return refs_conn.get("nodes", []), refs_conn.get("pageInfo", {})

            remaining_branch_nodes = _fetch_paginated_gql_data(
                build_branch_query_and_vars_for_repo,
                extract_branch_nodes_and_pageinfo,
                headers,
                start_cursor=branch_page_info.get("endCursor")
            )
            current_repo_branches.extend(remaining_branch_nodes)
        
        branch_map[repo_name] = current_repo_branches

        # --- Tag Pagination for current repo (if requested) --- 
        if include_tags:
            # Define helper functions for tag fetching, in scope for if/elif
            def build_tag_query_and_vars_for_repo(after_cursor: Optional[str]) -> Tuple[str, Dict[str, Any]]:
                query = textwrap.dedent(
                    f"""
                    query($owner: String!, $repoName: String!, $after: String, $refsPageSize: Int!) {{
                      repository(owner: $owner, name: $repoName) {{
                        tags: refs(refPrefix: \"refs/tags/\", first: $refsPageSize, after: $after, orderBy: {{field: TAG_COMMIT_DATE, direction: DESC}}) {{
                          pageInfo {{ hasNextPage endCursor }}
                          nodes {{
                            name
                            target {{
                              ... on Commit {{
                                oid
                                committedDate
                                messageHeadline
                                message
                                author {{ name email date }}
                                committer {{ name email date }}
                              }}
                              ... on Tag {{
                                oid
                                message
                                tagger {{ name email date }}
                                target {{
                                  ... on Commit {{
                                    oid
                                    committedDate
                                    messageHeadline
                                    message
                                    author {{ name email date }}
                                    committer {{ name email date }}
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
                return query, {"owner": username, "repoName": repo_name, "after": after_cursor, "refsPageSize": GRAPHQL_PAGE_SIZE}

            def extract_tag_nodes_and_pageinfo(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
                refs_conn = data.get("repository", {}).get("tags", {}) # Query aliases refs to tags
                return refs_conn.get("nodes", []), refs_conn.get("pageInfo", {})
            
            initial_tag_refs_conn = repo_node.get("tags", {})
            current_repo_tags: List[Dict[str, Any]] = list(initial_tag_refs_conn.get("nodes", []))
            tag_page_info = initial_tag_refs_conn.get("pageInfo", {})

            if tag_page_info.get("hasNextPage"):
                remaining_tag_nodes = _fetch_paginated_gql_data(
                    build_tag_query_and_vars_for_repo,
                    extract_tag_nodes_and_pageinfo,
                    headers,
                    start_cursor=tag_page_info.get("endCursor")
                )
                current_repo_tags.extend(remaining_tag_nodes)
            elif not current_repo_tags: # If no tags inline and not paginated, try a fresh fetch
                first_page_tag_nodes = _fetch_paginated_gql_data(
                    build_tag_query_and_vars_for_repo,
                    extract_tag_nodes_and_pageinfo,
                    headers,
                    start_cursor=None # Start fresh for the first page
                )
                current_repo_tags.extend(first_page_tag_nodes)
            
            tag_map[repo_name] = current_repo_tags

    # Post-process primaryLanguage in repo_map
    for repo_details in repo_map.values():
        lang_dict = repo_details.get('primaryLanguage')
        if isinstance(lang_dict, dict) and 'name' in lang_dict:
            repo_details['primaryLanguage'] = lang_dict['name']
        # If lang_dict is None, it remains None. If it's already a string, it's also fine.

    return repo_map, branch_map, tag_map


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
    # Capture all levels in root logger so file handler can store DEBUG logs
    root_logger = logging.getLogger() # Get the root logger
    root_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # store everything to file
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console handler prints INFO by default, DEBUG if --verbose specified
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Now, calls to query_github.logger (e.g., logger.info) will propagate to the configured root_logger.
    entity = "organization" if args.org else "user"
    logger.info("Starting query_github run for %s: %s", entity, args.login)

    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        parser.error("This tool requires a GitHub token. Please use --token or set GITHUB_TOKEN environment variable.")

    need_full_details = bool(args.save_json_dir or args.save_branch_json_dir or args.save_tag_json_dir)
    # Fetch tags if any tag-specific save is requested, or if the combined JSON (which includes tags) is requested.
    include_tags_for_fetch = bool(args.save_tag_json_dir or (args.save_json_dir and need_full_details))

    repo_map: Dict[str, Dict] = {}
    branch_map_full: Dict[str, List[Dict]] = {} # For detailed branch objects
    tag_map_full: Dict[str, List[Dict]] = {}    # For detailed tag objects
    repo_branch_list_map: Dict[str, List[str]] = {} # For {repo_name: [branch_names]}

    if need_full_details:
        repo_map, branch_map_full, tag_map_full = fetch_repos_full_graphql(
            args.login, token, include_tags_for_fetch, is_org=args.org
        )
        # Derive the simple branch list map from the full branch data
        if branch_map_full: # Ensure it's not empty
            repo_branch_list_map = {
                name: sorted(list(set(br_node["name"] for br_node in branches if "name" in br_node)))
                for name, branches in branch_map_full.items() if branches
            }
    else:
        repo_branch_list_map = list_repos_branches_graphql(args.login, token, is_org=args.org)

    # --- Handle empty results after fetching --- 
    if not repo_branch_list_map and not (args.json and need_full_details): # If JSON output and full details, empty JSON is fine.
                                                                       # Otherwise, if simple JSON or text and no repos, it's an empty result.
        logger.warning("No repositories found for %s: %s", entity, args.login)
        if not args.json: # Only print to stderr if not outputting JSON, to avoid mixed output.
            print(f"No repositories found for {entity}: {args.login}", file=sys.stderr)
    
    # --- Persist data to files --- 
    if args.save_dir:
        persist_branches(repo_branch_list_map, args.save_dir)

    if need_full_details: # These persistence options require the full data fetch
        if args.save_json_dir:
            if repo_map: # Only proceed if repo_map has data
                full_map = {
                    name: {
                        "repo": repo_data,
                        "branches": branch_map_full.get(name, []),
                        "tags": tag_map_full.get(name, []) if include_tags_for_fetch else [],
                    }
                    for name, repo_data in repo_map.items()
                }
                persist_repo_json(full_map, args.save_json_dir)
            else:
                logger.info("Skipping combined JSON persistence (--save-json-dir) as no repository data was fetched.")

        if args.save_branch_json_dir:
            if branch_map_full:
                persist_branch_json(branch_map_full, args.save_branch_json_dir)
            else:
                logger.info("Skipping branch JSON persistence (--save-branch-json-dir) as no branch data was fetched.")

        if args.save_tag_json_dir and include_tags_for_fetch:
            if tag_map_full:
                persist_tag_json(tag_map_full, args.save_tag_json_dir)
            else:
                logger.info("Skipping tag JSON persistence (--save-tag-json-dir) as no tag data was fetched.")
    
    # --- Console Output --- 
    # Use repo_branch_list_map for console output, as it's always populated for this purpose.
    if args.json:
        # If full details were requested for JSON output, use that structure, else use simple list map.
        # However, the primary JSON output for --json should consistently be the simple branch list for now,
        # as changing its structure based on other --save flags could be confusing.
        # If a user wants the full JSON, they should use --save-json-dir and inspect those files.
        json.dump(repo_branch_list_map, sys.stdout, indent=2)
        sys.stdout.write("\n")
    elif repo_branch_list_map: # Only print if not JSON and if there's data
        for repo, branches in repo_branch_list_map.items():
            print(repo)
            for br in branches:
                print(f"  └─ {br}")

    logger.info("Total GitHub API calls: %d", API_CALL_COUNT)
    logger.info("Total runtime: %.2f seconds", time.perf_counter() - start_time)


if __name__ == "__main__":
    cli()
