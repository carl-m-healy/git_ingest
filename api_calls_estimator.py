#!/usr/bin/env python3
"""api_calls_estimator.py

Estimate the number of GitHub API calls required by *query_github.py* to
persist repository + branch information for a large organisation.

The script uses simple arithmetic based on how the tool currently
interacts with the GitHub REST and GraphQL APIs.

Scenarios modelled
------------------
1. REST v3 – _branch-name only_ mode (the default `list_repos_branches`).
2. REST v3 – _full branch JSON_ mode (when `--save-json-dir` or
   `--save-branch-json-dir` is supplied).
3. GraphQL v4 – branches via `list_repos_branches_graphql` (no per-branch
   pagination when ≤100 branches).

Adjust the constants at the top of the file to explore other org sizes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

NUM_REPOS: int = 4  # total repositories in the account/org
AVG_BRANCHES: int = 3  # average branches per repository (≤100 so one page)
AVG_TAGS: int | None = None  # uncomment / set if you want to include tags

PAGE_SIZE = 100  # hard-coded in query_github.py for both REST & GraphQL


@dataclass
class Estimate:
    method: str
    description: str
    calls: int

    def __str__(self) -> str:  # pretty print
        return f"{self.method:26} | {self.calls:8,} calls | {self.description}"


def ceildiv(a: int, b: int) -> int:
    return (a + b - 1) // b


def rest_branch_names(num_repos: int, branches_per_repo: int) -> int:
    """REST v3 when we only fetch branch names via `/branches`."""
    repo_pages = ceildiv(num_repos, PAGE_SIZE)  # `/user/repos?per_page=100`
    branch_calls = num_repos * ceildiv(branches_per_repo, PAGE_SIZE)
    return repo_pages + branch_calls


def rest_branch_full(num_repos: int, branches_per_repo: int) -> int:
    """REST v3 when `fetch_branches_full` is used.

    For each repo:
      • 1 call to list branches (summary)
      • 1 call per branch for details, plus 1 per branch for commit detail
    """
    repo_pages = ceildiv(num_repos, PAGE_SIZE)
    per_repo = 1 + 2 * branches_per_repo  # list + (detail + commit) × N
    return repo_pages + num_repos * per_repo


def graphql_branches(num_repos: int, branches_per_repo: int) -> int:
    """GraphQL v4 call count using list_repos_branches_graphql.

    • Repos are fetched 100 per page → ceil(num_repos/100) calls.
    • Each repo embeds up to 100 branch refs, so no extra calls when
      branches ≤ 100. If branches >100 we would make extra calls per repo,
      but that scenario is not modelled here.
    """
    repo_pages = ceildiv(num_repos, PAGE_SIZE)
    extra_branch_calls = 0
    if branches_per_repo > PAGE_SIZE:
        # number of extra pages needed per repo × repos
        extra_pages = ceildiv(branches_per_repo, PAGE_SIZE) - 1
        extra_branch_calls = num_repos * extra_pages
    return repo_pages + extra_branch_calls


if __name__ == "__main__":
    scenarios = [
        Estimate(
            "REST (names only)",
            "`/branches` summary only",
            rest_branch_names(NUM_REPOS, AVG_BRANCHES),
        ),
        Estimate(
            "REST (full branches)",
            "`fetch_branches_full` incl. commit details",
            rest_branch_full(NUM_REPOS, AVG_BRANCHES),
        ),
        Estimate(
            "GraphQL (branches)",
            "`list_repos_branches_graphql`",
            graphql_branches(NUM_REPOS, AVG_BRANCHES),
        ),
    ]

    print("API CALL ESTIMATES (", NUM_REPOS, "repos,", AVG_BRANCHES, "branches each)")
    print("Method                     |    Calls | Notes")
    print("-" * 60)
    for est in scenarios:
        print(est)

    print("\nAssumptions:")
    print(" • Tags omitted (set AVG_TAGS to include).")
    print(" • No branch has >100 refs so GraphQL needs no per-repo pagination.")
    print(" • Personal Access Token used so GraphQL accessible.")
