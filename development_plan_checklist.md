# GitHub Ingestion Tool – Development Checklist

Use this checklist to track implementation progress. Mark each task with `[x]` when completed.

## Phase 1 – Default to GraphQL
- [x] Remove `--graphql` flag from CLI (argparse setup & help text).
- [x] Update CLI flow to always invoke GraphQL functions.
- [x] Enforce requirement for `GITHUB_TOKEN` / `--token`; emit error if missing.
- [x] Eliminate all `args.graphql` conditionals throughout the codebase.
- [x] Update unit tests that assumed REST behaviour to expect GraphQL output.
- [x] Run full test-suite and ensure all tests pass.

## Phase 2 – Delete REST Implementation
- [x] Delete REST helper `_github_get` and constants (e.g. `API_BASE`, `GITHUB_REST_PAGE_SIZE`).
- [x] Remove REST functions: `fetch_repos`, `fetch_branches`, `fetch_branches_full`, `fetch_tags_full`.
- [x] Delete or rewrite tests referencing the removed functions.
- [x] Purge unused imports / variables left from REST path.
- [x] Run test-suite; confirm green.

## Phase 3 – Simplify GraphQL Logic & Outputs
- [x] Decide on unified GraphQL function design (keep separate, refactor common logic).
- [x] Refactor common logic from `list_repos_branches_graphql` and `fetch_repos_full_graphql` into helper functions.
- [x] Consolidate file-output logic: handle `--save-dir`, `--json`, etc., in one section.
- [x] Fix bug: always call `persist_branches` when `--save-dir` is provided.
- [x] Remove dead helpers such as `_apply_gql_page_size`.
- [x] Ensure consistent logger usage across modules.
- [x] Update / add unit tests for the refactored flow.
- [x] Run tests.

## Phase 4 – Optimise GraphQL Query Fields
- [x] Add commit `message` / `messageBody` to commit fragment.
- [x] Extend tag query to follow annotated tags (`... on Tag { target { ... on Commit { ... } } }`).
- [x] Trim non-essential repository fields (e.g. full collaborator edges, topic details).
- [x] Update data-normalisation code to store new commit fields.
- [x] Adjust tests to validate new JSON shape.
- [x] Run tests.

## Phase 5 – Tune Pagination Defaults
- [ ] Increase default `GITHUB_GRAPHQL_PAGE_SIZE` to **50**.
- [ ] Update documentation/comments for new default & env override behaviour.
- [ ] Simulate large-org dataset; verify pagination logic and call count reduction.
- [ ] Add/adjust unit tests checking page-size handling.

## Phase 6 – Implement Advanced Batching (Optional)
- [ ] Detect repositories still requiring branch pagination after first fetch.
- [ ] Build dynamic GraphQL query with aliases to batch 5-10 repos per request.
- [ ] Merge paginated branch data back into in-memory structures.
- [ ] Write unit tests to confirm batching works and reduces API calls.

## Phase 7 – Update Documentation & CLI Help
- [ ] Remove REST references and `--graphql` option from README.
- [ ] Document token requirement and `GITHUB_GRAPHQL_PAGE_SIZE` env var.
- [ ] Update usage examples to reflect GraphQL-only mode.
- [ ] Regenerate CLI `-h` output to verify flags/help are correct.

## Phase 8 – Final Integration & Performance Validation
- [ ] Run tool against large real/test organisation.
- [ ] Verify only GraphQL (v4) endpoints are hit; check API call count.
- [ ] Confirm each query completes < 10 s and overall runtime acceptable.
- [ ] Spot-check output (branch lists, JSON files) against GitHub UI or previous runs.
- [ ] Record any regressions or performance issues; iterate if needed.
