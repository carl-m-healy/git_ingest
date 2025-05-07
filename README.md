# git_ingest

A command-line utility to query GitHub repositories, their branches, and tags with support for REST and GraphQL APIs.

## Features

- List all repositories for a **GitHub user _or organisation_** (public and private with authentication)
- Retrieve branch names or full branch details
- Fetch tags and embed full commit metadata
- Optimize HTTP requests with GitHub GraphQL API (v4)
- Persist data to text or JSON files (branches, full repo data, branch JSON, tag JSON)
- Timestamped logging for debugging and audit

## Requirements

- Python 3.8+
- requests >= 2.31.0
- python-dotenv >= 1.0.1
- pytest >= 7.2.2 (for running tests)

## Installation

```bash
git clone https://github.com/carl-m-healy/git_ingest.git
cd git_ingest
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root containing **one line**:
   ```
   GITHUB_TOKEN=<your_personal_access_token>
   ```
   The application automatically loads this file at runtime (no need to export
   the variable). If an environment variable called `GITHUB_TOKEN` is already
   present, the value in `.env` **overrides** it.

2. **Token scopes**
   * Classic PAT – add `repo` (private-repo access) and `read:org` scopes.
   * Fine-grained PAT – grant the token to the target organisation with
     “Repository contents → Read‐only” and “Metadata” permissions.

## Usage

```bash
python query_github.py <login> [OPTIONS]
```

### Positional Arguments

- `<login>`: GitHub username _or_ organisation name to query.

### Options

| Flag                         | Description                                                         |
|------------------------------|---------------------------------------------------------------------|
| `--token TOKEN`              | GitHub Personal Access Token (or use `GITHUB_TOKEN` env var)        |
| `--json`                     | Output result as JSON                                               |
| `--graphql`                  | Use GitHub GraphQL API (v4) to minimize HTTP requests               |
| `--org`                     | Treat `<login>` as an organisation rather than a user               |
| `--save-dir DIR`             | Persist per-repo branch lists to `DIR`                             |
| `--save-json-dir DIR`        | Persist full repo + branches JSON to `DIR`                         |
| `--save-branch-json-dir DIR` | Persist individual branch JSON files to `DIR`                      |
| `--save-tag-json-dir DIR`    | Persist individual tag JSON files to `DIR`                         |
| `--log-dir DIR`              | Directory for logs (default: `logs`)                               |

## Examples

- List public repos and branches:
  ```bash
  python query_github.py octocat
  ```

- Use GraphQL with token and JSON output:
  ```bash
  python query_github.py octocat --graphql --json  # token read from .env
  ```

- Save branch lists:
  ```bash
  python query_github.py octocat --save-dir output/branches
  ```

- Persist full JSON data:
  ```bash
  python query_github.py octocat --save-json-dir output/full_json
  ```

- Query an organisation using GraphQL and persist data:
  ```bash
  python query_github.py carlhealyorg --org --graphql \
      --save-dir repo_branch_states \
      --save-json-dir repo_full_json \
      --save-branch-json-dir branch_json \
      --save-tag-json-dir tag_json
  ```

## Logging

Logs are saved under the specified `--log-dir` (default: `logs/`) with timestamps for each run.

## Testing

Run unit tests with pytest:

```bash
pytest
```

## License

MIT License
