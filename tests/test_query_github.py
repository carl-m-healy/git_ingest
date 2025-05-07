import json
import sys
from pathlib import Path
import pytest
import query_github

def test_sanitize():
    assert query_github._sanitize("a/b/c") == "a__b__c"

def test_list_repos_branches_graphql(monkeypatch):
    data = {
        "user": {
            "repositories": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {
                        "name": "repo1",
                        "refs": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{"name": "main"}, {"name": "dev"}],
                        },
                    },
                    {
                        "name": "repo2",
                        "refs": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{"name": "master"}],
                        },
                    },
                ],
            }
        }
    }
    calls = []
    def fake_gql(query, variables, headers):
        calls.append((variables["login"], headers["Authorization"].split()[1]))
        return data
    monkeypatch.setattr(query_github, "_github_graphql", fake_gql)
    result = query_github.list_repos_branches_graphql("user", "token123")
    assert result == {"repo1": ["main", "dev"], "repo2": ["master"]}
    assert calls == [("user", "token123")]

def test_list_repos_branches(monkeypatch, capsys):
    # empty repositories
    monkeypatch.setattr(query_github, "fetch_repos", lambda u, h, *a: [])
    result = query_github.list_repos_branches("user", None)
    assert result == {}
    captured = capsys.readouterr()
    assert "No repositories found for user" in captured.out

    # skip on error
    repos = [{"name": "repo1"}]
    monkeypatch.setattr(query_github, "fetch_repos", lambda u, h, *a: repos)
    def fake_fetch(owner, name, headers):
        raise SystemExit("error")
    monkeypatch.setattr(query_github, "fetch_branches", fake_fetch)
    result2 = query_github.list_repos_branches("user", None)
    assert result2 == {}
    err = capsys.readouterr()
    assert "[warn] Skipping repo1" in err.err

def test_persist_branches(tmp_path, capsys):
    repo_map = {"repo1": ["b", "a"], "repo2": ["master"]}
    out_dir = tmp_path / "branches"
    query_github.persist_branches(repo_map, str(out_dir))
    assert (out_dir / "repo1.txt").read_text().splitlines() == ["a", "b"]
    assert (out_dir / "repo2.txt").read_text().splitlines() == ["master"]
    # no change
    capsys.readouterr()
    query_github.persist_branches(repo_map, str(out_dir))
    assert capsys.readouterr().out == ""

def test_persist_repo_json(tmp_path, capsys):
    payload = {"b": 2, "a": 1}
    repo_data = {"repo1": payload}
    out_dir = tmp_path / "json"
    query_github.persist_repo_json(repo_data, str(out_dir))
    data = json.loads((out_dir / "repo1.json").read_text())
    assert data == {"a": 1, "b": 2}
    captured = capsys.readouterr()
    assert "repo1: JSON updated" in captured.out
    # no change
    query_github.persist_repo_json(repo_data, str(out_dir))
    assert capsys.readouterr().out == ""

def test_persist_branch_json(tmp_path, capsys):
    branch_map = {"repo1": [{"name": "b1", "val": 1}], "repo2": [{"name": "b2", "val": 2}]}
    out_dir = tmp_path / "branch_json"
    query_github.persist_branch_json(branch_map, str(out_dir))
    data1 = json.loads((out_dir / "repo1" / "b1.json").read_text())
    assert data1 == {"name": "b1", "val": 1}
    captured = capsys.readouterr()
    assert "repo1/b1: branch JSON updated" in captured.out
    # no change
    query_github.persist_branch_json(branch_map, str(out_dir))
    assert capsys.readouterr().out == ""

def test_persist_tag_json(tmp_path, capsys):
    tag_map = {"repo1": [{"name": "t1", "val": 1}], "repo2": [{"name": "t2", "val": 2}]}
    out_dir = tmp_path / "tag_json"
    query_github.persist_tag_json(tag_map, str(out_dir))
    data1 = json.loads((out_dir / "repo1" / "t1.json").read_text())
    assert data1 == {"name": "t1", "val": 1}
    captured = capsys.readouterr()
    assert "repo1/t1: tag JSON updated" in captured.out

# ---------------------------------------------------------------------------
# New tests for organisation support ---------------------------------------
# ---------------------------------------------------------------------------

def test_list_repos_branches_graphql_org(monkeypatch):
    """GraphQL branch listing for an organisation."""
    data = {
        "organization": {
            "repositories": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {
                        "name": "repo1",
                        "refs": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{"name": "main"}, {"name": "dev"}],
                        },
                    },
                    {
                        "name": "repo2",
                        "refs": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": [{"name": "master"}],
                        },
                    },
                ],
            }
        }
    }

    calls: list[str] = []

    def fake_gql(query, variables, headers):
        # Ensure we are querying the expected organisation login.
        calls.append(variables["login"])
        return data

    monkeypatch.setattr(query_github, "_github_graphql", fake_gql)

    result = query_github.list_repos_branches_graphql("my-org", "tok", is_org=True)

    assert result == {"repo1": ["main", "dev"], "repo2": ["master"]}
    assert calls == ["my-org"]

def test_list_repos_branches_org(monkeypatch):
    """REST branch listing for an organisation."""

    repos = [{"name": "repo1"}]
    fetch_calls: list[bool] = []

    def fake_fetch_repos(login, headers, is_org=False):
        # Capture whether organisation flag is passed through.
        fetch_calls.append(is_org)
        return repos

    monkeypatch.setattr(query_github, "fetch_repos", fake_fetch_repos)

    monkeypatch.setattr(query_github, "fetch_branches", lambda o, n, h: ["main", "dev"])

    result = query_github.list_repos_branches("my-org", None, is_org=True)

    assert result == {"repo1": ["main", "dev"]}
    # Ensure we indicated organisation mode to fetch_repos
    assert fetch_calls == [True]
