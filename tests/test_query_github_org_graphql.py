import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch
import query_github

# Organization Test Cases

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_simple(mock_target_function):
    """Test fetching org's repositories with branches and tags (no pagination)."""
    org_repo_fetch_simple = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo1',
                        'description': 'Org Repo 1 Simple',
                        'url': 'https://github.com/org/org-repo1',
                        'stargazerCount': 10,
                        'forkCount': 5,
                        'isFork': False,
                        'isPrivate': False,
                        'primaryLanguage': {'name': 'Python'},
                        'createdAt': '2023-01-01T00:00:00Z',
                        'updatedAt': '2023-01-01T00:00:00Z',
                        'pushedAt': '2023-01-01T00:00:00Z',
                        'refs': { # Branches
                            'nodes': [
                                {'name': 'main', 'target': {'oid': 'main_oid_org1'}},
                                {'name': 'dev', 'target': {'oid': 'dev_oid_org1'}}
                            ],
                            'pageInfo': {'hasNextPage': False, 'endCursor': None}
                        },
                        'tags': { # Tags
                            'nodes': [
                                {'name': 'v1.0', 'target': {'oid': 'v10_oid_org1'}},
                                {'name': 'v1.1', 'target': {'oid': 'v11_oid_org1'}}
                            ],
                            'pageInfo': {'hasNextPage': False, 'endCursor': None}
                        }
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    api_calls_org_simple = []
    call_count_org_simple = 0

    def mock_gql_dispatch_org_simple(query, variables, headers):
        nonlocal call_count_org_simple
        api_calls_org_simple.append(variables.copy())
        call_count_org_simple += 1
        query_github.API_CALL_COUNT += 1
        # This test expects only one call for the repo list
        if variables.get("login") == "testorg_simple" and variables.get("after") is None:
            return org_repo_fetch_simple
        raise ValueError(f"Unexpected GQL call for org_simple test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_org_simple
    query_github.API_CALL_COUNT = 0

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username='testorg_simple', token='fake_token', include_tags=True, is_org=True
    )

    assert query_github.API_CALL_COUNT == 1
    assert call_count_org_simple == 1
    assert len(api_calls_org_simple) == 1
    assert api_calls_org_simple[0].get("login") == "testorg_simple"
    assert api_calls_org_simple[0].get("after") is None

    assert len(repo_map) == 1
    assert 'org-repo1' in repo_map
    repo_details = repo_map['org-repo1']
    assert repo_details['name'] == 'org-repo1'
    assert repo_details['description'] == 'Org Repo 1 Simple'
    assert repo_details['url'] == 'https://github.com/org/org-repo1'
    assert repo_details['stargazerCount'] == 10
    assert not repo_details['isFork']
    assert repo_details['primaryLanguage'] == 'Python' # Adjusted key based on processing

    assert 'org-repo1' in branch_map_full
    branches = branch_map_full['org-repo1']
    assert len(branches) == 2
    assert {'name': 'main', 'target': {'oid': 'main_oid_org1'}} in branches
    assert {'name': 'dev', 'target': {'oid': 'dev_oid_org1'}} in branches

    assert 'org-repo1' in tag_map_full
    tags = tag_map_full['org-repo1']
    assert len(tags) == 2
    assert {'name': 'v1.0', 'target': {'oid': 'v10_oid_org1'}} in tags
    assert {'name': 'v1.1', 'target': {'oid': 'v11_oid_org1'}} in tags

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_no_tags(mock_target_function):
    """Test fetching org's repository without tags."""
    org_repo_fetch_no_tags_data = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-no-tags',
                        'description': 'Org Repo No Tags',
                        'url': 'https://github.com/org/org-repo-no-tags',
                        'stargazerCount': 5,
                        'forkCount': 2,
                        'isFork': True,
                        'isPrivate': True,
                        'primaryLanguage': {'name': 'JavaScript'},
                        'createdAt': '2023-02-01T00:00:00Z',
                        'updatedAt': '2023-02-01T00:00:00Z',
                        'pushedAt': '2023-02-01T00:00:00Z',
                        'refs': { # Branches
                            'nodes': [
                                {'name': 'main', 'target': {'oid': 'main_oid_no_tags'}}
                            ],
                            'pageInfo': {'hasNextPage': False, 'endCursor': None}
                        }
                        # No 'tags' field as include_tags=False for the main query
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    api_calls_org_no_tags = []
    call_count_org_no_tags = 0

    def mock_gql_dispatch_org_no_tags(query, variables, headers):
        nonlocal call_count_org_no_tags
        api_calls_org_no_tags.append(variables.copy())
        call_count_org_no_tags += 1
        query_github.API_CALL_COUNT += 1
        if variables.get("login") == "testorg_no_tags" and variables.get("after") is None:
            # Verify that the query string does not ask for tags
            assert "tags(first: $refsPageSize, refPrefix: \"refs/tags/\")" not in query
            return org_repo_fetch_no_tags_data
        raise ValueError(f"Unexpected GQL call for org_no_tags test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_org_no_tags
    query_github.API_CALL_COUNT = 0

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username='testorg_no_tags', token='fake_token', include_tags=False, is_org=True
    )

    assert query_github.API_CALL_COUNT == 1
    assert call_count_org_no_tags == 1
    assert len(api_calls_org_no_tags) == 1
    assert api_calls_org_no_tags[0].get("login") == "testorg_no_tags"

    assert len(repo_map) == 1
    assert 'org-repo-no-tags' in repo_map
    repo_details = repo_map['org-repo-no-tags']
    assert repo_details['name'] == 'org-repo-no-tags'
    assert repo_details['description'] == 'Org Repo No Tags'
    assert repo_details['isFork']
    assert repo_details['isPrivate']

    assert 'org-repo-no-tags' in branch_map_full
    branches = branch_map_full['org-repo-no-tags']
    assert len(branches) == 1
    assert {'name': 'main', 'target': {'oid': 'main_oid_no_tags'}} in branches

    assert not tag_map_full.get('org-repo-no-tags') # Should be empty or repo key not present

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_repos(mock_target_function):
    """Test fetching org's repositories with pagination for the repo list itself."""
    org_repo_fetch_page1_data = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-page1',
                        'description': 'Org Paginated Repo 1',
                        'url': 'https://github.com/org/org-repo-page1',
                        'stargazerCount': 1,
                        'forkCount': 1,
                        'isFork': False,
                        'isPrivate': False,
                        'primaryLanguage': None,
                        'createdAt': '2023-03-01T00:00:00Z',
                        'updatedAt': '2023-03-01T00:00:00Z',
                        'pushedAt': '2023-03-01T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_p1_org'}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
                        'tags': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'org_tag_cursor_p1_repo1'}}
                    }
                ],
                'pageInfo': {'hasNextPage': True, 'endCursor': 'org_repo_cursor_p2'}
            }
        }
    }
    org_repo_fetch_page2_data = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-page2',
                        'description': 'Org Paginated Repo 2',
                        'url': 'https://github.com/org/org-repo-page2',
                        'stargazerCount': 2,
                        'forkCount': 2,
                        'isFork': False,
                        'isPrivate': False,
                        'primaryLanguage': None,
                        'createdAt': '2023-03-02T00:00:00Z',
                        'updatedAt': '2023-03-02T00:00:00Z',
                        'pushedAt': '2023-03-02T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_p2_org'}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
                        'tags': {'nodes': [], 'pageInfo': {'hasNextPage': False, 'endCursor': None}} # Empty inline, triggers fresh fetch
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    # No separate branch calls needed for org-repo-page1 or org-repo-page2 as inline refs.hasNextPage=False
    tag_fetch_org_repo_p1_data = {'repository': {'tags': {'nodes': [{'name': 'v_p1_org'}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}}} # For org-repo-page1, called with after="org_tag_cursor_p1_repo1"
    tag_fetch_org_repo_p2_data = {'repository': {'tags': {'nodes': [{'name': 'v_p2_org'}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}}} # For org-repo-page2, called with after=None (fresh fetch)
    
    api_calls_org_pag_repos = []
    call_count_org_pag_repos = 0

    def mock_gql_dispatch_org_pag_repos(query, variables, headers):
        nonlocal call_count_org_pag_repos
        api_calls_org_pag_repos.append(variables.copy()); call_count_org_pag_repos += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")
        
        if login_var == "testorg_pag_repos" and not repo_name_var: # Repo list calls
            if not after_cursor_var: return org_repo_fetch_page1_data
            elif after_cursor_var == "org_repo_cursor_p2": return org_repo_fetch_page2_data
        elif repo_name_var == "org-repo-page1":
            # No branch call expected
            if ref_prefix_var is None and after_cursor_var == "org_tag_cursor_p1_repo1": return tag_fetch_org_repo_p1_data
        elif repo_name_var == "org-repo-page2":
            # No branch call expected
            if ref_prefix_var is None and not after_cursor_var: return tag_fetch_org_repo_p2_data # Fresh tag fetch
        raise ValueError(f"Unexpected GQL call for org_paginated_repos test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_org_pag_repos
    query_github.API_CALL_COUNT = 0

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username='testorg_pag_repos', token='fake_token', include_tags=True, is_org=True
    )

    # Expected calls: repo_p1, repo_p2, THEN tags for repo_p1, THEN tags for repo_p2 = 4 calls
    assert query_github.API_CALL_COUNT == 4; assert call_count_org_pag_repos == 4
    assert len(api_calls_org_pag_repos) == 4
    # Call 0: org repo page 1
    assert api_calls_org_pag_repos[0].get("login") == "testorg_pag_repos" and api_calls_org_pag_repos[0].get("after") is None
    # Call 1: org repo page 2
    assert api_calls_org_pag_repos[1].get("login") == "testorg_pag_repos" and api_calls_org_pag_repos[1].get("after") == "org_repo_cursor_p2"
    # Call 2: tag_p1 (paginated for org-repo-page1)
    assert api_calls_org_pag_repos[2].get("repoName") == "org-repo-page1"
    assert api_calls_org_pag_repos[2].get("refPrefix") is None 
    assert api_calls_org_pag_repos[2].get("after") == "org_tag_cursor_p1_repo1"
    # Call 3: tag_p2 (fresh fetch for org-repo-page2)
    assert api_calls_org_pag_repos[3].get("repoName") == "org-repo-page2"
    assert api_calls_org_pag_repos[3].get("refPrefix") is None
    assert api_calls_org_pag_repos[3].get("after") is None

    assert len(repo_map) == 2
    assert "org-repo-page1" in repo_map and repo_map["org-repo-page1"]["description"] == "Org Paginated Repo 1"
    assert "org-repo-page2" in repo_map and repo_map["org-repo-page2"]["description"] == "Org Paginated Repo 2"
    assert "org-repo-page1" in branch_map_full and branch_map_full["org-repo-page1"][0]["name"] == "main_p1_org"
    assert "org-repo-page1" in tag_map_full and tag_map_full["org-repo-page1"][0]["name"] == "v_p1_org"
    assert "org-repo-page2" in branch_map_full and branch_map_full["org-repo-page2"][0]["name"] == "main_p2_org"
    assert "org-repo-page2" in tag_map_full and tag_map_full["org-repo-page2"][0]["name"] == "v_p2_org"


@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_branches(mock_target_function):
    """Test fetching org's repository with paginated branches."""
    org_repo_fetch_pag_branches_initial_data = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-pag-branches',
                        'description': 'Org Repo Paginated Branches',
                        'url': 'https://github.com/org/org-repo-pag-branches',
                        'stargazerCount': 30,
                        'forkCount': 3,
                        'isFork': False,
                        'isPrivate': False,
                        'primaryLanguage': {'name': 'Go'},
                        'createdAt': '2023-04-01T00:00:00Z',
                        'updatedAt': '2023-04-01T00:00:00Z',
                        'pushedAt': '2023-04-01T00:00:00Z',
                        'refs': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'org_branch_cursor_repo1'}},
                        'tags': {'nodes': [{'name': 'v1_org_pb', 'target': {'oid': 'tag_oid_org_pb'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    paginated_branch_fetch_org_repo_data = {
        'repository': {
            'refs': {
                'nodes': [
                    {'name': 'feat/branch1-org', 'target': {'oid': 'b1_oid_org_pb'}},
                    {'name': 'feat/branch2-org', 'target': {'oid': 'b2_oid_org_pb'}}
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    api_calls_org_pag_branches = []
    call_count_org_pag_branches = 0

    def mock_gql_dispatch_org_pag_branches(query, variables, headers):
        nonlocal call_count_org_pag_branches
        api_calls_org_pag_branches.append(variables.copy()); call_count_org_pag_branches += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")

        if login_var == "testorg_pag_branches" and not repo_name_var and not after_cursor_var:
            return org_repo_fetch_pag_branches_initial_data
        elif repo_name_var == "org-repo-pag-branches" and ref_prefix_var == "refs/heads/" and after_cursor_var == "org_branch_cursor_repo1":
            return paginated_branch_fetch_org_repo_data
        raise ValueError(f"Unexpected GQL call for org_paginated_branches test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_org_pag_branches
    query_github.API_CALL_COUNT = 0

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username='testorg_pag_branches', token='fake_token', include_tags=True, is_org=True
    )

    assert query_github.API_CALL_COUNT == 2; assert call_count_org_pag_branches == 2
    assert len(api_calls_org_pag_branches) == 2
    # Call 0: Org repo list
    assert api_calls_org_pag_branches[0].get("login") == "testorg_pag_branches" and api_calls_org_pag_branches[0].get("after") is None
    # Call 1: Paginated branches for 'org-repo-pag-branches'
    assert api_calls_org_pag_branches[1].get("repoName") == "org-repo-pag-branches"
    assert api_calls_org_pag_branches[1].get("refPrefix") == "refs/heads/"
    assert api_calls_org_pag_branches[1].get("after") == "org_branch_cursor_repo1"
    
    assert len(repo_map) == 1
    assert "org-repo-pag-branches" in repo_map
    assert repo_map["org-repo-pag-branches"]["primaryLanguage"] == "Go"

    assert "org-repo-pag-branches" in branch_map_full
    branches = branch_map_full["org-repo-pag-branches"]
    assert len(branches) == 2
    assert {'name': 'feat/branch1-org', 'target': {'oid': 'b1_oid_org_pb'}} in branches
    assert {'name': 'feat/branch2-org', 'target': {'oid': 'b2_oid_org_pb'}} in branches

    assert "org-repo-pag-branches" in tag_map_full
    tags = tag_map_full["org-repo-pag-branches"]
    assert len(tags) == 1
    assert {'name': 'v1_org_pb', 'target': {'oid': 'tag_oid_org_pb'}} in tags


@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_tags(mock_target_function):
    """Test fetching org's repository with paginated tags."""
    org_repo_fetch_pag_tags_initial_data = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-pag-tags',
                        'description': 'Org Repo Paginated Tags',
                        'url': 'https://github.com/org/org-repo-pag-tags',
                        'stargazerCount': 40,
                        'forkCount': 4,
                        'isFork': True,
                        'isPrivate': True,
                        'primaryLanguage': {'name': 'Ruby'},
                        'createdAt': '2023-05-01T00:00:00Z',
                        'updatedAt': '2023-05-01T00:00:00Z',
                        'pushedAt': '2023-05-01T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_org_pt', 'target': {'oid': 'branch_oid_org_pt'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
                        'tags': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'org_tag_cursor_repo1'}}
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    paginated_tag_fetch_org_repo_data = {
        'repository': {
            'tags': {
                'nodes': [
                    {'name': 'v2.0-org', 'target': {'oid': 't1_oid_org_pt'}},
                    {'name': 'v2.1-org', 'target': {'oid': 't2_oid_org_pt'}}
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    api_calls_org_pag_tags = []
    call_count_org_pag_tags = 0

    def mock_gql_dispatch_org_pag_tags(query, variables, headers):
        nonlocal call_count_org_pag_tags
        api_calls_org_pag_tags.append(variables.copy()); call_count_org_pag_tags += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")

        if login_var == "testorg_pag_tags" and not repo_name_var and not after_cursor_var:
            return org_repo_fetch_pag_tags_initial_data
        elif repo_name_var == "org-repo-pag-tags" and ref_prefix_var is None and after_cursor_var == "org_tag_cursor_repo1": # refPrefix is None for tags
            return paginated_tag_fetch_org_repo_data
        raise ValueError(f"Unexpected GQL call for org_paginated_tags test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_org_pag_tags
    query_github.API_CALL_COUNT = 0

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username='testorg_pag_tags', token='fake_token', include_tags=True, is_org=True
    )

    assert query_github.API_CALL_COUNT == 2; assert call_count_org_pag_tags == 2
    assert len(api_calls_org_pag_tags) == 2
    # Call 0: Org repo list
    assert api_calls_org_pag_tags[0].get("login") == "testorg_pag_tags" and api_calls_org_pag_tags[0].get("after") is None
    # Call 1: Paginated tags for 'org-repo-pag-tags'
    assert api_calls_org_pag_tags[1].get("repoName") == "org-repo-pag-tags"
    assert api_calls_org_pag_tags[1].get("refPrefix") is None # Tags
    assert api_calls_org_pag_tags[1].get("after") == "org_tag_cursor_repo1"

    assert len(repo_map) == 1
    assert "org-repo-pag-tags" in repo_map
    assert repo_map["org-repo-pag-tags"]["primaryLanguage"] == "Ruby"

    assert "org-repo-pag-tags" in branch_map_full
    branches = branch_map_full["org-repo-pag-tags"]
    assert len(branches) == 1
    assert {'name': 'main_org_pt', 'target': {'oid': 'branch_oid_org_pt'}} in branches

    assert "org-repo-pag-tags" in tag_map_full
    tags = tag_map_full["org-repo-pag-tags"]
    assert len(tags) == 2
    assert {'name': 'v2.0-org', 'target': {'oid': 't1_oid_org_pt'}} in tags
    assert {'name': 'v2.1-org', 'target': {'oid': 't2_oid_org_pt'}} in tags
