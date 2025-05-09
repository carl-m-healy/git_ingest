import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import patch
import query_github

# Common test data
TEST_ORG_NAME = "testorg"
TEST_TOKEN = "test_token"

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
                                {'name': 'main', 'target': {'oid': 'main_oid_org1', 'messageHeadline': 'Initial commit org', 'message': 'Test commit message.'}},
                                {'name': 'dev', 'target': {'oid': 'dev_oid_org1', 'messageHeadline': 'Feature commit org', 'message': 'Test commit message.'}}
                            ],
                            'pageInfo': {'hasNextPage': False, 'endCursor': None}
                        },
                        'tags': { # Tags
                            'nodes': [
                                {'name': 'v1.0', 'target': {'oid': 'v10_oid_org1', 'messageHeadline': 'Release v1.0 org', 'message': 'Test commit message.'}},
                                {'name': 'v1.1', 'target': {'oid': 'v11_oid_org1', 'messageHeadline': 'Release v1.1 org', 'message': 'Test commit message.'}}
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
    assert {'name': 'main', 'target': {'oid': 'main_oid_org1', 'messageHeadline': 'Initial commit org', 'message': 'Test commit message.'}} in branches
    assert {'name': 'dev', 'target': {'oid': 'dev_oid_org1', 'messageHeadline': 'Feature commit org', 'message': 'Test commit message.'}} in branches

    assert 'org-repo1' in tag_map_full
    tags = tag_map_full['org-repo1']
    assert len(tags) == 2
    assert {'name': 'v1.0', 'target': {'oid': 'v10_oid_org1', 'messageHeadline': 'Release v1.0 org', 'message': 'Test commit message.'}} in tags
    assert {'name': 'v1.1', 'target': {'oid': 'v11_oid_org1', 'messageHeadline': 'Release v1.1 org', 'message': 'Test commit message.'}} in tags

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
                                {'name': 'main', 'target': {'oid': 'main_oid_no_tags', 'messageHeadline': 'Initial commit org no tags', 'message': 'Test commit message.'}}
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
    assert {'name': 'main', 'target': {'oid': 'main_oid_no_tags', 'messageHeadline': 'Initial commit org no tags', 'message': 'Test commit message.'}} in branches

    assert not tag_map_full.get('org-repo-no-tags') # Should be empty or repo key not present

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_repos(mock_target_function):
    """Test fetching org's repositories with pagination for the repo list itself."""
    repo_fetch_org_page1 = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-page1',
                        'description': 'Org Paginated Repo 1',
                        'primaryLanguage': {'name': 'JavaScript'},
                        'createdAt': '2023-03-01T00:00:00Z',
                        'updatedAt': '2023-03-01T00:00:00Z',
                        'pushedAt': '2023-03-01T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_p1_org', 'target': {'oid': 'main_oid_p1_org', 'messageHeadline': 'Commit for org pag-repo1', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
                        'tags': {'nodes': [{'name': 'v_p1_org', 'target': {'oid': 'tag_oid_p1_org', 'messageHeadline': 'Tag for org pag-repo1', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}
                    }
                ],
                'pageInfo': {'hasNextPage': True, 'endCursor': 'org_repo_cursor_p2'}
            }
        }
    }
    repo_fetch_org_page2 = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-page2',
                        'description': 'Org Paginated Repo 2',
                        'primaryLanguage': {'name': 'TypeScript'},
                        'createdAt': '2023-03-02T00:00:00Z',
                        'updatedAt': '2023-03-02T00:00:00Z',
                        'pushedAt': '2023-03-02T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_p2_org', 'target': {'oid': 'main_oid_p2_org', 'messageHeadline': 'Commit for org pag-repo2', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}},
                        'tags': {'nodes': [{'name': 'v_p2_org', 'target': {'oid': 'tag_oid_p2_org', 'messageHeadline': 'Tag for org pag-repo2', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    
    api_calls_org_pag_repos = []
    call_count_org_pag_repos = 0

    def side_effect_org_pag_repos(query, variables, headers):
        nonlocal call_count_org_pag_repos
        api_calls_org_pag_repos.append({'query': query, 'variables': variables})
        call_count_org_pag_repos += 1
        if call_count_org_pag_repos == 1: # Initial repo fetch
            return repo_fetch_org_page1
        elif call_count_org_pag_repos == 2: # Second repo fetch (paginated)
            assert variables.get('after') == 'org_repo_cursor_p2'
            return repo_fetch_org_page2
        raise ValueError(f"Unexpected call to _github_graphql: count={call_count_org_pag_repos}, query={query}, variables={variables}")

    mock_target_function.side_effect = side_effect_org_pag_repos

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        TEST_ORG_NAME, TEST_TOKEN, include_tags=True, is_org=True
    )

    assert call_count_org_pag_repos == 2 # 2 repo fetches
    assert "org-repo-page1" in repo_map and repo_map["org-repo-page1"]["description"] == "Org Paginated Repo 1"
    assert "org-repo-page2" in repo_map and repo_map["org-repo-page2"]["description"] == "Org Paginated Repo 2"
    
    # Assertions for org-repo-page1 branches and tags
    assert "org-repo-page1" in branch_map_full
    branch1_data = branch_map_full["org-repo-page1"][0]
    assert branch1_data["name"] == "main_p1_org"
    assert branch1_data["target"]['oid'] == "main_oid_p1_org"
    assert branch1_data["target"]['messageHeadline'] == "Commit for org pag-repo1"
    assert branch1_data["target"]["message"] == "Test commit message."
    
    assert "org-repo-page1" in tag_map_full
    tag1_data = tag_map_full["org-repo-page1"][0]
    assert tag1_data["name"] == "v_p1_org"
    assert tag1_data["target"]['oid'] == "tag_oid_p1_org"
    assert tag1_data["target"]['messageHeadline'] == "Tag for org pag-repo1"
    assert tag1_data["target"]["message"] == "Test commit message."

    # Assertions for org-repo-page2 branches and tags
    assert "org-repo-page2" in branch_map_full
    branch2_data = branch_map_full["org-repo-page2"][0]
    assert branch2_data["name"] == "main_p2_org"
    assert branch2_data["target"]['oid'] == "main_oid_p2_org"
    assert branch2_data["target"]['messageHeadline'] == "Commit for org pag-repo2"
    assert branch2_data["target"]["message"] == "Test commit message."
    
    assert "org-repo-page2" in tag_map_full
    tag2_data = tag_map_full["org-repo-page2"][0]
    assert tag2_data["name"] == "v_p2_org"
    assert tag2_data["target"]['oid'] == "tag_oid_p2_org"
    assert tag2_data["target"]['messageHeadline'] == "Tag for org pag-repo2"
    assert tag2_data["target"]["message"] == "Test commit message."


@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_branches(mock_target_function):
    # Initial repo fetch (single repo, branches paginated, tags inline)
    repo_fetch_org_pag_branches_initial = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-pag-branches',
                        'description': 'Org Repo Paginated Branches',
                        'primaryLanguage': {'name': 'Go'},
                        'createdAt': '2023-04-01T00:00:00Z',
                        'updatedAt': '2023-04-01T00:00:00Z',
                        'pushedAt': '2023-04-01T00:00:00Z',
                        'refs': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'org_branch_cursor_repo1'}}, # Branches are paginated
                        'tags': {'nodes': [{'name': 'v1_org_pb', 'target': {'oid': 'tag_oid_org_pb', 'messageHeadline': 'Tag for org pag branch test', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}} # Tags are inline
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    # Paginated branch fetch for the repo
    branch_fetch_org_repo1_paginated = {
        'repository': {
            'refs': {
                'nodes': [
                    {'name': 'feat/branch1-org', 'target': {'oid': 'b1_oid_org_pb', 'messageHeadline': 'Org branch 1 commit', 'message': 'Test commit message.'}},
                    {'name': 'feat/branch2-org', 'target': {'oid': 'b2_oid_org_pb', 'messageHeadline': 'Org branch 2 commit', 'message': 'Test commit message.'}}
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }

    api_calls_org_pag_branches = []
    call_count_org_pag_branches = 0

    def side_effect_org_pag_branches(query, variables, headers):
        nonlocal call_count_org_pag_branches
        api_calls_org_pag_branches.append({'query': query, 'variables': variables})
        call_count_org_pag_branches += 1
        if call_count_org_pag_branches == 1: # Initial repo fetch
            return repo_fetch_org_pag_branches_initial
        elif call_count_org_pag_branches == 2: # Branch fetch for 'org-repo-pag-branches'
            assert variables.get('owner') == TEST_ORG_NAME
            assert variables.get('repoName') == 'org-repo-pag-branches'
            assert variables.get('after') == 'org_branch_cursor_repo1'
            return branch_fetch_org_repo1_paginated
        raise ValueError(f"Unexpected call to _github_graphql: count={call_count_org_pag_branches}, query={query}, variables={variables}")

    mock_target_function.side_effect = side_effect_org_pag_branches

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        TEST_ORG_NAME, TEST_TOKEN, include_tags=True, is_org=True
    )

    assert call_count_org_pag_branches == 2 # 1 repo fetch, 1 branch fetch
    assert "org-repo-pag-branches" in repo_map

    # Assertions for branches
    assert "org-repo-pag-branches" in branch_map_full
    branches = branch_map_full["org-repo-pag-branches"]
    assert len(branches) == 2
    expected_branches = [
        {'name': 'feat/branch1-org', 'target': {'oid': 'b1_oid_org_pb', 'messageHeadline': 'Org branch 1 commit', 'message': 'Test commit message.'}},
        {'name': 'feat/branch2-org', 'target': {'oid': 'b2_oid_org_pb', 'messageHeadline': 'Org branch 2 commit', 'message': 'Test commit message.'}}
    ]
    for expected_branch in expected_branches:
        assert expected_branch in branches

    # Assertions for tags (should be from initial fetch)
    assert "org-repo-pag-branches" in tag_map_full
    tags = tag_map_full["org-repo-pag-branches"]
    assert len(tags) == 1
    assert {'name': 'v1_org_pb', 'target': {'oid': 'tag_oid_org_pb', 'messageHeadline': 'Tag for org pag branch test', 'message': 'Test commit message.'}} in tags


@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_org_paginated_tags(mock_target_function):
    # Initial repo fetch (single repo, tags paginated, branches inline)
    repo_fetch_org_pag_tags_initial = {
        'organization': {
            'repositories': {
                'nodes': [
                    {
                        'name': 'org-repo-pag-tags',
                        'description': 'Org Repo Paginated Tags',
                        'primaryLanguage': {'name': 'Ruby'},
                        'createdAt': '2023-05-01T00:00:00Z',
                        'updatedAt': '2023-05-01T00:00:00Z',
                        'pushedAt': '2023-05-01T00:00:00Z',
                        'refs': {'nodes': [{'name': 'main_org_pt', 'target': {'oid': 'branch_oid_org_pt', 'messageHeadline': 'Branch for org pag tag test', 'message': 'Test commit message.'}}], 'pageInfo': {'hasNextPage': False, 'endCursor': None}}, # Branches are inline
                        'tags': {'nodes': [], 'pageInfo': {'hasNextPage': True, 'endCursor': 'org_tag_cursor_repo1'}} # Tags are paginated
                    }
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }
    # Paginated tag fetch for the repo
    tag_fetch_org_repo1_paginated = {
        'repository': {
            'tags': {
                'nodes': [
                    {'name': 'v2.0-org', 'target': {'oid': 't1_oid_org_pt', 'messageHeadline': 'Org tag 1 commit', 'message': 'Test commit message.'}},
                    {'name': 'v2.1-org', 'target': {'oid': 't2_oid_org_pt', 'messageHeadline': 'Org tag 2 commit', 'message': 'Test commit message.'}}
                ],
                'pageInfo': {'hasNextPage': False, 'endCursor': None}
            }
        }
    }

    api_calls_org_pag_tags = []
    call_count_org_pag_tags = 0

    def side_effect_org_pag_tags(query, variables, headers):
        nonlocal call_count_org_pag_tags
        api_calls_org_pag_tags.append({'query': query, 'variables': variables})
        call_count_org_pag_tags += 1
        if call_count_org_pag_tags == 1: # Initial repo fetch
            return repo_fetch_org_pag_tags_initial
        elif call_count_org_pag_tags == 2: # Tag fetch for 'org-repo-pag-tags'
            assert variables.get('owner') == TEST_ORG_NAME
            assert variables.get('repoName') == 'org-repo-pag-tags'
            assert variables.get('after') == 'org_tag_cursor_repo1'
            return tag_fetch_org_repo1_paginated
        raise ValueError(f"Unexpected call to _github_graphql: count={call_count_org_pag_tags}, query={query}, variables={variables}")

    mock_target_function.side_effect = side_effect_org_pag_tags

    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        TEST_ORG_NAME, TEST_TOKEN, include_tags=True, is_org=True
    )

    assert call_count_org_pag_tags == 2 # 1 repo fetch, 1 tag fetch
    assert "org-repo-pag-tags" in repo_map

    # Assertions for branches (should be from initial fetch)
    assert "org-repo-pag-tags" in branch_map_full
    branches = branch_map_full["org-repo-pag-tags"]
    assert len(branches) == 1
    assert {'name': 'main_org_pt', 'target': {'oid': 'branch_oid_org_pt', 'messageHeadline': 'Branch for org pag tag test', 'message': 'Test commit message.'}} in branches

    # Assertions for tags
    assert "org-repo-pag-tags" in tag_map_full
    tags = tag_map_full["org-repo-pag-tags"]
    assert len(tags) == 2
    expected_tags = [
        {'name': 'v2.0-org', 'target': {'oid': 't1_oid_org_pt', 'messageHeadline': 'Org tag 1 commit', 'message': 'Test commit message.'}},
        {'name': 'v2.1-org', 'target': {'oid': 't2_oid_org_pt', 'messageHeadline': 'Org tag 2 commit', 'message': 'Test commit message.'}}
    ]
    for expected_tag in expected_tags:
        assert expected_tag in tags
