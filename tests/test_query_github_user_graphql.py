import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import query_github
from unittest.mock import patch

# Test cases for fetch_repos_full_graphql (User)

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_simple(mock_target_function):
    """Test fetch_repos_full_graphql for a user with 1 repo, branches, tags, no pagination."""
    user_repo_fetch_simple = {"user": {"repositories": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "repo1_user", "description": "Test Repo 1 User", "url": "", "stargazerCount": 1, "forkCount": 1, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", "refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "main_user"}, {"name": "dev_user"}]}, "tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "v1.0_user"}, {"name": "v1.1_user"}]}}]}}}
    api_calls_user_simple = []
    call_count_user_simple = 0

    def mock_gql_dispatch_user_simple(query, variables, headers):
        nonlocal call_count_user_simple
        api_calls_user_simple.append(variables.copy()); call_count_user_simple += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")
        
        if login_var == "test_user_simple" and not repo_name_var and not after_cursor_var: 
            return user_repo_fetch_simple
        # No other calls are expected
        raise ValueError(f"Unexpected GQL call for user_simple test: {variables}")
    
    mock_target_function.side_effect = mock_gql_dispatch_user_simple 
    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username="test_user_simple", token="token_simple_user", include_tags=True, is_org=False
    )
    assert query_github.API_CALL_COUNT == 1 # Only the main repo fetch
    assert call_count_user_simple == 1
    assert len(api_calls_user_simple) == 1
    assert api_calls_user_simple[0].get("login") == "test_user_simple"
    
    assert len(repo_map) == 1
    assert "repo1_user" in repo_map and repo_map["repo1_user"]["description"] == "Test Repo 1 User"
    assert "repo1_user" in branch_map_full and sorted([b["name"] for b in branch_map_full["repo1_user"]]) == sorted(["main_user", "dev_user"])
    assert "repo1_user" in tag_map_full and sorted([t["name"] for t in tag_map_full["repo1_user"]]) == sorted(["v1.0_user", "v1.1_user"])

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_no_tags(mock_target_function):
    repo_fetch_response_user_no_tags = {
        "user": {
            "repositories": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {
                        "name": "repo_no_tag_test_user", "description": "Test repo, no tags fetched for user",
                        "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", 
                        "pushedAt": "2023-01-01T00:00:00Z", "stargazerCount": 0, "forkCount": 0, "isFork": False, "isPrivate": False, "url": "",
                        "refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "main_no_tags_user"}]},
                        # No 'tags' key here as include_tags will be False, query won't ask for it
                    }
                ]
            }
        }
    }
    api_calls_user_no_tags = []
    call_count_user_no_tags = 0
    
    def mock_gql_dispatch_user_no_tags(query, variables, headers):
        nonlocal call_count_user_no_tags
        api_calls_user_no_tags.append(variables.copy()); call_count_user_no_tags += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); 
        ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")

        if login_var == "user_no_tags_test" and not repo_name_var and not after_cursor_var:
             return repo_fetch_response_user_no_tags
        # No other calls expected
        raise ValueError(f"Unexpected GQL call for user_no_tags test: {variables}")
    
    mock_target_function.side_effect = mock_gql_dispatch_user_no_tags
    
    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(
        username="user_no_tags_test", token="token_no_tags_user", include_tags=False, is_org=False
    )
    assert query_github.API_CALL_COUNT == 1 # Only the main repo fetch
    assert call_count_user_no_tags == 1
    assert len(api_calls_user_no_tags) == 1
    assert api_calls_user_no_tags[0].get("login") == "user_no_tags_test"
    assert len(repo_map) == 1
    assert "repo_no_tag_test_user" in repo_map
    assert "repo_no_tag_test_user" in branch_map_full and branch_map_full["repo_no_tag_test_user"][0]["name"] == "main_no_tags_user"
    assert not tag_map_full # tag_map_full should be empty

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_paginated_repos(mock_target_function):
    user_repo_fetch_page1 = {"user": {"repositories": {"pageInfo": {"hasNextPage": True, "endCursor": "user_repo_cursor_p2"}, "nodes": [{"name": "user_repo_p1", "description": "User Paginated Repo 1", "url": "", "stargazerCount": 1, "forkCount": 1, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", "refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "main_p1"}]}, "tags": {"pageInfo": {"hasNextPage": True, "endCursor": "user_tag_cursor_p1"}, "nodes": []}}]}}}
    user_repo_fetch_page2 = {"user": {"repositories": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_repo_p2", "description": "User Paginated Repo 2", "url": "", "stargazerCount": 2, "forkCount": 2, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", "refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "main_p2"}]}, "tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": []}}]}}}
    # No separate branch calls needed for user_repo_p1 or user_repo_p2 as inline refs.hasNextPage=False
    tag_fetch_repo_p1 = {"repository": {"tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "v_p1"}]}}} # For user_repo_p1, called with after="user_tag_cursor_p1"
    tag_fetch_repo_p2 = {"repository": {"tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "v_p2"}]}}} # For user_repo_p2, called with after=None (fresh fetch)
    api_calls_user_pag_repos = []
    call_count_user_pag_repos = 0
    
    def mock_gql_dispatch_user_pag_repos(query, variables, headers):
        nonlocal call_count_user_pag_repos
        api_calls_user_pag_repos.append(variables.copy()); call_count_user_pag_repos += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")
        if login_var == "user_paginated_repos" and not repo_name_var: # Repo list calls
            if not after_cursor_var: return user_repo_fetch_page1
            elif after_cursor_var == "user_repo_cursor_p2": return user_repo_fetch_page2
        elif repo_name_var == "user_repo_p1":
            # No branch call expected for user_repo_p1 as inline refs.hasNextPage=False
            if ref_prefix_var is None and after_cursor_var == "user_tag_cursor_p1": return tag_fetch_repo_p1
        elif repo_name_var == "user_repo_p2":
            # No branch call expected for user_repo_p2 as inline refs.hasNextPage=False
            if ref_prefix_var is None and not after_cursor_var: return tag_fetch_repo_p2 # Fresh tag fetch due to empty inline nodes and hasNextPage=False
        raise ValueError(f"Unexpected GQL call for user_paginated_repos test: {variables}")
    
    mock_target_function.side_effect = mock_gql_dispatch_user_pag_repos
    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(username="user_paginated_repos", token="token_pag_repos", include_tags=True, is_org=False)
    # Expected calls: repo_p1, repo_p2, tag_p1(paginated), tag_p2(fresh_fetch) = 4 calls
    assert query_github.API_CALL_COUNT == 4; assert call_count_user_pag_repos == 4
    assert len(api_calls_user_pag_repos) == 4
    # Call 0: repo page 1
    assert api_calls_user_pag_repos[0].get("login") == "user_paginated_repos" and api_calls_user_pag_repos[0].get("after") is None
    # Call 1: repo page 2
    assert api_calls_user_pag_repos[1].get("login") == "user_paginated_repos" and api_calls_user_pag_repos[1].get("after") == "user_repo_cursor_p2"
    # Call 2: tag_p1 (paginated for user_repo_p1 from first repo page)
    assert api_calls_user_pag_repos[2].get("repoName") == "user_repo_p1"
    assert api_calls_user_pag_repos[2].get("refPrefix") is None 
    assert api_calls_user_pag_repos[2].get("after") == "user_tag_cursor_p1"
    # Call 3: tag_p2 (fresh fetch for user_repo_p2 from second repo page)
    assert api_calls_user_pag_repos[3].get("repoName") == "user_repo_p2"
    assert api_calls_user_pag_repos[3].get("refPrefix") is None
    assert api_calls_user_pag_repos[3].get("after") is None

    assert len(repo_map) == 2
    assert "user_repo_p1" in repo_map and repo_map["user_repo_p1"]["description"] == "User Paginated Repo 1"
    assert "user_repo_p2" in repo_map and repo_map["user_repo_p2"]["description"] == "User Paginated Repo 2"
    assert "user_repo_p1" in branch_map_full and branch_map_full["user_repo_p1"][0]["name"] == "main_p1"
    assert "user_repo_p1" in tag_map_full and tag_map_full["user_repo_p1"][0]["name"] == "v_p1"
    assert "user_repo_p2" in branch_map_full and branch_map_full["user_repo_p2"][0]["name"] == "main_p2"
    assert "user_repo_p2" in tag_map_full and tag_map_full["user_repo_p2"][0]["name"] == "v_p2"

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_paginated_branches(mock_target_function):
    # user_repo_fetch_single_many_b: inline tags hasNextPage=False, paginated branches hasNextPage=True, endCursor="user_branch_cursor_inline_end"
    user_repo_fetch_single_many_b = {"user": {"repositories": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "repo_with_many_branches_user", "description": "User Repo with paginated branches", "url": "", "stargazerCount": 0, "forkCount": 0, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", "refs": {"pageInfo": {"hasNextPage": True, "endCursor": "user_branch_cursor_inline_end"}, "nodes": [{"name": "main_for_user_branches"}]}, "tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_tag_for_branches_repo"}]}}]}}}
    user_branch_fetch_page1 = {"repository": {"refs": {"pageInfo": {"hasNextPage": True, "endCursor": "user_branch_cursor_page1_end"}, "nodes": [{"name": "user_branch_X"}]}}} # Fetched with after="user_branch_cursor_inline_end"
    user_branch_fetch_page2 = {"repository": {"refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_branch_Y"}]}}} # Fetched with after="user_branch_cursor_page1_end"
    # No separate tag call needed for repo_with_many_branches_user as inline tags.hasNextPage=False
    api_calls_user_pag_branches = []
    call_count_user_pag_branches = 0
    
    def mock_gql_dispatch_user_pag_branches(query, variables, headers):
        nonlocal call_count_user_pag_branches
        api_calls_user_pag_branches.append(variables.copy()); call_count_user_pag_branches += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")
        if login_var == "user_many_branches_test" and not repo_name_var: return user_repo_fetch_single_many_b
        elif repo_name_var == "repo_with_many_branches_user":
            if ref_prefix_var == "refs/heads/":
                if after_cursor_var == "user_branch_cursor_inline_end": return user_branch_fetch_page1
                elif after_cursor_var == "user_branch_cursor_page1_end": return user_branch_fetch_page2
            # No tag call expected
        raise ValueError(f"Unexpected GQL call for user_paginated_branches test: {variables}")
    
    mock_target_function.side_effect = mock_gql_dispatch_user_pag_branches
    
    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(username="user_many_branches_test", token="token_user_many_branches", include_tags=True, is_org=False)
    # Expected calls: initial_repo, branch_call_p1, branch_call_p2 = 3 calls
    assert query_github.API_CALL_COUNT == 3; assert call_count_user_pag_branches == 3
    assert len(api_calls_user_pag_branches) == 3
    # Call 0: initial_repo
    assert api_calls_user_pag_branches[0].get("login") == "user_many_branches_test"
    # Call 1: First paginated branch call
    assert api_calls_user_pag_branches[1].get("repoName") == "repo_with_many_branches_user"
    assert api_calls_user_pag_branches[1].get("refPrefix") == "refs/heads/"
    assert api_calls_user_pag_branches[1].get("after") == "user_branch_cursor_inline_end"
    # Call 2: Second paginated branch call
    assert api_calls_user_pag_branches[2].get("repoName") == "repo_with_many_branches_user"
    assert api_calls_user_pag_branches[2].get("refPrefix") == "refs/heads/"
    assert api_calls_user_pag_branches[2].get("after") == "user_branch_cursor_page1_end"

    assert len(repo_map) == 1
    assert "repo_with_many_branches_user" in repo_map
    assert "repo_with_many_branches_user" in branch_map_full and len(branch_map_full["repo_with_many_branches_user"]) == 3 # initial + p1 + p2
    branch_names = sorted([b["name"] for b in branch_map_full["repo_with_many_branches_user"]])
    assert branch_names == sorted(["main_for_user_branches", "user_branch_X", "user_branch_Y"])
    assert "repo_with_many_branches_user" in tag_map_full and len(tag_map_full["repo_with_many_branches_user"]) == 1
    assert tag_map_full["repo_with_many_branches_user"][0]["name"] == "user_tag_for_branches_repo"

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_paginated_tags(mock_target_function):
    # Mock data definitions
    # user_repo_fetch_single_many_t: inline branches hasNextPage=False, inline tags hasNextPage=True, endCursor="user_tag_cursor_inline_end"
    user_repo_fetch_single_many_t = {"user": {"repositories": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "repo_with_many_tags_user", "description": "User Repo with paginated tags", "url": "", "stargazerCount": 0, "forkCount": 0, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", "refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "main_for_user_tags"}]}, "tags": {"pageInfo": {"hasNextPage": True, "endCursor": "user_tag_cursor_inline_end"}, "nodes": [{"name": "user_tag_initial"}]}}]}}}
    # No separate branch call needed for repo_with_many_tags_user
    user_tag_fetch_page1 = {"repository": {"tags": {"pageInfo": {"hasNextPage": True, "endCursor": "user_tag_cursor_page1_end"}, "nodes": [{"name": "user_tag_X", "target": {"oid": "tX_user"}}]}}} # Fetched with after="user_tag_cursor_inline_end"
    user_tag_fetch_page2 = {"repository": {"tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_tag_Y", "target": {"oid": "tY_user"}}]}}} # Fetched with after="user_tag_cursor_page1_end"
    api_calls_user_pag_tags = []
    call_count_user_pag_tags = 0
    
    def mock_gql_dispatch_user_pag_tags(query, variables, headers):
        nonlocal call_count_user_pag_tags
        api_calls_user_pag_tags.append(variables.copy()); call_count_user_pag_tags += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")
        if login_var == "user_many_tags_test" and not repo_name_var: return user_repo_fetch_single_many_t
        elif repo_name_var == "repo_with_many_tags_user":
            # No branch call expected as inline refs.hasNextPage=False
            if ref_prefix_var is None:
                if after_cursor_var == "user_tag_cursor_inline_end": return user_tag_fetch_page1
                elif after_cursor_var == "user_tag_cursor_page1_end": return user_tag_fetch_page2
        raise ValueError(f"Unexpected GQL call for user_paginated_tags test: {variables}")
    
    mock_target_function.side_effect = mock_gql_dispatch_user_pag_tags
    
    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(username="user_many_tags_test", token="token_user_many_tags", include_tags=True, is_org=False)
    # Expected calls: initial_repo, tag_call_p1, tag_call_p2 = 3 calls
    assert query_github.API_CALL_COUNT == 3; assert call_count_user_pag_tags == 3
    assert len(api_calls_user_pag_tags) == 3
    # Call 0: initial_repo
    assert api_calls_user_pag_tags[0].get("login") == "user_many_tags_test"
    # Call 1: First paginated tag call
    assert api_calls_user_pag_tags[1].get("repoName") == "repo_with_many_tags_user"
    assert api_calls_user_pag_tags[1].get("refPrefix") is None
    assert api_calls_user_pag_tags[1].get("after") == "user_tag_cursor_inline_end"
    # Call 2: Second paginated tag call
    assert api_calls_user_pag_tags[2].get("repoName") == "repo_with_many_tags_user"
    assert api_calls_user_pag_tags[2].get("refPrefix") is None
    assert api_calls_user_pag_tags[2].get("after") == "user_tag_cursor_page1_end"

    assert len(repo_map) == 1
    assert "repo_with_many_tags_user" in repo_map
    assert "repo_with_many_tags_user" in branch_map_full and len(branch_map_full["repo_with_many_tags_user"]) == 1 and branch_map_full["repo_with_many_tags_user"][0]["name"] == "main_for_user_tags"
    assert "repo_with_many_tags_user" in tag_map_full and len(tag_map_full["repo_with_many_tags_user"]) == 3 # initial + p1 + p2
    tag_names = sorted([t["name"] for t in tag_map_full["repo_with_many_tags_user"]])
    assert tag_names == sorted(["user_tag_initial", "user_tag_X", "user_tag_Y"])

@patch('query_github._github_graphql')
def test_fetch_repos_full_graphql_user_paginated_branches_and_tags(mock_target_function):
    # user_repo_fetch_single_pag_b_t: paginated branches (inline + 2 pages), paginated tags (inline + 2 pages)
    user_repo_fetch_single_pag_b_t = {"user": {"repositories": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "repo_user_pag_b_t", "description": "User Repo paginated B & T", "url": "", "stargazerCount": 0, "forkCount": 0, "isFork": False, "isPrivate": False, "primaryLanguage": None, "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-01T00:00:00Z", "pushedAt": "2023-01-01T00:00:00Z", 
            "refs": {"pageInfo": {"hasNextPage": True, "endCursor": "user_b_t_branch_cursor_inline_end"}, "nodes": [{"name": "user_b_t_branch_main"}]}, 
            "tags": {"pageInfo": {"hasNextPage": True, "endCursor": "user_b_t_tag_cursor_inline_end"}, "nodes": [{"name": "user_b_t_tag_v1"}]}}]}}}
        
    user_b_t_branch_fetch_p1 = {"repository": {"refs": {"pageInfo": {"hasNextPage": True, "endCursor": "user_b_t_branch_cursor_page1_end"}, "nodes": [{"name": "user_b_t_branch_dev"}]}}} # after="user_b_t_branch_cursor_inline_end"
    user_b_t_branch_fetch_p2 = {"repository": {"refs": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_b_t_branch_feat"}]}}} # after="user_b_t_branch_cursor_page1_end"
        
    user_b_t_tag_fetch_p1 = {"repository": {"tags": {"pageInfo": {"hasNextPage": True, "endCursor": "user_b_t_tag_cursor_page1_end"}, "nodes": [{"name": "user_b_t_tag_v2"}]}}} # after="user_b_t_tag_cursor_inline_end"
    user_b_t_tag_fetch_p2 = {"repository": {"tags": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "user_b_t_tag_v3"}]}}} # after="user_b_t_tag_cursor_page1_end"
        
    api_calls_user_pag_b_t = []
    call_count_user_pag_b_t = 0

    def mock_gql_dispatch_user_pag_b_t(query, variables, headers):
        nonlocal call_count_user_pag_b_t
        api_calls_user_pag_b_t.append(variables.copy()); call_count_user_pag_b_t += 1; query_github.API_CALL_COUNT += 1
        login_var = variables.get("login"); repo_name_var = variables.get("repoName"); ref_prefix_var = variables.get("refPrefix"); after_cursor_var = variables.get("after")

        if login_var == "user_pag_b_t_test" and not repo_name_var: return user_repo_fetch_single_pag_b_t
        elif repo_name_var == "repo_user_pag_b_t":
            if ref_prefix_var == "refs/heads/": # Branch calls
                if after_cursor_var == "user_b_t_branch_cursor_inline_end": return user_b_t_branch_fetch_p1
                elif after_cursor_var == "user_b_t_branch_cursor_page1_end": return user_b_t_branch_fetch_p2
            elif ref_prefix_var is None: # Tag calls
                if after_cursor_var == "user_b_t_tag_cursor_inline_end": return user_b_t_tag_fetch_p1
                elif after_cursor_var == "user_b_t_tag_cursor_page1_end": return user_b_t_tag_fetch_p2
        raise ValueError(f"Unexpected GQL call for user_paginated_branches_and_tags test: {variables}")

    mock_target_function.side_effect = mock_gql_dispatch_user_pag_b_t

    query_github.API_CALL_COUNT = 0
    repo_map, branch_map_full, tag_map_full = query_github.fetch_repos_full_graphql(username="user_pag_b_t_test", token="token_user_pag_b_t", include_tags=True, is_org=False)
    # Expected calls: initial_repo, branch_p1, branch_p2, tag_p1, tag_p2 = 5 calls
    assert query_github.API_CALL_COUNT == 5; assert call_count_user_pag_b_t == 5
    assert len(api_calls_user_pag_b_t) == 5
    # Call 0: initial_repo
    assert api_calls_user_pag_b_t[0].get("login") == "user_pag_b_t_test"
    # Call 1: Branch page 1
    assert api_calls_user_pag_b_t[1].get("repoName") == "repo_user_pag_b_t"
    assert api_calls_user_pag_b_t[1].get("refPrefix") == "refs/heads/"
    assert api_calls_user_pag_b_t[1].get("after") == "user_b_t_branch_cursor_inline_end"
    # Call 2: Branch page 2
    assert api_calls_user_pag_b_t[2].get("repoName") == "repo_user_pag_b_t"
    assert api_calls_user_pag_b_t[2].get("refPrefix") == "refs/heads/"
    assert api_calls_user_pag_b_t[2].get("after") == "user_b_t_branch_cursor_page1_end"
    # Call 3: Tag page 1
    assert api_calls_user_pag_b_t[3].get("repoName") == "repo_user_pag_b_t"
    assert api_calls_user_pag_b_t[3].get("refPrefix") is None
    assert api_calls_user_pag_b_t[3].get("after") == "user_b_t_tag_cursor_inline_end"
    # Call 4: Tag page 2
    assert api_calls_user_pag_b_t[4].get("repoName") == "repo_user_pag_b_t"
    assert api_calls_user_pag_b_t[4].get("refPrefix") is None
    assert api_calls_user_pag_b_t[4].get("after") == "user_b_t_tag_cursor_page1_end"

    assert len(repo_map) == 1
    assert "repo_user_pag_b_t" in repo_map
    assert "repo_user_pag_b_t" in branch_map_full and len(branch_map_full["repo_user_pag_b_t"]) == 3
    branch_names = sorted([b["name"] for b in branch_map_full["repo_user_pag_b_t"]])
    assert branch_names == sorted(["user_b_t_branch_main", "user_b_t_branch_dev", "user_b_t_branch_feat"])
    assert "repo_user_pag_b_t" in tag_map_full and len(tag_map_full["repo_user_pag_b_t"]) == 3
    tag_names = sorted([t["name"] for t in tag_map_full["repo_user_pag_b_t"]])
    assert tag_names == sorted(["user_b_t_tag_v1", "user_b_t_tag_v2", "user_b_t_tag_v3"])

# === Organization Test Cases ===