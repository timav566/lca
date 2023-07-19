from collections import defaultdict
from typing import Any, Optional

import aiohttp

from lca.data_collection.github_collection import make_github_http_request, GITHUB_API_URL
from lca.data_collection.repo_info_provider import RepoInfoProvider


class IssueProvider(RepoInfoProvider):

    def __init__(self, http_session: aiohttp.ClientSession, github_tokens: list[str], data_folder: str):
        super().__init__(http_session, github_tokens, data_folder)
        self.repo_to_issues: dict[tuple[str, str], list[Any]] = defaultdict(list)

    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        current_url = f"{GITHUB_API_URL}/repos/{owner}/{name}/issues?per_page=100&state=all"

        while current_url is not None:
            print(f"Processing: {current_url}")

            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            data = github_api_response_or_error.data

            self.repo_to_issues[(owner, name)].append(data)
            self.dump_data(owner, name, data)

            current_url = github_api_response_or_error.headers.get("next", None)

        return None
