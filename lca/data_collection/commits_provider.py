from typing import Optional

import aiohttp

from lca.data_collection.additional_info_provider import AdditionalInfoProvider
from lca.data_collection.github_collection import make_github_http_request


class CommitsProvider(AdditionalInfoProvider):
    def __init__(
        self, http_session: aiohttp.ClientSession, github_tokens: list[str], src_data_folder: str, dst_data_folder: str
    ):
        super().__init__(http_session, github_tokens, src_data_folder, dst_data_folder)

    async def process_item(self, item: dict, owner: str, name: str, github_token: str) -> Optional[Exception]:
        commits_url = item["commits_url"]
        current_url = f"{commits_url}?per_page=100&state=all"

        while current_url is not None:
            print(f"Processing: {current_url}")

            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            commits_data = github_api_response_or_error.data

            for commit_data in commits_data:
                commit_sha = commit_data.get("sha")
                commit_diff_url = commit_data.get("url")
                if commit_sha and commit_diff_url:
                    diff_response = await make_github_http_request(self.http_session, github_token, commit_diff_url)
                    commit_data["diff"] = diff_response.data

            self.dump_data(owner, name, commits_data)

            current_url = github_api_response_or_error.headers.get("next", None)

        return None
