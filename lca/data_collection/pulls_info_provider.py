from datetime import datetime
from typing import Optional

import aiohttp
from lxml import html

from lca.data_collection.github_collection import make_github_http_request
from lca.data_collection.object_info_provider import AdditionalObjectsProvider


class PullsInfoProvider(AdditionalObjectsProvider):
    def __init__(
        self, http_session: aiohttp.ClientSession, github_tokens: list[str], src_data_folder: str, dst_data_folder: str
    ):
        super().__init__(http_session, github_tokens, src_data_folder, dst_data_folder)

    async def _get_linked_issues(self, html_url: str):
        time_start = datetime.now()
        async with self.http_session.get(html_url) as response:
            response.raise_for_status()
            html_content = await response.text()
            time_end = datetime.now()
            # print(f"Time load html: {time_end - time_start}")

            time_start = datetime.now()
            doc = html.fromstring(html_content.encode("utf-8"))
            linked_issues = [e.get("href") for e in doc.xpath('//form[@aria-label="Link issues"]/span/a')]
            time_end = datetime.now()
            # print(f"Time parse html: {time_end - time_start}")

            return linked_issues

    async def _get_commits(self, commits_url: str, github_token: str) -> list[dict] | Exception:
        current_url = f"{commits_url}?per_page=100&state=all"

        commits_data: list[dict] = []
        while current_url is not None:
            print(f"Processing: {current_url}")

            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            commits_data += github_api_response_or_error.data
            current_url = github_api_response_or_error.headers.get("next", None)

        return commits_data

    async def process_items(self, items: list[dict], owner: str, name: str, github_token: str) -> Optional[Exception]:
        for pull_data in items:
            commits_url = pull_data["commits_url"]
            commits_data = await self._get_commits(commits_url, github_token)
            if isinstance(commits_data, Exception):
                return commits_data
            pull_data["commits"] = commits_data

            html_url = pull_data["html_url"]
            commits_data = await self._get_linked_issues(html_url)
            if isinstance(commits_data, Exception):
                return commits_data
            pull_data["linked_issues"] = commits_data

        self.dump_data(owner, name, items)

        return None
