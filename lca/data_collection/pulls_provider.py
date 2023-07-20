from collections import defaultdict
from datetime import datetime
from typing import Optional, Any

import aiohttp
from lxml import html

from lca.data_collection.github_collection import GITHUB_API_URL, make_github_http_request
from lca.data_collection.repo_info_provider import RepoInfoProvider


class PullsProvider(RepoInfoProvider):

    def __init__(self, http_session: aiohttp.ClientSession, github_tokens: list[str], data_folder: str):
        super().__init__(http_session, github_tokens, data_folder)
        self.repo_to_pulls: dict[tuple[str, str], list[Any]] = defaultdict(list)

    async def _get_linked_issues(self, html_url: str):
        time_start = datetime.now()
        async with self.http_session.get(html_url) as response:
            response.raise_for_status()
            html_content = await response.text()
            time_end = datetime.now()
            # print(f"Time load html: {time_end - time_start}")

            time_start = datetime.now()
            doc = html.fromstring(html_content.encode('utf-8'))
            linked_issues = [e.get('href') for e in doc.xpath('//form[@aria-label="Link issues"]/span/a')]
            time_end = datetime.now()
            # print(f"Time parse html: {time_end - time_start}")

            return linked_issues

    async def _get_commits(self, commits_url: str, github_token: str):
        current_url = f"{commits_url}?per_page=100&state=all"

        commits_data = []
        while current_url is not None:
            print(f"Processing: {current_url}")

            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            commits_data += github_api_response_or_error.data
            current_url = github_api_response_or_error.headers.get("next", None)

            # for commit_data in commits_data:
            #     commit_sha = commit_data.get("sha")
            #     commit_diff_url = commit_data.get("url")
            #     if commit_sha and commit_diff_url:
            #         diff_response = await make_github_http_request(self.http_session, github_token, commit_diff_url)
            #         commit_data['diff'] = diff_response.data

        return commits_data

    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        current_url = f"{GITHUB_API_URL}/repos/{owner}/{name}/pulls?per_page=100&state=all"

        while current_url is not None:
            print(f"Processing: {current_url}")

            time_start = datetime.now()
            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            pulls_data = github_api_response_or_error.data
            time_end = datetime.now()
            # print(f"Time git: {time_end - time_start}")

            time_start = datetime.now()
            for pull_data in pulls_data:
                html_url = pull_data["html_url"]
                pull_data["linked_issues"] = await self._get_linked_issues(html_url)

                commit_url = pull_data["commits_url"]
                pull_data["commits"] = await self._get_commits(commit_url, github_token)

            time_end = datetime.now()
            # print(f"Time html: {time_end - time_start}")

            self.repo_to_pulls[(owner, name)].append(pulls_data)
            self.dump_data(owner, name, pulls_data)

            current_url = github_api_response_or_error.headers.get("next", None)

        return None
