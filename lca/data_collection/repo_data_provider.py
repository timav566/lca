import asyncio
import json
import os
from typing import Optional

import aiohttp

from lca.data_collection.github_collection import GITHUB_API_URL, make_github_http_request


class RepoObjectsProvider:
    def __init__(self, http_session: aiohttp.ClientSession, github_tokens: list[str], data_folder: str, data: str):
        self.http_session = http_session
        self.github_tokens = github_tokens
        self.data_folder = data_folder
        self.data = data

        os.makedirs(self.data_folder, exist_ok=True)

    async def process_repositories(self, repositories: list[tuple]):
        prepare_repositories_coroutines = []
        for i, (owner, name) in enumerate(repositories):
            token = self.github_tokens[i % len(self.github_tokens)]
            prepare_repositories_coroutines.append(
                self.process_repo(
                    github_token=token,
                    owner=owner,
                    name=name,
                )
            )

        for repositories_future in asyncio.as_completed(prepare_repositories_coroutines):
            await repositories_future

    def dump_data(self, owner: str, name: str, items: list[dict]):
        data_path = os.path.join(self.data_folder, f"{owner}__{name}.jsonl")
        with open(data_path, "a") as f_data_output:
            for item in items:
                f_data_output.write(json.dumps(item) + "\n")

    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        current_url = f"{GITHUB_API_URL}/repos/{owner}/{name}/{self.data}?per_page=100&state=all"

        while current_url is not None:
            print(f"Processing: {current_url}")

            github_api_response_or_error = await make_github_http_request(self.http_session, github_token, current_url)

            if isinstance(github_api_response_or_error, Exception):
                return github_api_response_or_error

            data = github_api_response_or_error.data

            self.dump_data(owner, name, data)

            current_url = github_api_response_or_error.headers.get("next", None)

        return None
