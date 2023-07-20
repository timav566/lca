import abc
import asyncio
import json
import os
from typing import Optional

import aiohttp


class RepoInfoProvider(abc.ABC):
    def __init__(self, http_session: aiohttp.ClientSession, github_tokens: list[str], data_folder: str):
        self.http_session = http_session
        self.github_tokens = github_tokens
        self.data_folder = data_folder

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

    @abc.abstractmethod
    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        pass
