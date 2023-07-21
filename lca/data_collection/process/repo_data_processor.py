import abc
import asyncio
import json
import os
from typing import Optional

import aiohttp


class RepoDataProcessor(abc.ABC):
    def __init__(
            self, http_session: aiohttp.ClientSession, github_tokens: list[str], src_data_folder: str,
            dst_data_folder: str
    ):
        self.http_session = http_session
        self.github_tokens = github_tokens
        self.src_data_folder = src_data_folder
        self.dst_data_folder = dst_data_folder

        os.makedirs(self.dst_data_folder, exist_ok=True)

    @abc.abstractmethod
    async def process_items(self, items: list[dict], owner: str, name: str, github_token: str) -> Optional[Exception]:
        pass

    async def process_repositories_in_batches(self, repositories: list[tuple], batch_size: int = 10):
        for i in range(0, len(repositories), batch_size):
            await self.process_repositories(repositories[i:i + batch_size])

    async def process_repositories(self, repositories: list[tuple]):
        prepare_repositories_coroutines = []
        for i, (owner, name) in enumerate(repositories):
            token = self.github_tokens[i % len(self.github_tokens)]
            file_path = os.path.join(self.src_data_folder, f"{owner}__{name}.jsonl")
            print(f"Processing: {file_path}")

            if os.path.isfile(file_path):
                with open(file_path, "r") as f:
                    items = [json.loads(line) for line in f]
                    prepare_repositories_coroutines.append(
                        self.process_items(
                            items=items,
                            github_token=token,
                            owner=owner,
                            name=name,
                        )
                    )

        for repositories_future in asyncio.as_completed(prepare_repositories_coroutines):
            await repositories_future

    def dump_data(self, owner: str, name: str, items: list[dict]):
        data_path = os.path.join(self.dst_data_folder, f"{owner}__{name}.jsonl")
        with open(data_path, "a") as f_data_output:
            for item in items:
                f_data_output.write(json.dumps(item) + "\n")
