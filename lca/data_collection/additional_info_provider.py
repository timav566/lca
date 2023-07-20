import abc
import json
import os
from typing import Optional

import aiohttp

from lca.data_collection.repo_info_provider import RepoInfoProvider


class AdditionalInfoProvider(RepoInfoProvider):
    def __init__(
        self, http_session: aiohttp.ClientSession, github_tokens: list[str], src_data_folder: str, dst_data_folder: str
    ):
        super().__init__(http_session, github_tokens, dst_data_folder)
        self.src_data_folder = src_data_folder

    @abc.abstractmethod
    async def process_item(self, item: dict, owner: str, name: str, github_token: str) -> Optional[Exception]:
        pass

    def dump_data(self, owner: str, name: str, items: list[dict]):
        data_path = os.path.join(self.data_folder, f"{owner}__{name}.jsonl")
        with open(data_path, "a") as f_data_output:
            for item in items:
                f_data_output.write(json.dumps(item) + "\n")

    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        for file in os.listdir(self.src_data_folder):
            file_path = os.path.join(self.src_data_folder, file)
            if os.path.isfile(file_path):
                with open(file_path, "r") as f:
                    for line in f:
                        item = json.loads(line)
                        result = await self.process_item(item, owner, name, github_token)

                        if isinstance(result, Exception):
                            return result

        return None
