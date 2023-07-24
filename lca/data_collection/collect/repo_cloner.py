import os
from typing import Optional

from lca.data_collection.collect.github_utils import clone_repo
from lca.data_collection.collect.repo_processor import RepoProcessor


class RepoCloner(RepoProcessor):

    def __init__(self, github_tokens: list[str], repos_dir: str):
        super().__init__(github_tokens)
        self.repos_dir = repos_dir

        os.makedirs(self.repos_dir, exist_ok=True)

    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        repo_dir = f"{self.repos_dir}/{owner}__{name}"
        if not os.path.exists(repo_dir):
            return await clone_repo(owner, name, github_token, repo_dir)
