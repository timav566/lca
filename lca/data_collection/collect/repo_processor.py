import abc
import asyncio
from typing import Optional


class RepoProcessor(abc.ABC):
    def __init__(self, github_tokens: list[str]):
        self.github_tokens = github_tokens

    async def process_repositories_in_batches(self, repositories: list[tuple], batch_size: int = 10):
        for i in range(0, len(repositories), batch_size):
            await self.process_repositories(repositories[i:i + batch_size])

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

    @abc.abstractmethod
    async def process_repo(self, github_token: str, owner: str, name: str) -> Optional[Exception]:
        pass
