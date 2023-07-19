import asyncio
from typing import List

import aiohttp

from lca.data_collection.commits_provider import CommitsProvider
from lca.data_collection.pulls_provider import PullsProvider


def get_repos() -> List[tuple]:
    with open("./../../resources/repository_list_short.txt", 'r') as f_repos:
        return [tuple(line.strip().split("/")) for line in f_repos]


def get_tokens() -> List[str]:
    with open("./../../resources/tokens.txt", 'r') as f_tokens:
        return [line.strip() for line in f_tokens]


async def collect_repo_info():
    tokens = get_tokens()

    repos = get_repos()

    async with aiohttp.ClientSession() as http_session:
        provider = PullsProvider(http_session, tokens, "./../../pulls")
        await provider.process_repositories(repos)


async def collect_additional_info():
    tokens = get_tokens()

    repos = get_repos()

    async with aiohttp.ClientSession() as http_session:
        provider = CommitsProvider(http_session, tokens, "./../../pulls", "./../../commits")
        await provider.process_repositories(repos)


if __name__ == "__main__":
    asyncio.run(collect_repo_info())
