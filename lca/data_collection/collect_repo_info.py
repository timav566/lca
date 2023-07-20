import asyncio
from argparse import ArgumentParser
from typing import List

import aiohttp

from lca.data_collection.pulls_provider import PullsProvider


def get_repos(repos_path) -> List[tuple]:
    with open(repos_path, 'r') as f_repos:
        return [tuple(line.strip().split("/")) for line in f_repos]


def get_tokens(tokens_path) -> List[str]:
    with open(tokens_path, 'r') as f_tokens:
        return [line.strip() for line in f_tokens]


async def collect_repo_info(repos: List[tuple], tokens: List[str], data_folder: str):
    async with aiohttp.ClientSession() as http_session:
        provider = PullsProvider(http_session, tokens, data_folder)
        await provider.process_repositories(repos)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-r",
        "--repos-path",
        type=str,
        default="./../../resources/repository_list.txt",
        help="Path to txt file with repos list to process",
    )

    argparser.add_argument(
        "-t",
        "--tokens-path",
        type=str,
        default="./../../resources/tokens.txt",
        help="Path to txt file with qit tokens",
    )

    argparser.add_argument(
        "-s",
        "--save-dir",
        type=str,
        default="./../../repo_info",
        help="Path to the directory where collected data will be saved",
    )

    args = argparser.parse_args()

    repos = get_repos(args.repos_path)
    tokens = get_tokens(args.tokens_path)

    asyncio.run(collect_repo_info(repos, tokens, args.save_dir))
