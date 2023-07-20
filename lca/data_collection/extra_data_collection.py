import asyncio
from argparse import ArgumentParser

import aiohttp

from lca.data_collection.pulls_extra_data_provider import PullsExtraDataProvider


def get_repos(repos_path) -> list[tuple]:
    with open(repos_path, "r") as f_repos:
        return [tuple(line.strip().split("/")) for line in f_repos]


def get_tokens(tokens_path) -> list[str]:
    with open(tokens_path, "r") as f_tokens:
        return [line.strip() for line in f_tokens]


async def collect_extra_data(repos: list[tuple], tokens: list[str], src_data_folder: str, dst_data_folder: str):
    async with aiohttp.ClientSession() as http_session:
        provider = PullsExtraDataProvider(http_session, tokens, src_data_folder, dst_data_folder)
        await provider.process_repositories(repos)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-r",
        "--repos-path",
        type=str,
        default="./../../resources/repository_list_short.txt",
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
        "--src-data-folder",
        type=str,
        default="./../../repo_info",
        help="Path to the directory where data stored initially",
    )

    argparser.add_argument(
        "-d",
        "--dst-data-folder",
        type=str,
        default="./../../repo_info",
        help="Path to the directory where data with extra loaded features will be stored",
    )

    args = argparser.parse_args()

    repos = get_repos(args.repos_path)
    tokens = get_tokens(args.tokens_path)

    asyncio.run(collect_extra_data(repos, tokens, args.src_data_folder, args.dst_data_folder))
