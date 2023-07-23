import asyncio
from argparse import ArgumentParser

import aiohttp

from lca.data_collection.process.prs_issues_link import CommentsProcessor


def get_repos(repos_path, from_repo) -> list[tuple]:
    with open(repos_path, "r") as f_repos:
        repos = [tuple(line.strip().split("/")) for line in f_repos]
        from_repo_index = 0
        from_owner, from_name = from_repo.split("__")
        for i, r in enumerate(repos):
            owner, name = r
            if owner == from_owner and name == from_name:
                from_repo_index = i
        return repos[from_repo_index:]


def get_tokens(tokens_path) -> list[str]:
    with open(tokens_path, "r") as f_tokens:
        return [line.strip() for line in f_tokens]


async def process_data(repos: list[tuple], tokens: list[str], src_data_folder: str, dst_data_folder: str):
    async with aiohttp.ClientSession() as http_session:
        provider = CommentsProcessor(http_session, tokens, src_data_folder, dst_data_folder)
        await provider.process_repositories_in_batches(repos)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-r",
        "--repos-path",
        type=str,
        default="./../../../resources/repository_list.txt",
        help="Path to txt file with repos list to process",
    )

    argparser.add_argument(
        "-t",
        "--tokens-path",
        type=str,
        default="./../../../resources/tokens.txt",
        help="Path to txt file with qit tokens",
    )

    argparser.add_argument(
        "-f",
        "--from-repo",
        type=str,
        default="mpusz__mp-units",
        help="Repo name to start from",
    )

    argparser.add_argument(
        "-s",
        "--src-data-folder",
        type=str,
        default="./../../../comments",
        help="Path to the directory where data stored initially",
    )

    argparser.add_argument(
        "-d",
        "--dst-data-folder",
        type=str,
        default="./../../../issues_pulls_link",
        help="Path to the directory where data with extra loaded features will be stored",
    )

    args = argparser.parse_args()

    repos = get_repos(args.repos_path, args.from_repo)
    tokens = get_tokens(args.tokens_path)

    asyncio.run(process_data(repos, tokens, args.src_data_folder, args.dst_data_folder))
