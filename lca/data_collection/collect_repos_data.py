import asyncio
import os
from argparse import ArgumentParser
from typing import Optional

import aiohttp

from lca.data_collection.github_utils import GITHUB_API_URL, make_github_http_request
from lca.data_collection.process_utils import dump_repo_data_to_jsonl, get_github_tokens, get_repos, process_repos


async def _process_repos_under_http_session(repos_path: str, tokens_path: str, search_object: str, save_dir: str):
    async with aiohttp.ClientSession() as http_session:

        async def _collect_repo_data(owner: str, name: str, github_token: str) -> Optional[Exception]:
            current_url = f"{GITHUB_API_URL}/repos/{owner}/{name}/{search_object}?per_page=100&state=all"

            while current_url is not None:
                print(f"Processing: {current_url}")

                github_api_response_or_error = await make_github_http_request(http_session, github_token, current_url)
                if isinstance(github_api_response_or_error, Exception):
                    return github_api_response_or_error

                data = github_api_response_or_error.data
                dump_repo_data_to_jsonl(owner, name, data, save_dir)
                current_url = github_api_response_or_error.headers.get("next", None)

            return None

        await process_repos(_collect_repo_data, repos_path, tokens_path, batch_size=10)


def main(repos_path: str, tokens_path: str, search_object: str, save_dir: str):
    os.makedirs(save_dir, exist_ok=True)
    asyncio.run(_process_repos_under_http_session(repos_path, tokens_path, search_object, save_dir))


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-o",
        "--search-object",
        type=str,
        default="pulls",
        help="Object to search in github (e.x. pulls/comments/issues)",
    )

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
        "--save-dir",
        type=str,
        default="./../../repo_info",
        help="Path to the directory where collected data will be saved",
    )

    args = argparser.parse_args()

    repos = get_repos(args.repos_path)
    tokens = get_github_tokens(args.tokens_path)

    main(args.repos_path, args.tokens_path, args.search_object, args.save_dir)
