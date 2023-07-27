import asyncio
import os
from argparse import ArgumentParser
from typing import Optional

from lca.data_collection.github_utils import clone_repo
from lca.data_collection.process_utils import process_repos


def main(repos_path: str, tokens_path: str, save_dir: str):
    async def _clone_repo(owner: str, name: str, github_token: str) -> Optional[Exception]:
        repo_dir = f"{save_dir}/{owner}__{name}"
        if not os.path.exists(repo_dir):
            return await clone_repo(owner, name, github_token, repo_dir)

        return None

    os.makedirs(save_dir, exist_ok=True)
    asyncio.run(process_repos(_clone_repo, repos_path, tokens_path, batch_size=10))


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
        "--save-dir",
        type=str,
        default="./../../repos",
        help="Path to the directory where collected data will be saved",
    )

    args = argparser.parse_args()

    main(args.repos_path, args.tokens_path, args.save_dir)
