import asyncio
from argparse import ArgumentParser

from lca.data_collection.collect.repo_cloner import RepoCloner
from lca.data_collection.collect.utils import get_repos, get_tokens


async def collect_repo_data(repos: list[tuple], tokens: list[str], data_folder: str):
    provider = RepoCloner(tokens, data_folder)
    await provider.process_repositories_in_batches(repos)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-r",
        "--repos-path",
        type=str,
        default="./../../../../resources/repository_list_short.txt",
        help="Path to txt file with repos list to process",
    )

    argparser.add_argument(
        "-t",
        "--tokens-path",
        type=str,
        default="./../../../../resources/tokens.txt",
        help="Path to txt file with qit tokens",
    )

    argparser.add_argument(
        "-s",
        "--save-dir",
        type=str,
        default="./../../../../repos",
        help="Path to the directory where collected data will be saved",
    )

    args = argparser.parse_args()

    repos = get_repos(args.repos_path)
    tokens = get_tokens(args.tokens_path)

    asyncio.run(collect_repo_data(repos, tokens, args.save_dir))
