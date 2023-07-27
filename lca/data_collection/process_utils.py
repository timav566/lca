import asyncio
import json
import os
from typing import Any, Callable, Optional


def get_repos(repos_path: str) -> list[tuple[str, str]]:
    repos = []
    with open(repos_path, "r") as f_repos:
        for line in f_repos:
            owner, name = line.strip().split("/")
            repos.append((owner, name))
        return repos


def get_github_tokens(tokens_path: str) -> list[str]:
    with open(tokens_path, "r") as f_tokens:
        return [line.strip() for line in f_tokens]


async def process_repos(
    process_repo: Callable[[str, str, Optional[str]], Any],
    repos_path: str,
    tokens_path: Optional[str] = None,
    batch_size: Optional[int] = None,
):
    """
    Runs `process_repo` on each repo from file located in `repos_path`
    :param process_repo: func that takes owner, name and optional token as input and does some processing task on repo
    :param repos_path: path to file where list of repos is stored
    :param tokens_path: path to file where list of tokens is stored
    :param batch_size: size of repos batch to process simultaneously or None if process without batching
    """
    repos = get_repos(repos_path)
    github_tokens = get_github_tokens(tokens_path) if tokens_path is not None else None
    if batch_size is None:
        await _process_repos(process_repo, repos, github_tokens)
    else:
        for i in range(0, len(repos), batch_size):
            await _process_repos(process_repo, repos[i : i + batch_size], github_tokens)


async def _process_repos(
    process_repo: Callable[[str, str, Optional[str]], Any],
    repos: list[tuple[str, str]],
    github_tokens: Optional[list[str]],
):
    prepare_repositories_coroutines = []

    for i, (owner, name) in enumerate(repos):
        github_token = github_tokens[i % len(github_tokens)] if github_tokens is not None else None
        prepare_repositories_coroutines.append(process_repo(owner, name, github_token))

    for repositories_future in asyncio.as_completed(prepare_repositories_coroutines):
        await repositories_future


async def process_repos_data(
    process_repo_data: Callable[[str, str, list[dict], Optional[str]], Any],
    repos_data_path: str,
    tokens_path: Optional[str] = None,
    batch_size: Optional[int] = None,
):
    """
    Runs `process_repo` on each repo from file located in `repos_path`
    :param process_repo_data: func that takes owner, name, repo data items and optional token as input and does some processing task on repo's data
    :param repos_data_path: path to file where list of repos' data is stored
    :param tokens_path: path to file where list of tokens is stored
    :param batch_size: size of repos batch to process simultaneously or None if process without batching
    """
    filenames = os.listdir(repos_data_path)
    github_tokens = get_github_tokens(tokens_path) if tokens_path is not None else None
    if batch_size is None:
        await _process_repos_data(process_repo_data, repos_data_path, filenames, github_tokens)
    else:
        for i in range(0, len(filenames), batch_size):
            await _process_repos_data(process_repo_data, repos_data_path, filenames[i : i + batch_size], github_tokens)


async def _process_repos_data(
    process_repo_data: Callable[[str, str, list[dict], Optional[str]], Any],
    repos_data_path: str,
    filenames: list[str],
    github_tokens: Optional[list[str]] = None,
):
    prepare_repositories_coroutines = []
    for i, filename in enumerate(filenames):
        file_path = os.path.join(repos_data_path, filename)

        if os.path.isfile(file_path) and filename.endswith(".jsonl"):
            # Parse from <owner>__<name>.jsonl
            owner, name = filename.split(".")[0].split("__")
            github_token = github_tokens[i % len(github_tokens)] if github_tokens is not None else None
            with open(file_path, "r") as f:
                items = [json.loads(line) for line in f]
                prepare_repositories_coroutines.append(process_repo_data(owner, name, items, github_token))

    for repositories_future in asyncio.as_completed(prepare_repositories_coroutines):
        await repositories_future


def dump_repo_data_to_jsonl(owner: str, name: str, items: list[dict], data_folder: str):
    data_path = os.path.join(data_folder, f"{owner}__{name}.jsonl")
    with open(data_path, "a") as f_data_output:
        for item in items:
            f_data_output.write(json.dumps(item) + "\n")
