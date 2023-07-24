def get_repos(repos_path) -> list[tuple]:
    with open(repos_path, "r") as f_repos:
        return [tuple(line.strip().split("/")) for line in f_repos]


def get_tokens(tokens_path) -> list[str]:
    with open(tokens_path, "r") as f_tokens:
        return [line.strip() for line in f_tokens]
