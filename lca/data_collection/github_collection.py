import asyncio
import dataclasses
import logging
import time
import urllib
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import aiohttp
from tenacity import after_log, before_sleep_log, retry, retry_if_result, stop_after_attempt, wait_fixed

GITHUB_API_TRIES_LIMIT = 10

SECONDARY_LIMIT_SLEEP_TIME = 30
OTHER_ERRORS_SLEEP_TIME = 10

GITHUB_API_URL = "https://api.github.com"
TIME_DIVERGENCE_CONST = 300

REPOSITORIES_START_DATE = datetime.fromisoformat("2008-01-01T00:00:00")
REPOSITORIES_PER_SINGLE_TOKEN_PERIOD_IN_DAYS = 30
REPOSITORIES_PAGE_SIZE = 100
REPOSITORIES_MAX_AMOUNT_PER_SEARCH = 850.0

MAX_OPEN_HTTP_CONNECTIONS = 512

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

"""
Github API specific headers
"""
RETRY_AFTER = "Retry-After"  # Indicates when the request should be retried after hitting secondary rate limit
X_RATELIMIT_RESET = "X-RateLimit-Reset"  # Indicates when the primary rate limit will be reset


@dataclasses.dataclass(frozen=True)
class GithubRepository:
    """
    Github repository data with branch, last commit SHA and collection timestamp
    """

    repo_id: Optional[int]
    name: str
    owner: str
    created_at: Optional[datetime]
    branch: Optional[str]
    commit_sha: Optional[str]
    collection_timestamp: datetime
    meta: Optional[dict]
    problems: Optional[str]


@dataclasses.dataclass(frozen=True)
class GithubApiRequestQuery:
    """
    Search request for Github API containing created_at date range, query based on Github API query language and
    some other parameters like sorting.
    """

    created_at_start_date: datetime
    created_at_end_date: datetime
    search_query: str
    other_parameters: str


@dataclasses.dataclass(frozen=True)
class GithubApiResponse:
    """
    Basic Github API response with one header "next" (link to the next page of results in search) if it exists
    """

    data: dict
    headers: dict


@dataclasses.dataclass(frozen=True)
class GithubApiListRepositoriesResponse:
    """
    Search response which contains repository data, link to the next page of results if any and basic information
    like total number of found repositories and whether the search result is incomplete.
    """

    total_count: int
    incomplete_results: bool
    repositories: List[GithubRepository]
    next_page_url: Optional[str]


GithubApiResponseOrError = Union[GithubApiResponse, Exception]
GithubRepositoryOrError = Union[GithubRepository, Exception]
GithubApiListRepositoriesResponseOrError = Union[GithubApiListRepositoriesResponse, Exception]


# Github API related errors


class GithubApiError(Exception):
    pass


class NotRetryableGithubApiError(Exception):
    pass


# General requests methods
def return_last_value(retry_state):
    """return the result of the last call attempt"""
    if retry_state.args:
        logger.error(f"Not processed: url {retry_state.args[-1]}")
    else:
        logger.error(f"Not processed: args - {retry_state.args} kwargs - {retry_state.kwargs}")
    return retry_state.outcome.result()


@retry(
    reraise=True,
    wait=wait_fixed(OTHER_ERRORS_SLEEP_TIME),
    stop=stop_after_attempt(GITHUB_API_TRIES_LIMIT),
    retry=retry_if_result(lambda res: isinstance(res, Exception) and not isinstance(res, NotRetryableGithubApiError)),
    before_sleep=before_sleep_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
    retry_error_callback=return_last_value,
)
async def make_github_http_request(
    http_session: aiohttp.ClientSession, github_token: str, url: str
) -> GithubApiResponseOrError:
    """
    Make http request for specified url with github authorization and return http response body
    or throw an aggregated error.
    :param http_session: http session
    :param github_token: GitHub auth token
    :param url: url to open
    :return: response body and important headers or throws an exception
    """

    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.mercy-preview+json",  # allows to retrieve topics from repositories
    }

    try:
        logger.debug(f"Trying to make a request: {url}")
        async with http_session.get(url, headers=headers) as response:
            status_code = response.status

            if status_code == 200:
                response_headers = {}
                if "next" in response.links and "url" in response.links["next"]:
                    response_headers["next"] = response.links["next"]["url"]

                logger.debug("Success")
                return GithubApiResponse(await response.json(), response_headers)

            elif status_code == 403:
                return await handle_github_rate_limit(response)

            elif status_code == 504:
                return await handle_github_ban(response)

            elif status_code in [404, 422, 409, 451]:
                # Not retryable errors:
                # 404 - Not Found
                # 422 - Unprocessable Entity
                # 409 - Conflict
                # 451 - Unavailable For Legal Reasons
                response_json = await response.json()
                error_message = response_json.get("message", f'No "message" key in response. HTTP code {status_code}.')
                logger.error(f"{error_message} for {url}; {response}")
                return NotRetryableGithubApiError(error_message)
            else:
                error_message = f"HTTP code {status_code} for {url}; {response}"
                logger.error(error_message)
                return GithubApiError(error_message)

    except aiohttp.ClientError as e:
        error_message = f"Error happened while performing the request for {url}: {e}"
        return GithubApiError(error_message)


async def handle_github_ban(response: aiohttp.ClientResponse) -> GithubApiError:
    sleep_time = 600
    error_message = f"Github API returned 504 error. {response.url} sleep for {sleep_time}"
    logger.warning(error_message)
    await asyncio.sleep(sleep_time)
    return GithubApiError(error_message)


async def handle_github_rate_limit(response: aiohttp.ClientResponse) -> GithubApiError:
    """
    Rate limit errors from github have 403 HTTP status. This method handles rate limit errors and propagates other errors.
    To fix exceeded rate limit this method performs a delay before making the next call.
    :param response: http response
    :return: an error about rate limiting or an error during parsing the response
    """
    response_json = await response.json()

    if "message" in response_json:
        message = response_json["message"]

        if message.startswith("You have exceeded a secondary rate limit."):
            sleep_time = int(response.headers[RETRY_AFTER])
            error_message = f"Secondary Github API rate limit was exceeded. {response.url} sleep for {sleep_time}"
            logger.warning(error_message)
            await asyncio.sleep(sleep_time)
            return GithubApiError(error_message)

        elif message.startswith("API rate limit exceeded"):
            reset_time = int(response.headers[X_RATELIMIT_RESET])
            #  add some time because of possible time divergence
            sleep_time = reset_time - int(time.time()) + TIME_DIVERGENCE_CONST
            error_message = f"Github API rate limit exceeded. {response.url} sleep for {sleep_time}"
            logger.warning(error_message)
            await asyncio.sleep(sleep_time)
            return GithubApiError(error_message)

        else:
            error_message = f"Not a rate limiting error. {response}"
            logger.error(error_message)
            return GithubApiError(error_message)

    else:
        error_message = f"No message in response. {response}"
        logger.error(error_message)
        return GithubApiError(error_message)


# Specific Github API requests


async def get_repository_meta(
    http_session: aiohttp.ClientSession, github_token: str, owner: str, name: str
) -> GithubRepositoryOrError:
    """
    Get github repository representation with id, default branch and meta
    :param http_session: http session
    :param github_token: GitHub auth token
    :param owner: repository owner
    :param name: repository name
    :return: GithubRepository with appropriate data or an error
    """
    url = f"{GITHUB_API_URL}/repos/{owner}/{name}"

    github_api_response_or_error = await make_github_http_request(http_session, github_token, url)
    if isinstance(github_api_response_or_error, Exception):
        return github_api_response_or_error

    default_branch = github_api_response_or_error.data["default_branch"]
    repo_id = int(github_api_response_or_error.data["id"])
    repo_name = github_api_response_or_error.data["name"]
    repo_owner = github_api_response_or_error.data["owner"]["login"]
    repo_collection_time = datetime.now(timezone.utc)
    repo_created_at = datetime.fromisoformat(github_api_response_or_error.data["created_at"].replace("Z", "+00:00"))

    return GithubRepository(
        repo_id=repo_id,
        name=repo_name,
        owner=repo_owner,
        created_at=repo_created_at,
        branch=default_branch,
        commit_sha=None,
        collection_timestamp=repo_collection_time,
        meta=github_api_response_or_error.data,
        problems=None,
    )


async def get_repository_last_commit_sha(
    http_session: aiohttp.ClientSession, github_token: str, owner: str, name: str, branch: str
) -> Union[str, Exception]:
    """
    Get repository last commit sha for specified branch
    :param http_session: http session
    :param github_token: GitHub auth token
    :param owner: repository owner
    :param name: repository name
    :param branch: repository branch
    :return: sha or an error
    """
    url = f"{GITHUB_API_URL}/repos/{owner}/{name}/commits/{branch}"

    github_api_response_or_error = await make_github_http_request(http_session, github_token, url)
    if isinstance(github_api_response_or_error, Exception):
        return github_api_response_or_error

    return github_api_response_or_error.data["sha"]


async def get_all_branches_from_repository(
    http_session: aiohttp.ClientSession, github_token: str, owner: str, name: str
) -> Union[List[str], Exception]:
    """
    Get all branch names from repository.
    :param http_session: http session
    :param github_token: GitHub auth token
    :param owner: repository owner
    :param name: repository name
    :return: list of branch names or an error
    """
    url = f"{GITHUB_API_URL}/repos/{owner}/{name}/branches"

    github_api_response_or_error = await make_github_http_request(http_session, github_token, url)
    if isinstance(github_api_response_or_error, Exception):
        return github_api_response_or_error

    return [branch["name"] for branch in github_api_response_or_error.data]


# Auto collection of repository information


async def list_and_process_repositories_by_query(
    http_session: aiohttp.ClientSession, github_token: str, query: GithubApiRequestQuery, process_all_branches: bool
) -> GithubApiListRepositoriesResponseOrError:
    """
    Lists the first page of all repositories matching search query and parameters,
    passes them one by one to the consumer and returns an url with the next page of results.
    :param http_session: http session
    :param github_token: GitHub auth token
    :param query: query for Github Search API which contains: not urlencoded search query,
                  other parameters (sorting is not supported), specific time interval for repository creation date
    :param process_all_branches: process all available branches or only default branch
    :return: list of repos or an error
    """
    if query.created_at_start_date is not None and query.created_at_end_date is not None:
        created_query = f"+created:{query.created_at_start_date.isoformat()}..{query.created_at_end_date.isoformat()}"
    else:
        if query.created_at_start_date is not None:
            created_query = f"+created:>={query.created_at_start_date.isoformat()}"
        else:
            created_query = f"+created:<{query.created_at_end_date.isoformat()}"

    search_query = f"{urllib.parse.quote(query.search_query)}{created_query}"

    url = (
        f"{GITHUB_API_URL}/search/repositories"
        f"?per_page={REPOSITORIES_PAGE_SIZE}"
        f"&q={search_query}"
        f"&{query.other_parameters}"
    )
    return await list_and_process_repositories_by_url(http_session, github_token, url, process_all_branches)


async def list_and_process_repositories_by_url(
    http_session: aiohttp.ClientSession, github_token: str, url: str, process_all_branches: bool
) -> GithubApiListRepositoriesResponseOrError:
    """
    Lists the first page of all repositories matching search query and parameters,
    passes them one by one to the consumer and returns an url with the next page of results.
    :param http_session: http session
    :param github_token: GitHub auth token
    :param url: Github Search API full url for example: https://api.github.com/search/repositories?per_page=100&sort=stars&order=desc&q=stars:%3E0
    :param process_all_branches: process all available branches or only default branch
    :return: list of repos or an error
    """
    logger.info(f"Starting to process repositories found by this url: {url}")
    timestamp = datetime.now(timezone.utc)
    github_api_response_or_error = await make_github_http_request(http_session, github_token, url)
    if isinstance(github_api_response_or_error, Exception):
        return github_api_response_or_error

    total_count = github_api_response_or_error.data["total_count"]

    incomplete_results = github_api_response_or_error.data["incomplete_results"]
    next_page_url = github_api_response_or_error.headers.get("next", None)
    repositories: List[GithubRepository] = []

    if total_count > REPOSITORIES_MAX_AMOUNT_PER_SEARCH:
        logger.warning(
            f"Total count of repositories {total_count}"
            f" exceeds the maximum amount per search {REPOSITORIES_MAX_AMOUNT_PER_SEARCH};"
            f" skipping fetching repository datas"
        )
        return GithubApiListRepositoriesResponse(
            total_count=total_count,
            incomplete_results=incomplete_results,
            repositories=repositories,
            next_page_url=next_page_url,
        )

    for item in github_api_response_or_error.data["items"]:
        repo_id = int(item["id"])
        repo_name = item["name"]
        repo_owner = item["owner"]["login"]
        repo_collection_time = timestamp
        repo_created_at = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))

        collected_repositories = await iterate_over_branches_and_init_last_commit_sha(
            http_session=http_session,
            github_token=github_token,
            repo_id=repo_id,
            repo_name=repo_name,
            repo_owner=repo_owner,
            repo_collection_time=repo_collection_time,
            repo_created_at=repo_created_at,
            repo_specific_branch=item["default_branch"],
            repo_branches_url=item["branches_url"].partition("{")[0],
            repo_meta=item,
            process_all_branches=process_all_branches,
        )

        repositories.extend(collected_repositories)

    return GithubApiListRepositoriesResponse(
        total_count=total_count,
        incomplete_results=incomplete_results,
        repositories=repositories,
        next_page_url=next_page_url,
    )


# Utils


async def iterate_over_branches_and_init_last_commit_sha(
    http_session: aiohttp.ClientSession,
    github_token: str,
    repo_id: int,
    repo_name: str,
    repo_owner: str,
    repo_collection_time: datetime,
    repo_created_at: datetime,
    repo_specific_branch: str,
    repo_branches_url: str,
    repo_meta: dict,
    process_all_branches: bool,
) -> List[GithubRepository]:
    """
    Gets last commit sha for specific branches of the repo and returns repositories with this data.
    :param http_session: http session
    :param github_token: GitHub auth token
    :param repo_id: internal id of the repo in Github
    :param repo_name: name of the repo
    :param repo_owner: owner of the repo
    :param repo_collection_time: repository meta collection time
    :param repo_created_at: repository creation date
    :param repo_specific_branch: name of a specific branch to get commit sha or default branch
    :param repo_branches_url: url to list all repository branches
    :param repo_meta: collected repository meta
    :param process_all_branches: process all available branches or only default branch
    :return: list of repos
    """

    repositories = []

    if process_all_branches:
        logger.debug(f"Processing all branches for {repo_owner}/{repo_name}")

        branches_url = repo_branches_url.partition("{")[0]
        branches_response_or_error = await make_github_http_request(http_session, github_token, branches_url)

        if isinstance(branches_response_or_error, Exception):
            repository = GithubRepository(
                repo_id=repo_id,
                name=repo_name,
                owner=repo_owner,
                created_at=repo_created_at,
                branch=None,
                commit_sha=None,
                collection_timestamp=repo_collection_time,
                meta=repo_meta,
                problems=str(branches_response_or_error),
            )
            repositories.append(repository)

        else:
            for branch in branches_response_or_error.data:
                branch_name = branch["name"]
                commit_sha = branch["commit"]["sha"] if branch["commit"] else None
                repository = GithubRepository(
                    repo_id=repo_id,
                    name=repo_name,
                    owner=repo_owner,
                    created_at=repo_created_at,
                    branch=branch_name,
                    commit_sha=commit_sha,
                    collection_timestamp=repo_collection_time,
                    meta=repo_meta,
                    problems=None,
                )
                repositories.append(repository)

    else:
        logger.debug(f"Processing branch {repo_specific_branch} for {repo_owner}/{repo_name}")

        commit_sha_or_error = await get_repository_last_commit_sha(
            http_session=http_session,
            github_token=github_token,
            owner=repo_owner,
            name=repo_name,
            branch=repo_specific_branch,
        )

        repository = GithubRepository(
            repo_id=repo_id,
            name=repo_name,
            owner=repo_owner,
            created_at=repo_created_at,
            branch=repo_specific_branch,
            commit_sha=None if isinstance(commit_sha_or_error, Exception) else commit_sha_or_error,
            collection_timestamp=repo_collection_time,
            meta=repo_meta,
            problems=str(commit_sha_or_error) if isinstance(commit_sha_or_error, Exception) else None,
        )
        repositories.append(repository)

    return repositories


def parse_github_url(url: str) -> Tuple[str, str]:
    """
    Extract repository owner and name from a github url
    https://github.com/DimaProskurin/News-Bot -> (DimaProskurin, News-Bot)
    :param url: github url
    :return: tuple with repository owner and name
    """
    owner, name = urlparse(url).path.split("/")[1:]
    return owner, name
