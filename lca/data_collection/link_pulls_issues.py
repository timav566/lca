import asyncio
import os
import re
from argparse import ArgumentParser
from datetime import datetime
from typing import Optional

import aiohttp
from lxml import html

from lca.data_collection.process_utils import dump_repo_data_to_jsonl, process_repos_data


async def _get_linked_issues_from_html(html_url: str, http_session: aiohttp.ClientSession) -> Optional[list[str]]:
    """To slow to run as can not be batched."""
    time_start = datetime.now()
    async with http_session.get(html_url) as response:
        response.raise_for_status()
        html_content = await response.text()
        time_end = datetime.now()
        print(f"Time load html: {time_end - time_start}")

        time_start = datetime.now()
        doc = html.fromstring(html_content.encode("utf-8"))
        linked_issues = [str(e.get("href")) for e in doc.xpath('//form[@aria-label="Link issues"]/span/a')]
        time_end = datetime.now()
        print(f"Time parse html: {time_end - time_start}")

        return linked_issues


def _get_linked_issue_from_comment(comment_body: str, owner: str, name: str):
    """https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/autolinked-references-and-urls"""

    patterns = [
        # https://github.com/jlord/sheetsee.js/issues/26
        r"https:\/\/github\.com\/[^\/\s]+\/[^\/\s]+\/issues\/(?P<issue_number>\d+)",
        # #26
        r"\s#(?P<issue_number>\d+)",
        # GH-26
        r"GH\-(?P<issue_number>\d+)",
        # jlord/sheetsee.js#26
        r"[^\/\s]+\/[^\/\s]+#(?P<issue_number>\d+)",
    ]

    linked_issues_ids = []
    for p in patterns:
        linked_issues_ids += re.findall(p, comment_body)

    return [f"https://api.github.com/repos/{owner}/{name}/issues/{issue_id}" for issue_id in linked_issues_ids]


async def _get_links_from_comments(owner: str, name: str, items: list[dict], save_dir: str) -> Optional[Exception]:
    prs_issues_links = []

    for item in items:
        prs_issues_links.append(
            {
                "comment_url": item["url"],
                "issue_url": item["issue_url"],
                "linked_issue_urls": _get_linked_issue_from_comment(item["body"], owner, name),
            }
        )

    dump_repo_data_to_jsonl(owner, name, prs_issues_links, save_dir)

    return None


def main(data_path: str, save_dir: str):
    os.makedirs(save_dir, exist_ok=True)

    asyncio.run(
        process_repos_data(
            lambda owner, name, items, _: _get_links_from_comments(owner, name, items, save_dir),
            data_path,
            tokens_path=None,
            batch_size=10,
        )
    )


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument(
        "-r",
        "--data-path",
        type=str,
        default="./../../comments",
        help="Path to directory where comments for repos are stored",
    )

    argparser.add_argument(
        "-s",
        "--save-dir",
        type=str,
        default="./../../pull_issues_links_2",
        help="Path to the directory where collected data will be saved",
    )

    args = argparser.parse_args()

    main(args.data_path, args.save_dir)
