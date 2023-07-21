import re
from typing import Optional

import aiohttp

from lca.data_collection.process.repo_data_processor import RepoDataProcessor


class CommentsProcessor(RepoDataProcessor):
    def __init__(
        self,
        http_session: aiohttp.ClientSession,
        github_tokens: list[str],
        comments_data_folder: str,
        dst_data_folder: str,
    ):
        super().__init__(http_session, github_tokens, comments_data_folder, dst_data_folder)

    @staticmethod
    def _find_linked_issue_urls(body: str, owner: str, name: str):
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
            linked_issues_ids += re.findall(p, body)

        return [f"https://api.github.com/repos/{owner}/{name}/issues/{issue_id}" for issue_id in linked_issues_ids]

    async def process_items(self, items: list[dict], owner: str, name: str, github_token: str) -> Optional[Exception]:
        prs_issues_links = []

        for item in items:
            prs_issues_links.append(
                {
                    "comment_url": item["url"],
                    "issue_url": item["issue_url"],
                    "linked_issue_urls": self._find_linked_issue_urls(item["body"], owner, name),
                }
            )

        self.dump_data(owner, name, prs_issues_links)

        return None
