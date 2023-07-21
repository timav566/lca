import re
from typing import Optional

import aiohttp

from lca.data_collection.process.repo_data_processor import RepoDataProcessor


class CommentsProcessor(RepoDataProcessor):

    def __init__(
            self, http_session: aiohttp.ClientSession, github_tokens: list[str], src_data_folder: str,
            dst_data_folder: str
    ):
        super().__init__(http_session, github_tokens, src_data_folder, dst_data_folder)

    @staticmethod
    def _find_linked_issues_ids(body: str):
        """ https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/autolinked-references-and-urls """

        patterns = [
            # https://github.com/jlord/sheetsee.js/issues/26
            r"https:\/\/github\.com\/[^\/\s]+\/[^\/\s]+\/issues\/(?P<issue_number>\d+)",
            # #26
            r"\s#(?P<issue_number>\d+)",
            # GH-26
            r"GH\-(?P<issue_number>\d+)",
            # jlord/sheetsee.js#26
            r"[^\/\s]+\/[^\/\s]+#(?P<issue_number>\d+)"
        ]

        linked_issues_ids = []
        for p in patterns:
            linked_issues_ids += re.findall(p, body)

        return linked_issues_ids

    async def process_item(self, item: dict, owner: str, name: str, github_token: str) -> Optional[Exception]:
        print(item['html_url'])
        print(item['issue_url'])
        print(self._find_linked_issues_ids(item['body']))
        self.dump_data(owner, name, item)

        return None
