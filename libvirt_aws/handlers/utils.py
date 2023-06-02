from __future__ import annotations
from typing import Optional, List, Protocol

import sqlite3


class DBExecutemany(Protocol):
    def executemany(self, query: str, params: list) -> DBExecutemany:
        """sqlite3.Cursor.executemany()"""

class DBExecute(Protocol):
    def execute(self, query: str, params: list) -> None:
        """sqlite3.Cursor.execute()"""


def add_tags(
    db: DBExecutemany,
    resource_name: str,
    resource_type: str,
    tag_specs: Optional[List[dict]]
) -> None:
    '''insert tag records but don't commit the transaction''' 
    if not tag_specs:
        return

    tags = {}
    for tag_spec in tag_specs:
        for tag in tag_spec["Tag"]:
            tags[tag["Key"]] = tag["Value"]

    db.executemany(
        """
            INSERT INTO tags
                (resource_name, resource_type, tagname, tagvalue)
            VALUES (?, ?, ?, ?)
        """,
        [[resource_name, resource_type, k, v] for k, v in tags.items()]
    )

def remove_tags(
    db: DBExecute,
    resource_name: str,
    resource_type: str
) -> None:
    db.execute(
        """
            DELETE FROM tags
            WHERE
                resource_name = ? AND
                resource_type = ?
        """,
        [resource_name, resource_type],
    )
