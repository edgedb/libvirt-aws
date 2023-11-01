from __future__ import annotations
from typing import Optional, List, Protocol, Dict

import sqlite3


def add_tags(
    db,
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
    db,
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


def get_tags(
    db,
    resource_name: str,
    resource_type: str
) -> List[Dict[str, str]]:
    rows = db.execute(
        """
            SELECT tagname, tagvalue FROM tags
            WHERE
                resource_name = ? AND
                resource_type = ?
        """,
        [resource_name, resource_type],
    ).fetchall()

    tags = []
    for row in rows:
        tags.append({
            'key': row[0],
            'value': row[1],
        })

    return tags
