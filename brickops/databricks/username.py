from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brickops.databricks.context import DbContext


def get_username(db_context: DbContext) -> str:
    """Return username (an email), stripped for dots and special chars.

    It is used part of naming of dev db prefix, and similar.
    """
    # must be passed in as param as databricks lib not available in UC cluster
    email = db_context.username
    return email.split("@")[0].replace(".", "").replace("-", "")
