from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brickops.databricks.context import DbContext


def nbrelpath(db_context: DbContext) -> Path:
    """Get relative path of deploy notebook in repo, from the full notebook path."""
    _nbpath = db_context.notebook_path
    return Path("/".join(_nbpath.split("/")[4:]))


def nbrelfolder(db_context: DbContext, root_folder: str) -> str:
    """Return relative path of the folder of the notebook.

    Example path:
    domains/transport/projects/taxinyc/flows/prep/revenue
    """
    if not root_folder:
        root_folder = str(nbrelpath(db_context))

    return str(Path(db_context.notebook_path).relative_to(root_folder).parent)
