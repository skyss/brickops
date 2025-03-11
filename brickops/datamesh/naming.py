from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING, Any

from brickops.databricks.context import current_env, get_context
from brickops.databricks.username import get_username
from brickops.dataops.deploy.repo import git_source
from brickops.gitutils import clean_branch, commit_shortref


if TYPE_CHECKING:
    from brickops.databricks.context import DbContext


@dataclass
class ParsedPath:
    org: Optional[str] = None
    domain: str
    project: str
    flow: str
    flow_type: str


def escape_sql_name(name: str) -> str:
    parts = name.split(".")
    return ".".join(
        [escape_norwegian_chars(part) if "`" not in part else part for part in parts]
    )


def escape_norwegian_chars(name: str) -> str:
    norwegian_chars = ["æ", "ø", "å"]
    return f"`{name}`" if any((c in norwegian_chars) for c in name) else name


def build_table_name(
    tbl: str,
    db: str,
    cat: str | None = None,
    db_context: DbContext | None = None,
) -> str:
    """Cat is the Unity Catalog catalog name."""
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    if not tbl:
        msg = "tbl must be a non-empty string"
        raise ValueError(msg)
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not cat:
        cat = catname_from_path()
    if not db_context:
        db_context = get_context()

    db_name = dbname(db=db, cat=cat, db_context=db_context)
    return escape_sql_name(f"{db_name}.{tbl}")


def dbname(
    db: str,
    cat: str,
    db_context: DbContext | None = None,
) -> str:
    """Generate a database name from db, cat, env."""
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not db_context:
        db_context = get_context()
    env = current_env(db_context)
    db_prefix = dbprefix(env=env, db_context=db_context)
    return escape_sql_name(f"{cat}.{db_prefix}{db}")


def dbprefix(env: str, db_context: DbContext) -> str:
    """Compose deployment prefix from env and git config."""
    if env == "prod":
        return ""
    dep_prefix = f"{get_username(db_context)}_"
    # Get git state to build db name
    git_src = _git_src(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{dep_prefix}{branch}_{short_ref}_"


def _git_src(db_context: DbContext) -> dict[str, Any]:
    """Get git src params from either task params or repos api.

    Widget parameters take precedence over repos api.
    """
    git_data = git_source(db_context)
    git_data_from_widgets = _git_src_from_widget_params(db_context)
    return git_data | git_data_from_widgets


def _git_src_from_widget_params(db_context: DbContext) -> dict[str, Any]:
    widget_data = {
        "git_url": db_context.widgets.get("git_url"),
        "git_branch": db_context.widgets.get("git_branch"),
        "git_commit": db_context.widgets.get("git_commit"),
        "git_path": db_context.widgets.get("git_path"),
    }
    return {k: v for k, v in widget_data.items() if v is not None}


def parse_path(path: str) -> ParsedPath | None:
    """Parse path to extract org, domain, project, and flow."""

    if full_mesh_env():  # Include org section if full mesh
        rexp = r".*\/org/([^/]+)\/domains/([^/]+)\/projects\/([^/]+)\/(?:flows|explore)\/([^/]+)\/.+"
    else:
        rexp = r".*\/domains/([^/]+)\/projects\/([^/]+)\/(?:flows|explore)\/([^/]+)\/.+"
    re_ret = re.search(
        rexp,
        path,
        re.IGNORECASE,
    )
    if re_ret is None:
        return None


    if full_mesh_env():  # Include org section if full mesh
        if len(re_ret.groups()) < 5:  # noqa: PLR2004
            logging.warning(
                """parse_path: unexpected number of groups for full mesh.
                Is the notebook in the correct folder!?"""
            )
            return None
        return ParsedPath(
            org=re_ret[0],
            domain=re_ret[1],
            project=re_ret[2],
            flowtype=re_ret[3],
            flow=re_ret[4],
        )
    if len(re_ret.groups()) < 4:  # noqa: PLR2004
        logging.warning(
            """parse_path: unexpected number of groups.
            Is the notebook in the correct folder!?"""
        )
        return None
    return ParsedPath(
        domain=re_ret[0],
        project=re_ret[1],
        flowtype=re_ret[2],
        flow=re_ret[3],
    )


def extract_catname_from_path(path: str) -> str:
    """Derive catalog name from repo data mesh structure.

    We simply use domain as base catalog name.
    """
    if result := parse_path(path):
        org, domain, proj, _ = result
        if full_mesh_env():
            return f"{org}_{domain}_{proj}"
        else:
            return domain
    return ""


def catname_from_path() -> str:
    """Derive catalog name from repo data mesh structure.

    We simply use domain as base catalog name.

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    db_context = get_context()
    nb_path = db_context.notebook_path
    return escape_sql_name(extract_catname_from_path(nb_path))


def full_mesh_env():
    """Return True if BRICKOPS_FULL_MESH is set to True."""
    return os.environ.get("BRICKOPS_FULL_MESH", False)
