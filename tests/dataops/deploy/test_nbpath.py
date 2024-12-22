from pathlib import Path

import pytest

from src.databricks.context import DbContext
from src.dataops.deploy.nbpath import nbrelfolder


@pytest.fixture
def db_context() -> DbContext:
    return DbContext(
        api_token="token",  # noqa: S106
        api_url="url",
        notebook_path="/Users/john.erik.sloper@vlfk.no/dp-notebooks/domains/test/projects/project/flows/flow/testflow/deploy.py",
        username="TestUser",
    )


def test_that_nbrelfolder_correctly_strips_root_folder(db_context: DbContext) -> None:
    result = nbrelfolder(
        db_context, root_folder="/Users/john.erik.sloper@vlfk.no/dp-notebooks"
    )
    assert result == str(Path("domains/test/projects/project/flows/flow/testflow"))


def test_that_nbrelfolder_throws_value_error_when_root_folder_is_wrong(
    db_context: DbContext,
) -> None:
    with pytest.raises(ValueError):
        nbrelfolder(
            db_context, root_folder="/Wrong/john.erik.sloper@vlfk.no/dp-notebooks"
        )
