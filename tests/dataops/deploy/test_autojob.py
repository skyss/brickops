import pytest
from brickops.dataops.deploy.autojob import autojob
from brickops.databricks.context import DbContext


@pytest.fixture
def db_context() -> DbContext:
    return DbContext(
        api_token="token",  # noqa: S106
        api_url="url",
        notebook_path="/Users/john.erik.sloper@vlfk.no/dp-notebooks/domains/test/projects/project/flows/flow/testflow/deploy.py",
        username="TestUser",
    )


@pytest.mark.parametrize("env", ["invalid", "dev"])
def test_that_autojob_throws_with_invalid_env(
    env: str, db_context, monkeypatch
) -> None:
    monkeypatch.setattr(
        "brickops.dataops.deploy.autojob.get_context", lambda: db_context
    )
    with pytest.raises(ValueError):
        autojob(env=env)
