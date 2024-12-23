[![Code checks and tests](https://github.com/paalvibe/bricksops/actions/workflows/check-and-test.yaml/badge.svg)](https://github.com/paalvibe/bricksops/actions/workflows/check-and-test.yaml)
# bricksops
DataOps framework for Databricks

## Getting started
This project uses [uv](https://docs.astral.sh/uv/), to begin, follow the installation instructions for your platform on the project homepage.

Next, make sure you are in the project root and run the following command in the terminal:

```shell
uv sync
```

This will create a virtual environment and install the required packages in it.
The project configuration can be found in [`pyproject.toml`](./pyproject.toml).

You can now run the tests with
```shell
uv run pytest
```
