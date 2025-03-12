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

## How to get into devcontainer

```
make start-devcontainer
make devcontainer-shell
```

## Configuration options for naming and mesh levels

Mesh levels refers here to the granularity/depth of the repo structure, e.g. organization,domain,project,flow

Default prefixing of data sets are:

```
    return os.environ.get("BRICKOPS_MESH_CATALOG_LEVELS", "domain")

...

    return os.environ.get("BRICKOPS_MESH_JOBPREFIX_LEVELS", "domain,project,flow")
```

You can override with the env vars above.

For catalogs this means the domain section of a path is used, for jobs a combination of domain,project,flow.
You can also specify org, if wanted. Example:


* In the following notebook: `something/domains/sanntid/projects/testproject/flows/testflow/test_notebook`
  * By default:
    * catalog name: `sanntid`
    * job name: `sanntid_testproject_testflow`
  * With `BRICKOPS_MESH_CATALOG_LEVELS` = `domain,project`
    * catalog name: `sanntid_testproject`
* With org support, in the following notebook: `/Repos/test@foobar.foo/dataplatform/something/org/acme/domains/sanntid/projects/testproject/flows/testflow/test_notebook`
  * By default:
    * catalog name: `sanntid`
    * job name: `sanntid_testproject_testflow`
  * With `BRICKOPS_MESH_CATALOG_LEVELS` = `org,domain,project`
    * catalog name: `acme_sanntid_testproject`
