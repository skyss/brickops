from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from requests.exceptions import RequestException

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any, ParamSpec, TypeVar

    Param = ParamSpec("Param")
    RetType = TypeVar("RetType")

from functools import wraps

logger = logging.getLogger(__name__)


# This provides a common error handling decorator for the API client methods.
# The nested decorator pattern is used to allow the error_handling decorator to
# accept arguments.
def error_handling(
    method: str,
) -> Callable[[Callable[Param, RetType]], Callable[Param, RetType]]:
    def decorator(
        func: Callable[Param, RetType],
    ) -> Callable[Param, RetType]:
        @wraps(func)
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> RetType:
            try:
                return func(*args, **kwargs)
            except RequestException as err:
                msg = f"Api error while making {method} call:"
                if err.response is not None:
                    msg += f" {err.response.text}"
                logger.error(msg)

                raise ApiClientError(message=msg) from err

        return wrapper

    return decorator


class ApiClientError(Exception):
    """Custom exception for API client errors."""

    def __init__(self: ApiClientError, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class ApiClient:
    """Wrapper for databricks API."""

    def __init__(self: ApiClient, host: str, token: str) -> None:
        self.api_host = host
        self.api_token = token
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def get_job_by_name(self: ApiClient, job_name: str) -> dict[str, Any] | None:
        result = self.get("jobs/list", params={"name": job_name})
        jobs: list[dict[str, Any]] = result.get("jobs", [])
        if jobs is None or len(jobs) == 0:
            return None
        return jobs[0]

    def get_jobs(self: ApiClient) -> list[dict[str, Any]]:
        result = self.get("jobs/list", version="2.2")
        job_list: list[dict[str, Any]] = result.get("jobs", [])
        while next_page_token := result.get("next_page_token"):
            result = self.get(
                "jobs/list", version="2.2", params={"page_token": next_page_token}
            )
            job_list.extend(result.get("jobs", []))
        return job_list

    def delete_job(self: ApiClient, job_id: str) -> dict[str, Any]:
        return self.post("jobs/delete", payload={"job_id": job_id})

    def get_catalogs(self: ApiClient) -> list[dict[str, Any]]:
        return self.get("unity-catalog/catalogs").get("catalogs", [])  # type: ignore [no-any-return]

    def get_schemas(self: ApiClient, catalog: str) -> list[dict[str, Any]]:
        return self.get("unity-catalog/schemas", params={"catalog_name": catalog}).get(  # type: ignore [no-any-return]
            "schemas", []
        )

    def get_volumes(self: ApiClient, catalog: str, schema: str) -> list[dict[str, Any]]:
        return self.get(
            "unity-catalog/volumes",
            params={"catalog_name": catalog, "schema_name": schema},
        ).get("volumes", [])

    def delete_schema(self: ApiClient, full_name: str) -> dict[str, Any]:
        return self.delete(f"unity-catalog/schemas/{full_name}")

    def delete_volume(self: ApiClient, full_name: str) -> dict[str, Any]:
        return self.delete(f"unity-catalog/volumes/{full_name}")

    def get_tables(self: ApiClient, catalog: str, schema: str) -> list[dict[str, Any]]:
        return self.get(  # type: ignore [no-any-return]
            "unity-catalog/tables",
            params={"catalog_name": catalog, "schema_name": schema},
        ).get("tables", [])

    def get_dashboards(self: ApiClient) -> list[dict[str, Any]]:
        return self.get(  # type: ignore [no-any-return]
            "lakeview/dashboards", version="2.0", params={"page_size": "1000"}
        ).get("dashboards", [])

    def patch_permissions(
        self: ApiClient,
        request_object_type: str,
        request_object_id: str,
        permission_principals: dict[str, str],
        permission_level: str,
    ) -> dict[str, Any]:
        payload = {
            "access_control_list": [
                {
                    "permission_level": permission_level,
                }
            ]
        }

        acl = payload["access_control_list"][0]
        acl.update(permission_principals)
        return self.patch(
            f"permissions/{request_object_type}/{request_object_id}",
            payload,
            version="2.0",
        )

    def get_job_permissions(self: ApiClient, job_id: str) -> dict[str, Any]:
        return self.get(stub=f"permissions/jobs/{job_id}", version="2.0")

    def delete_table(self: ApiClient, full_name: str) -> dict[str, Any]:
        return self.delete(f"unity-catalog/tables/{full_name}")

    def run_now(self: ApiClient, job_id: str) -> dict[str, Any]:
        logger.info(f"Running job: {job_id}")
        return self.post("jobs/run-now", payload={"job_id": job_id})

    def reset_job(
        self: ApiClient, *, job_id: str, job_name: str, job_config: dict[str, Any]
    ) -> dict[str, Any]:
        logger.info(f"Resetting job: {job_name}")
        data = {"job_id": job_id, "new_settings": job_config}
        return self.post("jobs/reset", version="2.2", payload=data)

    def create_job(
        self: ApiClient, job_name: str, job_config: dict[str, Any]
    ) -> dict[str, Any]:
        logger.info(f"Creating job: {job_name}")
        return self.post("jobs/create", version="2.2", payload=job_config)

    def get_clusters(self: ApiClient) -> list[dict[str, Any]]:
        response = self.get("clusters/list")
        return response["clusters"]  # type: ignore [no-any-return]

    def get_workspace_status(self: ApiClient, path: str) -> dict[str, Any]:
        return self.get("workspace/get-status", version="2.0", params={"path": path})

    def get_repo(self: ApiClient, repo_id: str) -> dict[str, Any]:
        return self.get(f"repos/{repo_id}", version="2.0")

    def get_repos(self: ApiClient) -> list[dict[str, Any]]:
        repos_response = self.get(
            "repos", version="2.0", params={"path_prefix": "/Repos"}
        )
        folders_response = self.get(
            "repos", version="2.0", params={"path_prefix": "/Users"}
        )
        return repos_response.get("repos", []) + folders_response.get("repos", [])  # type: ignore [no-any-return]

    def unpack_response(self: ApiClient, response: requests.Response) -> dict[str, Any]:
        response.raise_for_status()
        response_json = response.json()
        logger.debug(f"Api response: {response_json}")
        return response_json  # type: ignore [no-any-return]

    def build_url(self: ApiClient, stub: str, version: str = "2.1") -> str:
        return f"{self.api_host}/api/{version}/{stub}"

    def handle_errors(
        self: ApiClient, func: Callable[[], dict[str, Any]], method: str
    ) -> dict[str, Any]:
        try:
            return func()
        except RequestException as err:
            msg = f"Api error while making {method} call:"
            if err.response is not None:
                msg += f" {err.response.text}"
            logger.error(msg)

            raise ApiClientError(message=msg) from err

    @error_handling("POST")
    def post(
        self: ApiClient,
        stub: str,
        version: str = "2.1",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        logger.debug(f"POST {self.build_url(stub, version)}")
        return self.unpack_response(
            requests.post(
                url=self.build_url(stub, version),
                headers=self.headers,
                json=payload,
                timeout=10,
            )
        )

    @error_handling("DELETE")
    def delete(
        self: ApiClient,
        stub: str,
        version: str = "2.1",
    ) -> dict[str, Any]:
        logger.debug(f"DELETE {self.build_url(stub, version)}")
        return self.unpack_response(
            requests.delete(
                self.build_url(stub, version),
                headers=self.headers,
                timeout=10,
            )
        )

    @error_handling("GET")
    def get(
        self: ApiClient,
        stub: str,
        version: str = "2.1",
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        logger.debug(f"GET {self.build_url(stub, version)}")
        return self.unpack_response(
            requests.get(
                self.build_url(stub, version),
                headers=self.headers,
                params=params,
                timeout=10,
            )
        )

    @error_handling("PUT")
    def put(
        self: ApiClient, stub: str, payload: dict[str, Any], version: str = "2.1"
    ) -> dict[str, Any]:
        logger.debug(f"PUT {self.build_url(stub, version)}")
        return self.unpack_response(
            requests.put(
                url=self.build_url(stub, version),
                headers=self.headers,
                json=payload,
                timeout=10,
            )
        )

    @error_handling("PATCH")
    def patch(
        self: ApiClient, stub: str, payload: dict[str, Any], version: str = "2.1"
    ) -> dict[str, Any]:
        logger.debug(f"PATCH {self.build_url(stub, version)}")
        return self.unpack_response(
            requests.patch(
                url=self.build_url(stub, version),
                headers=self.headers,
                json=payload,
                timeout=10,
            )
        )
