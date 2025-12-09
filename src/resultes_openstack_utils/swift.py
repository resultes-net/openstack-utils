import collections.abc as _cabc
import contextlib as _ctx
import pathlib as _pl

import resultes_openstack_utils.clouds_yaml as _cyaml
import resultes_openstack_utils.keystone as _ks
import resultes_pydantic_models.runner as _mrunner
import swiftclient.client as _sclient


type Headers = _cabc.Mapping[str, str]
type Chunks = _cabc.Iterable[bytes]

_CHUNK_SIZE = 8 * 1024


@_ctx.contextmanager
def create_connection(
    clouds_yaml_file_path: _pl.Path,
) -> _cabc.Iterator[_sclient.Connection]:
    data = _cyaml.get_clouds_yaml_openstack_json(clouds_yaml_file_path)
    os_options = {"region_name": data["region_name"]}

    auth = _ks.create_application_credential(clouds_yaml_file_path)

    with _ks.create_session(auth=auth) as session:
        connection = _sclient.Connection(session=session, os_options=os_options)
        yield connection
        connection.close()


def get_size_in_bytes(
    object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
    connection: _sclient.Connection,
) -> int:
    query_string = _create_query_string(object_storage_input_file_path)

    headers: _cabc.Mapping[str, str] = connection.head_object(
        object_storage_input_file_path.container,
        object_storage_input_file_path.path,
        query_string=query_string,
    )

    size_in_bytes = int(headers["Content-Length"])

    return size_in_bytes


def download_object_storage_chunks(
    object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
    connection: _sclient.Connection,
) -> tuple[Headers, Chunks]:
    query_string = _create_query_string(object_storage_input_file_path)

    return connection.get_object(
        object_storage_input_file_path.container,
        object_storage_input_file_path.path,
        resp_chunk_size=_CHUNK_SIZE,
        query_string=query_string,
    )


def _create_query_string(
    object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
) -> str | None:
    version = object_storage_input_file_path.version
    query_string = None if version is None else f"version={version}"
    return query_string


def upload_storage_object(
    input_file_path: _pl.Path,
    object_storage_path: _mrunner.ObjectStorageOutputFilePath,
    connection: _sclient.Connection,
) -> None:
    with input_file_path.open("br") as contents:
        connection.put_object(
            object_storage_path.container, object_storage_path.path, contents
        )
