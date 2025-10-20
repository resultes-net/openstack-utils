import collections.abc as _cabc
import contextlib as _ctx
import pathlib as _pl

import resultes_openstack_utils.clouds_yaml as _cyaml
import resultes_openstack_utils.keystone as _ks
import resultes_pydantic_models.runner as _mrunner
import swiftclient.client as _sclient

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


def download_object_storage_chunks(
    object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
    connection: _sclient.Connection,
) -> _cabc.Iterable[bytes]:
    version = object_storage_input_file_path.version
    query_string = None if version is None else f"version={version}"

    _, chunks = connection.get_object(
        object_storage_input_file_path.container,
        object_storage_input_file_path.path,
        resp_chunk_size=_CHUNK_SIZE,
        query_string=query_string,
    )

    for chunk in chunks:
        yield chunk


def upload_storage_object(
    input_file_path: _pl.Path,
    object_storage_path: _mrunner.ObjectStorageOutputFilePath,
    connection: _sclient.Connection,
) -> None:
    with input_file_path.open("br") as contents:
        connection.put_object(
            object_storage_path.container, object_storage_path.path, contents
        )
