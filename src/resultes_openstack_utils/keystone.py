import collections.abc as _cabc
import contextlib as _ctx
import os as _os
import pathlib as _pl

import keystoneauth1.identity.v3 as _kidv3
import keystoneauth1.session as _ksess
import resultes_openstack_utils.clouds_yaml as _cyaml


def create_application_credential(
    clouds_file_path: _pl.Path | None = None,
) -> _kidv3.ApplicationCredential:
    openstack = _cyaml.get_clouds_yaml_openstack_json(clouds_file_path)

    auth = openstack["auth"]

    application_credential = _kidv3.ApplicationCredential(**auth)

    return application_credential


def create_password(
    clouds_file_path: _pl.Path | None = None, os_password: str | None = None
) -> _kidv3.Password:
    openstack = _cyaml.get_clouds_yaml_openstack_json(clouds_file_path)

    auth = openstack["auth"]

    os_password = os_password if os_password else _os.environ["OS_PASSWORD"]

    auth["password"] = os_password

    password = _kidv3.Password(**auth)

    return password


def _create_session(auth: _kidv3.Auth) -> _ksess.Session:
    session = _ksess.Session(auth=auth)
    return session


@_ctx.contextmanager
def create_session(auth: _kidv3.Auth) -> _cabc.Iterator[_ksess.Session]:
    session = _create_session(auth)
    yield session
    session.invalidate()
