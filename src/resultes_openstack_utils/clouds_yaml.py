import collections.abc as _cabc
import pathlib as _pl

import pydantic as _pyd
import yaml as _yaml

DEFAULT_CLOUD_YAML_CONTAINING_DIR_PATH = _pl.Path(__file__).parents[3] / "config"


def get_clouds_yaml_file_path(
    name: str = "clouds.yaml",
    containing_dir_path: _pl.Path = DEFAULT_CLOUD_YAML_CONTAINING_DIR_PATH,
) -> _pl.Path:
    return containing_dir_path / name


def get_clouds_yaml_openstack_json(
    clouds_file_path: _pl.Path,
) -> _cabc.Mapping[str, _pyd.JsonValue]:
    with clouds_file_path.open() as stream:
        data = _yaml.safe_load(stream)

    openstack = data["clouds"]["openstack"]

    return openstack
