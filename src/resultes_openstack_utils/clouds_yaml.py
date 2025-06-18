import collections.abc as _cabc
import pathlib as _pl

import pydantic as _pyd
import yaml as _yaml

DEFAULT_CLOUDS_YAML_FILE_PATH = _pl.Path(__file__).parents[3] / "config" / "clouds.yaml"


def get_clouds_yaml_openstack_json(
    clouds_file_path: _pl.Path | None = None,
) -> _cabc.Mapping[str, _pyd.JsonValue]:
    clouds_file_path = clouds_file_path if clouds_file_path else DEFAULT_CLOUDS_YAML_FILE_PATH
    
    with clouds_file_path.open() as stream:
        data = _yaml.safe_load(stream)

    openstack = data["clouds"]["openstack"]

    return openstack
