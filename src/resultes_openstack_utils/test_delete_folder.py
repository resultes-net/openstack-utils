import unittest.mock as _um

import pytest as _pt
import resultes_pydantic_models.runner as _mrunner

import resultes_openstack_utils.swift as _swift


def test_delete_folder() -> None:
    connection = _um.Mock()
    connection.get_container.return_value = (
        {},
        [{"name": "results/5e0a17c3d2/a.png"}, {"name": "results/5e0a17c3d2/b.log"}],
    )
    path = _mrunner.ObjectStorageInputFilePath(
        container="resultes-results", path="results/5e0a17c3d2/"
    )

    _swift.delete_folder(path, connection)

    connection.get_container.assert_called_once_with(
        "resultes-results", prefix="results/5e0a17c3d2/", full_listing=True
    )
    assert connection.delete_object.call_args_list == [
        _um.call("resultes-results", "results/5e0a17c3d2/a.png"),
        _um.call("resultes-results", "results/5e0a17c3d2/b.log"),
    ]


def test_delete_folder_requires_trailing_slash() -> None:
    path = _mrunner.ObjectStorageInputFilePath(
        container="resultes-results", path="results/5e0a17c3d2"
    )

    with _pt.raises(ValueError):
        _swift.delete_folder(path, _um.Mock())
