import resultes_openstack_utils.keystone as _ks


def test_create_and_invalidate_session_using_application_credential() -> None:
    auth = _ks.create_application_credential()
    with _ks.create_session(auth):
        pass


def test_create_and_invalidate_session_using_password() -> None:
    auth = _ks.create_password()
    with _ks.create_session(auth):
        pass
