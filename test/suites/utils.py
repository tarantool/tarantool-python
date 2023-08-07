"""
Various test utilities.
"""


def assert_admin_success(resp):
    """
    Util to assert admin text request response.
    It is expected that request last line is `return true`.
    If something went wrong on executing, Tarantool throws an error
    which would be a part of return values.
    """

    assert isinstance(resp, list), f'got unexpected resp type: {type(resp)}'
    assert len(resp) > 0, 'got unexpected empty resp'
    assert resp[0] is True, f'got unexpected resp: {resp}'
