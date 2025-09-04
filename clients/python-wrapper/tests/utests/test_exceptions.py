import http
import json

from lakefs_sdk import ApiException
from lakefs.exceptions import api_exception_handler, ServerException


class TestException(ApiException):
    __test__ = False  # Not a test case
    def __init__(self, status: int, reason: str, body: str):
        super().__init__()
        self.status = status
        self.reason = reason
        self.body = body

def test_http_error_list():
    expected_reason = "my reason"
    body = json.dumps(['foo', {'bar': ('baz', None, 1.0, 2)}])
    resp =  TestException(http.HTTPStatus.FORBIDDEN.value, expected_reason, body)
    
    try:
        with api_exception_handler():
            raise resp
    except ServerException as e:
        assert expected_reason == e.reason
        assert e.body is not None
        actual_body = json.dumps(e.body)
        assert actual_body == body
        return
    
    # make sure exception is not swallowed
    assert False, "Exception not caught"

def test_http_error_xml():
    expected_reason = ""
    body = '''<note>
<to>Tove</to>
<from>Jani</from>
<heading>Reminder</heading>
<body>Don't forget me this weekend!</body>
</note>
    '''
    resp =  TestException(http.HTTPStatus.FORBIDDEN.value, expected_reason, body)

    try:
        with api_exception_handler():
            raise resp
    except ServerException as e:
        assert expected_reason == e.reason
        assert e.body is not None
        assert not e.body # Body should be empty
        return
    
    # make sure exception is not swallowed
    assert False, "Exception not caught"