import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import sys
from lakefs_sdk import AuthenticationToken
from pydantic import StrictInt, StrictStr

# Mock boto3 if it's not installed
try:
    import boto3
except ImportError:
    boto3 = MagicMock()
    sys.modules['boto3'] = boto3

from lakefs.config import ClientConfig
from lakefs.exceptions import NoAuthenticationFound
from tests.utests.common import (
    lakectl_test_config_context,
    lakectl_no_config_context,
    env_var_context,
    expect_exception_context,
    TEST_SERVER, TEST_ENDPOINT_PATH,
)

MOCK_PRESIGNED_URL = """
https://my-bucket.s3.amazonaws.com/my-object.txt?
Action=GetObject&
Version=2012-10-17&
X-Amz-Algorithm=AWS4-HMAC-SHA256&
X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20250506%2Fus-east-1%2Fs3%2Faws4_request&
X-Amz-Date=20250506T114100Z&
X-Amz-Expires=3600&
X-Amz-SignedHeaders=host&
X-Amz-Signature=abcd1234example5678
"""

class TestAuthenticationFlow:
    """Test suite for authentication flow in lakeFS client"""

    def test_client_auth_type_detection(self, monkeypatch, tmp_path):
        """Test that client correctly detects authentication type"""
        with lakectl_test_config_context(monkeypatch, tmp_path) as client:
            # Test with credentials
            clt = client.Client(username="user", password="pass")
            assert clt.config.get_auth_type() == ClientConfig.AuthType.CREDENTIALS

            # Test with session token
            clt = client.Client(access_token="token123")
            assert clt.config.get_auth_type() == ClientConfig.AuthType.SESSION_TOKEN

            # Test with IAM provider
            iam_provider = ClientConfig.IAMProvider(
                type=ClientConfig.ProviderType.AWS_IAM,
                aws_iam=ClientConfig.AWSIAMProviderConfig(
                    token_ttl_seconds=3600,
                    url_presign_ttl_seconds=60,
                    token_request_headers=None
                )
            )
            clt = client.Client()
            clt.config._iam_provider = iam_provider
            assert clt.config.get_auth_type() == ClientConfig.AuthType.IAM

    @patch('lakefs.auth.access_token_from_aws_iam_role')
    def test_client_iam_auth_initialization(self, mock_access_token, monkeypatch, tmp_path):
        """Test client initialization with IAM auth"""
        # Mock response from access_token_from_aws_iam_role
        mock_token = "iam_token_123"
        reset_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        mock_access_token.return_value = (mock_token, reset_time)

        with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
            # Set up IAM provider configuration
            with patch.object(ClientConfig, 'get_auth_type', return_value=ClientConfig.AuthType.IAM):
                with patch('lakefs.client.ClientConfig.iam_provider', new_callable=PropertyMock) as mock_iam_provider:
                    mock_iam_provider.return_value = ClientConfig.IAMProvider(
                        type=ClientConfig.ProviderType.AWS_IAM,
                        aws_iam=ClientConfig.AWSIAMProviderConfig(
                            token_ttl_seconds=3600,
                            url_presign_ttl_seconds=60,
                            token_request_headers=None
                        )
                    )
                    with patch('boto3.Session'):
                        # Initialize client with IAM authentication
                        clt = client_module.Client(host=TEST_SERVER)

                        # Verify that access_token_from_aws_iam_role was called
                        mock_access_token.assert_called_once()

                        # Verify token was set properly
                        assert clt._conf.access_token == mock_token
                        assert clt._reset_token_time == reset_time

    @patch('lakefs.auth.access_token_from_aws_iam_role')
    def test_token_refresh_on_expiry(self, mock_access_token, monkeypatch, tmp_path):
        """Test token refresh when token expires"""
        # First token and expiry time
        initial_token = "initial_token"
        initial_expiry = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)  # Already expired

        # Second token and expiry time (for refresh)
        refreshed_token = "refreshed_token"
        refreshed_expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

        # Mock to return different values on successive calls
        mock_access_token.side_effect = [(initial_token, initial_expiry), (refreshed_token, refreshed_expiry)]

        with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
            # Set up for IAM provider
            with patch.object(ClientConfig, 'get_auth_type', return_value=ClientConfig.AuthType.IAM):
                with patch('lakefs.client.ClientConfig.iam_provider', new_callable=PropertyMock) as mock_iam_provider:
                    mock_iam_provider.return_value = ClientConfig.IAMProvider(
                        type=ClientConfig.ProviderType.AWS_IAM,
                        aws_iam=ClientConfig.AWSIAMProviderConfig(
                            token_ttl_seconds=3600,
                            url_presign_ttl_seconds=60,
                            token_request_headers=None
                        )
                    )
                    with patch('boto3.Session'):
                        # Initialize client
                        clt = client_module.Client(host=TEST_SERVER)
                        # First token setup
                        assert clt.config.access_token == initial_token
                        assert clt._reset_token_time == initial_expiry

                        # This should trigger a token refresh since the token is expired
                        _ = clt.sdk_client

                        # Verify token was refreshed
                        assert clt.config.access_token == refreshed_token
                        assert clt._reset_token_time == refreshed_expiry

    def test_no_token_refresh_for_non_iam_auth(self, monkeypatch, tmp_path):
        """Test that token refresh is not triggered for non-IAM auth methods"""
        with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
            # Create client with session token auth
            with patch.object(ClientConfig, 'get_auth_type', return_value=ClientConfig.AuthType.SESSION_TOKEN):
                clt = client_module.Client(access_token="static_token")

                # Mock _reset_token_time to appear expired
                expired_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
                clt._reset_token_time = expired_time

                # This should NOT trigger a refresh for SESSION_TOKEN auth
                with patch('lakefs.auth.access_token_from_aws_iam_role') as mock_access_token:
                    _ = clt.sdk_client
                    mock_access_token.assert_not_called()

    @patch('lakefs.auth.access_token_from_aws_iam_role')
    @patch('botocore.signers.RequestSigner.generate_presigned_url')
    def test_from_aws_role_factory_method(self, mock_request_signer, mock_access_token, monkeypatch, tmp_path):
        """Test the from_aws_role factory method for creating client"""
        with patch('lakefs_sdk.api.auth_api.AuthApi.external_principal_login') as mock_external_principal_login:
            test_token = "aws_role_token"
            test_reset_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
            mock_access_token.return_value = (test_token, test_reset_time)

            mock_request_signer.return_value = MOCK_PRESIGNED_URL
            mock_external_principal_login.return_value = (
                AuthenticationToken(token=StrictStr("test_token"), token_expiration=StrictInt(1))
            )
            with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
                # Create mock session
                mock_session = MagicMock(spec=boto3.Session)

                # Create client using factory method
                clt = client_module.from_aws_role(
                    session=mock_session,
                    ttl_seconds=7200,  # Custom TTL
                    presigned_ttl=120,  # Custom presigned TTL
                    additional_headers={"X-Custom-Header": "value"},
                    host=TEST_SERVER
                )

                # Verify client was created with correct parameters
                mock_access_token.assert_called_once()
                args, kwargs = mock_access_token.call_args

                # Verify the AWS provider config was passed correctly
                aws_provider = kwargs.get('aws_provider_auth_params', None)
                if not aws_provider:
                    aws_provider = args[3]  # Fallback to positional args

                assert aws_provider.token_ttl_seconds == 7200
                assert aws_provider.url_presign_ttl_seconds == 120
                assert aws_provider.token_request_headers == {"X-Custom-Header": "value"}

                # Verify token was set in client
                assert clt.config.access_token == test_token
                assert clt._reset_token_time == test_reset_time

    def test_environment_variables_auth(self, monkeypatch):
        """Test authentication using environment variables"""
        with lakectl_no_config_context(monkeypatch):
            with env_var_context():
                # Set environment variables
                monkeypatch.setenv("LAKECTL_SERVER_ENDPOINT_URL", TEST_SERVER)
                monkeypatch.setenv("LAKECTL_CREDENTIALS_ACCESS_KEY_ID", "env_access_key")
                monkeypatch.setenv("LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY", "env_secret_key")

                from lakefs import client
                # Create client using environment vars
                clt = client.Client()

                # Verify credentials from environment
                assert clt.config.host == TEST_SERVER + TEST_ENDPOINT_PATH
                assert clt.config.username == "env_access_key"
                assert clt.config.password == "env_secret_key"
                assert clt.config.get_auth_type() == ClientConfig.AuthType.CREDENTIALS

    def test_environment_variables_with_session_token(self, monkeypatch):
        """Test authentication using environment variables with session token"""
        with lakectl_no_config_context(monkeypatch):
            with env_var_context():
                # Set environment variables
                monkeypatch.setenv("LAKECTL_SERVER_ENDPOINT_URL", TEST_SERVER)
                monkeypatch.setenv("LAKECTL_CREDENTIALS_SESSION_TOKEN", "env_session_token")

                from lakefs import client
                # Create client using environment vars
                clt = client.Client()

                # Verify session token from environment
                assert clt.config.host == TEST_SERVER + TEST_ENDPOINT_PATH
                assert clt.config.access_token == "env_session_token"
                assert clt.config.get_auth_type() == ClientConfig.AuthType.SESSION_TOKEN

    @patch('botocore.signers.RequestSigner.generate_presigned_url')
    def test_environment_variables_iam_provider(self, mock_request_signer, monkeypatch):
        """Test authentication using environment variables with IAM provider"""
        # Clear any cached modules or state
        if 'lakefs.client' in sys.modules:
            monkeypatch.delattr(sys.modules['lakefs.client'], '_client_instance', raising=False)
        mock_request_signer.return_value = MOCK_PRESIGNED_URL
        # Use context manager for all patches to ensure proper cleanup
        with lakectl_no_config_context(monkeypatch):
            with env_var_context():
                # Set environment variables with properly formatted data
                monkeypatch.setenv("LAKECTL_SERVER_ENDPOINT_URL", TEST_SERVER)
                monkeypatch.setenv("LAKECTL_CREDENTIALS_PROVIDER_TYPE", "aws_iam")
                monkeypatch.setenv("LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS", "7200")
                monkeypatch.setenv("LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_URL_PRESIGN_TTL_SECONDS", "60")

                # Create independent patches with context managers
                with patch('lakefs_sdk.api.auth_api.AuthApi.external_principal_login') as mock_external_principal_login:
                    # Set up mock return value
                    test_token = "env_iam_token"
                    test_reset_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
                    mock_external_principal_login.return_value = AuthenticationToken(
                        token=StrictStr(test_token),
                        token_expiration=StrictInt(int(test_reset_time.timestamp()))
                    )

                    # Patch boto3 before importing client
                    with patch('boto3.Session'):
                        # Dynamic import to ensure we get a fresh instance
                        import importlib
                        import lakefs.client
                        importlib.reload(lakefs.client)
                        from lakefs.client import Client

                        # Create client - should trigger external_principal_login
                        clt = Client()

                        # Verify the mock was called
                        mock_external_principal_login.assert_called_once()

                        # Verify the token was set
                        assert clt.config.access_token == test_token

    @patch('lakefs.auth.access_token_from_aws_iam_role')
    def test_token_request_headers(self, mock_access_token, monkeypatch, tmp_path):
        """Test that token request headers are passed correctly"""
        test_token = "token_with_headers"
        test_reset_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        mock_access_token.return_value = (test_token, test_reset_time)

        with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
            # Custom headers
            headers = {"X-Custom-1": "value1", "X-Custom-2": "value2"}

            # Create client using factory method
            mock_session = MagicMock(spec=boto3.Session)
            client_module.from_aws_role(
                session=mock_session,
                additional_headers=headers,
                host=TEST_SERVER
            )

            # Verify headers were passed to access_token_from_aws_iam_role
            mock_access_token.assert_called_once()
            args, kwargs = mock_access_token.call_args

            # Check provider config contains headers
            aws_provider = kwargs.get('aws_provider_auth_params', None)
            if not aws_provider:
                aws_provider = args[3]  # Fallback to positional args

            assert aws_provider.token_request_headers == headers

    def test_handling_missing_authentication(self, monkeypatch):
        """Test proper error handling when no authentication method is available"""
        with lakectl_no_config_context(monkeypatch):
            with env_var_context():
                # No environment variables set

                from lakefs import client
                # Attempt to create client without authentication
                with expect_exception_context(NoAuthenticationFound):
                    client.Client()

    def test_region_extraction(self, monkeypatch):
        """Test region extraction from STS endpoint"""
        from lakefs.auth import _extract_region_from_endpoint

        # Test standard regional endpoint
        assert _extract_region_from_endpoint("https://sts.eu-central-1.amazonaws.com/") == "eu-central-1"

        # Test global endpoint
        assert _extract_region_from_endpoint("https://sts.amazonaws.com/") == "us-east-1"

        # Test custom endpoint with more segments
        assert _extract_region_from_endpoint("https://prefix.sts.us-west-2.amazonaws.com/") == "us-west-2"

    @patch('lakefs.client.access_token_from_aws_iam_role')
    def test_from_web_identity(self, mock_access_token, monkeypatch, tmp_path):
        """Test creating client from web identity token"""
        with lakectl_test_config_context(monkeypatch, tmp_path) as client_module:
            # Mock web identity login
            mock_client = MagicMock()
            mock_auth_token = MagicMock()
            mock_auth_token.token = "web_identity_token"
            mock_client.experimental_api.sts_login.return_value = mock_auth_token

            # Mock Client creation
            with patch.object(client_module, 'Client') as mock_client_class:
                mock_client_instance = MagicMock()
                mock_client_instance.sdk_client = mock_client
                mock_client_class.return_value = mock_client_instance

                # Call from_web_identity
                client_module.from_web_identity(
                    code="auth_code",
                    state="state_token",
                    redirect_uri="https://redirect.example.com",
                    ttl_seconds=3600,
                    host=TEST_SERVER
                )

                # Verify sts_login was called with correct parameters
                mock_client.experimental_api.sts_login.assert_called_once()
                args, _ = mock_client.experimental_api.sts_login.call_args
                sts_request = args[0]
                assert sts_request.code == "auth_code"
                assert sts_request.state == "state_token"
                assert sts_request.redirect_uri == "https://redirect.example.com"
                assert sts_request.ttl_seconds == 3600

                # Verify token was set in client
                assert mock_client_instance.config.access_token == "web_identity_token"
