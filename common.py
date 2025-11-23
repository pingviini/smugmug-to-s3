import os
from rauth import OAuth1Service
import sys
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

OAUTH_ORIGIN = 'https://secure.smugmug.com'
REQUEST_TOKEN_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/getRequestToken'
ACCESS_TOKEN_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/getAccessToken'
AUTHORIZE_URL = OAUTH_ORIGIN + '/services/oauth/1.0a/authorize'

API_ORIGIN = 'https://api.smugmug.com'

SERVICE = None


def get_service():
    """Get OAuth1Service using credentials from environment variables."""
    global SERVICE
    if SERVICE is None:
        consumer_key = os.getenv('SMUGMUG_OAUTH_CONSUMER_KEY')
        consumer_secret = os.getenv('SMUGMUG_OAUTH_CONSUMER_SECRET')

        if not consumer_key or not consumer_secret:
            print('====================================================')
            print('Failed to load OAuth credentials from .env file!')
            print('Please set SMUGMUG_OAUTH_CONSUMER_KEY and')
            print('SMUGMUG_OAUTH_CONSUMER_SECRET in your .env file.')
            print('====================================================')
            sys.exit(1)

        SERVICE = OAuth1Service(
                name='smugmug-oauth-web-demo',
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                request_token_url=REQUEST_TOKEN_URL,
                access_token_url=ACCESS_TOKEN_URL,
                authorize_url=AUTHORIZE_URL,
                base_url=API_ORIGIN + '/api/v2')
    return SERVICE


def add_auth_params(auth_url, access=None, permissions=None):
    """Add access and permissions parameters to an OAuth authorization URL."""
    if access is None and permissions is None:
        return auth_url
    parts = urlsplit(auth_url)
    query = parse_qsl(parts.query, True)
    if access is not None:
        query.append(('Access', access))
    if permissions is not None:
        query.append(('Permissions', permissions))
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(query, True),
        parts.fragment))

