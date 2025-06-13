import requests, base64, datetime
import pandas as pd

"""
Connect to the Netilion Hub 
"""


class hub_connector:
    """
    Connects to the Netilion Hub and provides methods to interact with it.
    """
    
    staging_URL = "https://api.staging-env.netilion.endress.com/v1/"
    production_URL = "https://api.netilion.endress.com/v1/"
    production_india_URL = "https://in.api.netilion.endress.com/v1/"

    oauth_production_URL = "https://api.netilion.endress.com/oauth/token"
    oauth_production_india_URL = "https://in.api.netilion.endress.com/oauth/token"
    oauth_staging_URL = "https://api.staging-env.netilion.endress.com/oauth/token"

    def __init__(self, credential=None,
                 error_pass_through=False,
                 verbose=False):
        if credential is None:
            raise ValueError("credential must not be None")

        self.api_key = credential.api_key
        self.api_secret = credential.api_secret
        self.username = credential.user
        self.pwd = credential.pwd
        self.error_pass_through = error_pass_through
        self.verbose = verbose

        # Determine which environment to use
        production_region = getattr(credential, 'production_region', None)
        if production_region == "Staging":
            self.hub_URL = self.staging_URL
            self.oauth_URL = self.oauth_staging_URL
        elif production_region == "Global":
            self.hub_URL = self.production_URL
            self.oauth_URL = self.oauth_production_URL
        elif production_region == "India":
            self.hub_URL = self.production_india_URL
            self.oauth_URL = self.oauth_production_india_URL
        else:
            raise ValueError("production_region must be None, 'Global', or 'India'")

        self.bearer_token = None
        self.bearer_token_expires_at = None

        astr = self.username + ":" + self.pwd
        astr = astr.encode("ascii")
        self.auth_str = b"Basic " + base64.b64encode(astr)

    def ensure_oauth_token(self):

        payload = None

        if self.bearer_token is None:
            payload = {"client_id": self.api_key,
                       "client_secret": self.api_secret,
                       "grant_type": "password",
                       "username": self.username,
                       "password": self.pwd}
        elif self.bearer_token_expires_at <= datetime.datetime.now():
            payload = {"client_id": self.api_key,
                       "client_secret": self.api_secret,
                       "grant_type": "refresh_token",
                       "refresh_token": self.bearer_token["refresh_token"]
                       }

        if payload is not None:
            response = requests.post(self.oauth_URL, data=payload)
            response.raise_for_status()

            self.bearer_token = response.json()
            bt_ts = self.bearer_token["created_at"] + self.bearer_token["expires_in"] - 60  # 60 secs tolerance
            self.bearer_token_expires_at = datetime.datetime.fromtimestamp(bt_ts)

    def call_hub(self,  cmd='', verb='GET', params='', payload='', fullCMD=False):
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'api-key': self.api_key,
            'Cache-Control': "no-cache"
        }

        if self.api_secret is not None:
            # oauth2
            self.ensure_oauth_token()
            headers['Authorization'] = self.bearer_token["token_type"] + " " + self.bearer_token["access_token"]
        else:
            # basic auth
            headers['Authorization'] = self.auth_str

        if not fullCMD:
            cmd = self.hub_URL + cmd

        if self.verbose:
            print("Calling LCM Hub with " + verb + " " + cmd)
            if params:
                print("Params: " + str(params))
            if payload:
                print("Payload: " + str(payload))
                
        response = requests.request(verb, cmd, headers=headers, params=params, data=payload)
        if response.content == b'':
            # in case of DELETE nothing is returned and response.json() fails
            jresponse = None
        else:
            jresponse = response.json()
            if not self.error_pass_through:
                err = jresponse.get("errors")
                if err is not None:
                    raise Exception("LCMHUB Error: " + str(err))

        return jresponse

    def call_hub_pagination(self, cmd='', next_key=''):
        """
        Call the LCM Hub with pagination support.
        cmd: the command to call, including the base URL
        next_key: the key to use for pagination (e.g., "next")
        """
        if not cmd.startswith(self.hub_URL):
            cmd = self.hub_URL + cmd

        next_url = cmd
        all_results = []

        while next_url is not None:
            response = self.call_hub(cmd=next_url, fullCMD=True)
            all_results.extend(response.get(next_key) or [])

            # Check for pagination
            next_url = response.get("pagination", {}).get(next_key)

        return all_results
    

# Data class for credentials
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class credential:
    """
    Data class for storing credentials to connect to the Netilion Hub.
    Attributes:
        user (str): Username for authentication.
        pwd (str): Password for authentication.
        api_key (str): API key for authentication.
        api_secret (Optional[str]): API secret for OAuth2 authentication, if applicable.
    """
    user: str
    pwd: str
    api_key: str
    api_secret: Optional[str] = field(default=None)
    production_region: Optional[str] = field(default=None)  # staging, "global", or "india"

    def __post_init__(self):
        if not isinstance(self.user, str) or self.user is None:
            raise ValueError("user must be a non-None string")
        if not isinstance(self.pwd, str) or self.pwd is None:
            raise ValueError("pwd must be a non-None string")
        if not isinstance(self.api_key, str) or self.api_key is None:
            raise ValueError("api_key must be a non-None string")
        if self.api_secret is not None and not isinstance(self.api_secret, str):
            raise ValueError("api_secret must be a string or None")
        if self.production_region is not None and self.production_region not in ("Staging", "Global", "India"):
            raise ValueError("production_region must be one of: Staging, Global, India")

        # check if we can authenticate with the provided credentials
        try:
            hub = hub_connector(credential=self)
            hub.call_hub(cmd="assets") # can we list assets?
        except Exception as e:
            raise ValueError(f"Authentication failed: {str(e)}") from e
