"""REST client handling, including LeverStream base class."""

import requests, json
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from urllib.parse import unquote

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class LeverStream(RESTStream):
    """Lever stream class."""

    url_base = "https://api.lever.co/v1"
    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.next"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self, username=self.config["api_key"], password=""
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            offsetVal = json.loads(unquote(next_page_token))
            params["updated_at_start"] = offsetVal[0]
        elif self.replication_key:
            filterDate = self.get_starting_timestamp(context)
            timestampDate = int(filterDate.timestamp() * 1000) # Milliseconds
            params["updated_at_start"] = timestampDate
        return params
