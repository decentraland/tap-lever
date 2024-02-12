"""REST client handling, including LeverStream base class."""

import requests
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from urllib.parse import unquote
import copy

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
            timestampDate = int(filterDate.timestamp() * 1000)  # Milliseconds
            params["updated_at_start"] = timestampDate
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        response_json = response.json()

        hasNext = response_json.get("hasNext", False)

        if hasNext == False:
            return None

        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)

            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        if next_page_token == previous_token:
            return None

        return next_page_token


def request_records(self, context: Optional[dict]) -> Iterable[dict]:
    next_page_token: Any = None
    finished = False
    decorated_request = self.request_decorator(self._request)

    while not finished:
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = decorated_request(prepared_request, context)
        for row in self.parse_response(resp):
            yield row
        previous_token = copy.deepcopy(next_page_token)
        next_page_token = self.get_next_page_token(
            response=resp, previous_token=previous_token
        )
        if next_page_token and next_page_token == previous_token:
            finished = True
            return

        finished = not next_page_token
