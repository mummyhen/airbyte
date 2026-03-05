#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass, InitVar
from typing import Any, Mapping, Optional, MutableMapping, Union, Callable

import boto3
import datetime as dt
import logging
import requests

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.requesters.requester import Requester, HttpMethod
from airbyte_cdk.sources.declarative.types import Config, StreamSlice, StreamState


@dataclass
class Boto3Authenticator(DeclarativeAuthenticator):
    """
    Authenticator that uses boto3 to generate temporary credentials for AWS services. It supports assuming a role via STS if a role ARN is
    provided in the configuration.
    """
    config: Config

    _session: boto3.Session

    def __post_init__(self):
        _session = self._assume_role_session(self.config)

    @staticmethod
    def _assume_role_session(config: Mapping[str, Any]) -> boto3.Session:
        """
        Uses STS to assume the role specified in config['role_arn']
        """
        base_session = boto3.Session(
            region_name=config["region_name"],
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
        )

        if not config.get("role_arn"):
            return base_session

        sts_client = base_session.client("sts")
        assumed_role = sts_client.assume_role(
            RoleArn=config["role_arn"],
            RoleSessionName="airbyte-cloudwatch-session",
            DurationSeconds=config.get("role_session_duration", 3600),
        )

        credentials = assumed_role["Credentials"]
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=config["region_name"],
        )
        return session

    @property
    def auth_header(self) -> str:
        return "Authorization"

    @property
    def token(self) -> str:
        return "Token"


@dataclass
class Boto3LogRequester(Requester):
    region_name: str
    log_group_name: str
    name: str
    authenticator: Boto3Authenticator
    parameters: InitVar[Mapping[str, Any]]

    # Use self.logger in subclasses to log any messages

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._session = self.authenticator._session
        self._client = self._session.client("logs")
        self._name = self.name
        self._parameters = parameters

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"airbyte.Boto3Requester")

    def get_authenticator(self) -> DeclarativeAuthenticator:
        return self.authenticator

    def get_url(
        self,
        *,
        stream_state: Optional[StreamState],
        stream_slice: Optional[StreamSlice],
        next_page_token: Optional[Mapping[str, Any]],
    ) -> str:
        return ""

    def get_url_base(
        self,
        *,
        stream_state: Optional[StreamState],
        stream_slice: Optional[StreamSlice],
        next_page_token: Optional[Mapping[str, Any]],
    ) -> str:
        return ""

    def get_path(
        self,
        *,
        stream_state: Optional[StreamState],
        stream_slice: Optional[StreamSlice],
        next_page_token: Optional[Mapping[str, Any]],
    ) -> str:
        return ""

    def get_method(self) -> HttpMethod:
        """
        Specifies the HTTP method to use
        """
        return HttpMethod.POST

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        stream_slice = stream_slice or {}
        current_time = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)  # milliseconds
        start_time = stream_slice.get("start_time", 0)
        end_time = stream_slice.get("end_time", current_time)

        params = {
            "logGroupName": self.log_group_name,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 10000,
        }
        self.logger.info(f"Fetching log events with parameters: {params}")
        return params

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        return ""

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return {}

    def send_request(
        self,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        path: Optional[str] = None,
        request_headers: Optional[Mapping[str, Any]] = None,
        request_params: Optional[Mapping[str, Any]] = None,
        request_body_data: Optional[Union[Mapping[str, Any], str]] = None,
        request_body_json: Optional[Mapping[str, Any]] = None,
        log_formatter: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        params = request_params or {}

        if next_page_token:
            params["nextToken"] = next_page_token

        response = self._client.filter_log_events(**params, **self._parameters)
        return response
