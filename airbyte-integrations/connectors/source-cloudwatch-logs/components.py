#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
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
class Boto3Requester(Requester):

    # Use self.logger in subclasses to log any messages
    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"airbyte.Boto3Requester")

    def get_authenticator(self) -> DeclarativeAuthenticator:
        return Boto3Authenticator

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
        current_time = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)  # milliseconds
        if stream_slice:
            start_time = stream_slice.get("start_time", 0)
            end_time = stream_slice.get("end_time", current_time)
        else:
            start_time = 0
            end_time = current_time
        return {
            "logGroupName": self.log_group_name,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 10000,
        }

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
        return self.read_records(
            sync_mode=SyncMode.incremental,
            cursor_field=self.cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state,
        )

    def read_records(
        self,
        stream_slice: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        stream_slice = stream_slice or {}

        start_time = stream_slice.get("start_time", 0)
        end_time = stream_slice.get("end_time")
        self._logger.info(f"Fetching logs from: {start_time} to {end_time} for group: {self.log_group_name}")

        next_token = None
        while True:
            params = {
                "logGroupName": self.log_group_name,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 10000,
            }
            self.logger.debug(f"Fetching log events with parameters: {params}")

            if next_token:
                params["nextToken"] = next_token

            response = self.client.filter_log_events(**params, **self.kwargs)
            events = response.get("events", [])

            for event in events:
                if self._cursor_value is None:
                    self._cursor_value = event[self.cursor_field]
                else:
                    self._cursor_value = max(event[self.cursor_field], self._cursor_value)
                yield event

            next_token = response.get("nextToken")
            if not next_token:
                break
