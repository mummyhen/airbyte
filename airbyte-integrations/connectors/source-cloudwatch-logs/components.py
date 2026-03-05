#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# components.py – Custom declarative CDK components for source-cloudwatch-logs.
#
# These are wired into manifest.yaml via `type: CustomRetriever` /
# `type: CustomStreamPartitionRouter` so that the connector can keep using the
# boto3 AWS SDK while still being driven by the declarative framework.
#

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import boto3
from dateutil import parser as dateutil_parser

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.partition_routers.partition_router import PartitionRouter
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.pagination_strategy import (
    PaginationStrategy,
)
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.declarative.types import Config, Record, StreamSlice, StreamState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assume_role_session(config: Mapping[str, Any]) -> boto3.Session:
    """Return a boto3 Session, optionally assuming an IAM role."""
    base_session = boto3.Session(
        region_name=config["region_name"],
        aws_access_key_id=config.get("aws_access_key_id"),
        aws_secret_access_key=config.get("aws_secret_access_key"),
    )

    if not config.get("role_arn"):
        return base_session

    sts = base_session.client("sts")
    assumed = sts.assume_role(
        RoleArn=config["role_arn"],
        RoleSessionName="airbyte-cloudwatch-session",
        DurationSeconds=config.get("session_duration", 3600),
    )
    creds = assumed["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=config["region_name"],
    )


def _ms_from_iso(date_str: Optional[str]) -> Optional[int]:
    """Convert an ISO-8601 date string to a millisecond epoch integer."""
    if not date_str:
        return None
    return int(dateutil_parser.parse(date_str).timestamp() * 1000)


# ---------------------------------------------------------------------------
# CloudWatchLogsPartitionRouter
# ---------------------------------------------------------------------------

@dataclass
class CloudWatchLogsPartitionRouter(PartitionRouter):
    """
    Produces one stream-slice per (log_group, day) combination so that the
    declarative framework can iterate over them.

    Each slice looks like:
        {
            "log_group_name": "/aws/lambda/my-function",
            "start_time": 1700000000000,   # ms epoch, inclusive
            "end_time":   1700086399999,   # ms epoch, inclusive
            "log_stream_names": [...],     # optional
            "filter_pattern": "...",       # optional
            "stream_name": "...",          # human-readable stream name override
        }
    """

    parameters: dict
    config: Config = field(default_factory=dict)

    _ONE_DAY_MS: int = field(init=False, default=24 * 60 * 60 * 1000, repr=False)

    def get_request_params(self, *args, **kwargs) -> Mapping[str, Any]:
        return {}

    def get_request_headers(self, *args, **kwargs) -> Mapping[str, str]:
        return {}

    def get_request_body_data(self, *args, **kwargs) -> Optional[Union[Mapping, str]]:
        return None

    def get_request_body_json(self, *args, **kwargs) -> Optional[Mapping]:
        return None

    # ------------------------------------------------------------------
    # Core implementation
    # ------------------------------------------------------------------

    def _build_day_slices(
        self,
        log_group_name: str,
        start_ms: int,
        extra: Optional[dict] = None,
        stream_name: Optional[str] = None,
    ) -> Iterable[StreamSlice]:
        now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        extra = extra or {}
        for ts in range(start_ms, now_ms + 1, self._ONE_DAY_MS):
            slice_data = {
                "log_group_name": log_group_name,
                "start_time": ts,
                "end_time": min(ts + self._ONE_DAY_MS - 1, now_ms),
                **extra,
            }
            if stream_name:
                slice_data["stream_name"] = stream_name
            yield StreamSlice(partition=slice_data, cursor_slice={})

    def _earliest_timestamp(self, client, log_group_name: str, extra: dict) -> Optional[int]:
        response = client.filter_log_events(
            logGroupName=log_group_name,
            startTime=0,
            limit=1,
            **extra,
        )
        events = response.get("events", [])
        return events[0]["timestamp"] if events else None

    def stream_slices(self) -> Iterable[StreamSlice]:
        session = _assume_role_session(self.config)
        client = session.client("logs")

        start_date_ms = _ms_from_iso(self.config.get("start_date"))
        prefix = self.config.get("log_group_prefix")

        # ---- Discovered log groups ----------------------------------------
        paginator = client.get_paginator("describe_log_groups")
        page_kwargs = {"logGroupNamePrefix": prefix} if prefix else {}
        discovered_groups: List[str] = []
        for page in paginator.paginate(**page_kwargs):
            for group in page.get("logGroups", []):
                discovered_groups.append(group["logGroupName"])

        for group_name in discovered_groups:
            start_ms = start_date_ms or self._earliest_timestamp(client, group_name, {})
            if not start_ms:
                continue
            yield from self._build_day_slices(group_name, start_ms)

        # ---- Custom log reports -------------------------------------------
        for custom in self.config.get("custom_log_reports", []):
            log_group_name = custom["log_group_name"]
            extra: dict = {}
            if custom.get("log_stream_names"):
                extra["log_stream_names"] = custom["log_stream_names"]
            if custom.get("filter_pattern"):
                extra["filter_pattern"] = custom["filter_pattern"]

            start_ms = start_date_ms or self._earliest_timestamp(client, log_group_name, {})
            if not start_ms:
                continue
            yield from self._build_day_slices(
                log_group_name, start_ms, extra=extra, stream_name=custom["name"]
            )


# ---------------------------------------------------------------------------
# CloudWatchLogsRetriever
# ---------------------------------------------------------------------------

@dataclass
class CloudWatchLogsRetriever(Retriever):
    """
    Reads log events from a single (log_group, time_window) slice returned by
    CloudWatchLogsPartitionRouter and emits individual log records.

    Each emitted record has the shape:
        {
            "timestamp":      <int ms>,
            "message":        <str>,
            "logStreamName":  <str>,
            "ingestionTime":  <int ms>,
            "eventId":        <str>,
        }
    """

    parameters: dict
    config: Config = field(default_factory=dict)

    _logger: logging.Logger = field(
        init=False,
        default_factory=lambda: logging.getLogger("airbyte.source.cloudwatch"),
        repr=False,
    )

    # Incremental cursor tracking (per-retriever instance)
    _cursor_value: Optional[int] = field(init=False, default=None, repr=False)

    # ------------------------------------------------------------------
    # Retriever interface
    # ------------------------------------------------------------------

    def read_records(
        self,
        records_schema: Mapping[str, Any],
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[Record]:
        if stream_slice is None:
            return

        slice_data = dict(stream_slice.partition)
        log_group_name: str = slice_data["log_group_name"]
        start_time: int = slice_data["start_time"]
        end_time: int = slice_data["end_time"]

        filter_kwargs: dict = {}
        if slice_data.get("log_stream_names"):
            filter_kwargs["logStreamNames"] = slice_data["log_stream_names"]
        if slice_data.get("filter_pattern"):
            filter_kwargs["filterPattern"] = slice_data["filter_pattern"]

        session = _assume_role_session(self.config)
        client = session.client("logs")

        self._logger.info(
            f"Fetching logs from {start_time} to {end_time} for group: {log_group_name}"
        )

        next_token = None
        while True:
            params: dict = {
                "logGroupName": log_group_name,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 10000,
                **filter_kwargs,
            }
            if next_token:
                params["nextToken"] = next_token

            response = client.filter_log_events(**params)
            for event in response.get("events", []):
                ts = event["timestamp"]
                if self._cursor_value is None or ts > self._cursor_value:
                    self._cursor_value = ts
                yield event

            next_token = response.get("nextToken")
            if not next_token:
                break

    @property
    def cursor_field(self) -> str:
        return "timestamp"

    @property
    def state(self) -> MutableMapping[str, Any]:
        if self._cursor_value is not None:
            return {self.cursor_field: self._cursor_value}
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._cursor_value = value.get(self.cursor_field)
