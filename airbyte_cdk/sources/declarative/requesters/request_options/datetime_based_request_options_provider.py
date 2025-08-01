#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from typing import Any, Mapping, MutableMapping, Optional, Union

from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.requesters.request_option import (
    RequestOption,
    RequestOptionType,
)
from airbyte_cdk.sources.declarative.requesters.request_options.request_options_provider import (
    RequestOptionsProvider,
)
from airbyte_cdk.sources.types import Config, StreamSlice, StreamState


@dataclass
class DatetimeBasedRequestOptionsProvider(RequestOptionsProvider):
    """
    Request options provider that extracts fields from the stream_slice and injects them into the respective location in the
    outbound request being made
    """

    config: Config
    parameters: InitVar[Mapping[str, Any]]
    start_time_option: Optional[RequestOption] = None
    end_time_option: Optional[RequestOption] = None
    partition_field_start: Optional[str] = None
    partition_field_end: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._partition_field_start = InterpolatedString.create(
            self.partition_field_start or "start_time", parameters=parameters
        )
        self._partition_field_end = InterpolatedString.create(
            self.partition_field_end or "end_time", parameters=parameters
        )

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.request_parameter, stream_slice)

    def get_request_headers(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # Only stream_slice is relevant for this provider
        return self._get_request_options(RequestOptionType.header, stream_slice)

    def get_request_body_data(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Union[Mapping[str, Any], str]:
        return self._get_request_options(RequestOptionType.body_data, stream_slice)

    def get_request_body_json(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.body_json, stream_slice)

    def _get_request_options(
        self, option_type: RequestOptionType, stream_slice: Optional[StreamSlice]
    ) -> Mapping[str, Any]:
        # Minimize dict and attribute access in the main hot path
        if stream_slice is None:
            # Return early with empty dict if no slice
            return {}

        options: MutableMapping[str, Any] = {}

        start_time_option = self.start_time_option
        if start_time_option is not None and start_time_option.inject_into == option_type:
            key_start = self._partition_field_start.eval(self.config)
            start_time_value = stream_slice.get(key_start)
            start_time_option.inject_into_request(options, start_time_value, self.config)

        end_time_option = self.end_time_option
        if end_time_option is not None and end_time_option.inject_into == option_type:
            key_end = self._partition_field_end.eval(self.config)
            end_time_value = stream_slice.get(key_end)
            end_time_option.inject_into_request(options, end_time_value, self.config)

        return options
