#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Optional, Union

import requests

from airbyte_cdk.sources.declarative.requesters.error_handlers.http_response_filter import (
    HttpResponseFilter,
)
from airbyte_cdk.sources.streams.http.error_handlers.default_error_mapping import (
    DEFAULT_ERROR_MAPPING,
)
from airbyte_cdk.sources.streams.http.error_handlers.response_models import (
    ErrorResolution,
    create_fallback_error_resolution,
)


class DefaultHttpResponseFilter(HttpResponseFilter):
    def matches(
        self, response_or_exception: Optional[Union[requests.Response, Exception]]
    ) -> Optional[ErrorResolution]:
        mapped_key = (
            response_or_exception.status_code
            if isinstance(response_or_exception, requests.Response)
            else type(response_or_exception)
            if isinstance(response_or_exception, Exception)
            else None
        )
        default_mapped_error_resolution = (
            DEFAULT_ERROR_MAPPING.get(mapped_key) if mapped_key is not None else None
        )

        if default_mapped_error_resolution:
            return default_mapped_error_resolution
        return create_fallback_error_resolution(response_or_exception)
