#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from __future__ import annotations

from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional, Union

from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.types import Config


@dataclass
class InterpolatedString:
    """
    Wrapper around a raw string to be interpolated with the Jinja2 templating engine

    Attributes:
        string (str): The string to evalute
        default (Optional[str]): The default value to return if the evaluation returns an empty string
        parameters (Mapping[str, Any]): Additional runtime parameters to be used for string interpolation
    """

    string: str
    parameters: InitVar[Mapping[str, Any]]
    default: Optional[str] = None

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.default = self.default or self.string
        self._interpolation = JinjaInterpolation()
        self._parameters = parameters
        # indicates whether passed string is just a plain string, not Jinja template
        # This allows for optimization, but we do not know it yet at this stage
        self._is_plain_string = None

    def eval(self, config: Config, **kwargs: Any) -> Any:
        """
        Interpolates the input string using the config and other optional arguments passed as parameter.

        :param config: The user-provided configuration as specified by the source's spec
        :param kwargs: Optional parameters used for interpolation
        :return: The interpolated string
        """
        is_plain = self._is_plain_string
        if is_plain is True:
            return self.string
        elif is_plain is None:
            # Heuristic: if no jinja delimiters, mark as plain without invoking jinja at all!
            # (This shortcut avoids even the first roundtrip through Jinja parser when not needed.)
            s = self.string
            if "{{" not in s and "{%" not in s:
                self._is_plain_string = True
                return s
            # Otherwise, fallback to evaluation and cache result
            interpolation = self._interpolation
            params = self._parameters
            result = interpolation.eval(s, config, self.default, parameters=params, **kwargs)
            if s == result:
                self._is_plain_string = True
            else:
                self._is_plain_string = False
            return result
        # _is_plain_string is False, do normal evaluation
        return self._interpolation.eval(
            self.string, config, self.default, parameters=self._parameters, **kwargs
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InterpolatedString):
            return False
        return self.string == other.string and self.default == other.default

    @classmethod
    def create(
        cls,
        string_or_interpolated: Union["InterpolatedString", str],
        *,
        parameters: Mapping[str, Any],
    ) -> "InterpolatedString":
        """
        Helper function to obtain an InterpolatedString from either a raw string or an InterpolatedString.

        :param string_or_interpolated: either a raw string or an InterpolatedString.
        :param parameters: parameters propagated from parent component
        :return: InterpolatedString representing the input string.
        """
        if isinstance(string_or_interpolated, str):
            return InterpolatedString(string=string_or_interpolated, parameters=parameters)
        else:
            return string_or_interpolated
