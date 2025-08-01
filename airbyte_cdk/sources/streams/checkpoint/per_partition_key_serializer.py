# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import json
from functools import lru_cache
from typing import Any, Mapping


class PerPartitionKeySerializer:
    """
    We are concerned of the performance of looping through the `states` list and evaluating equality on the partition. To reduce this
    concern, we wanted to use dictionaries to map `partition -> cursor`. However, partitions are dict and dict can't be used as dict keys
    since they are not hashable. By creating json string using the dict, we can have a use the dict as a key to the dict since strings are
    hashable.
    """

    @staticmethod
    def to_partition_key(to_serialize: Any) -> str:
        t = _any_to_tuple(to_serialize)
        return PerPartitionKeySerializer._cached_json_dumps(t)

    @staticmethod
    def to_partition(to_deserialize: Any) -> Mapping[str, Any]:
        return json.loads(to_deserialize)  # type: ignore # The partition is known to be a dict, but the type hint is Any

    @staticmethod
    @lru_cache(maxsize=65536)
    def _cached_json_dumps(tuple_partition) -> str:
        # separators have changed in Python 3.4. To avoid being impacted by further change, we explicitly specify our own value
        return json.dumps(
            _tuple_to_obj(tuple_partition), indent=None, separators=(",", ":"), sort_keys=True
        )


def _any_to_tuple(v):
    if isinstance(v, dict):
        return tuple((k, _any_to_tuple(val)) for k, val in sorted(v.items()))
    elif isinstance(v, list):
        return tuple(_any_to_tuple(i) for i in v)
    else:
        return v


def _tuple_to_obj(obj):
    if not isinstance(obj, tuple):
        return obj
    if all(isinstance(e, tuple) and len(e) == 2 and isinstance(e[0], str) for e in obj):
        return {k: _tuple_to_obj(v) for k, v in obj}
    else:
        return [_tuple_to_obj(i) for i in obj]
