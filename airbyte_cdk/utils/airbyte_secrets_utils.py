#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping


def get_secret_paths(spec: Mapping[str, Any]) -> List[List[str]]:
    paths = []

    def traverse_schema(schema_item: Any, path: List[str]) -> None:
        """
        schema_item can be any property or value in the originally input jsonschema, depending on how far down the recursion stack we go
        path is the path to that schema item in the original input
        for example if we have the input {'password': {'type': 'string', 'airbyte_secret': True}} then the arguments will evolve
        as follows:
        schema_item=<whole_object>, path=[]
        schema_item={'type': 'string', 'airbyte_secret': True}, path=['password']
        schema_item='string', path=['password', 'type']
        schema_item=True, path=['password', 'airbyte_secret']
        """
        if isinstance(schema_item, dict):
            for k, v in schema_item.items():
                # Avoid new list allocations by mutating in place
                path.append(k)
                traverse_schema(v, path)
                path.pop()
        elif isinstance(schema_item, list):
            for i in schema_item:
                traverse_schema(i, path)
        else:
            if path and path[-1] == "airbyte_secret" and schema_item is True:
                # Remove "properties" and "oneOf" from path, only once per segment
                filtered_path = [p for p in path[:-1] if p not in ("properties", "oneOf")]
                # Instead of using a set to remove dups, append directly
                paths.append(filtered_path)

    traverse_schema(spec, [])
    return paths


def get_secrets(
    connection_specification: Mapping[str, Any], config: Mapping[str, Any]
) -> List[Any]:
    """
    Get a list of secret values from the source config based on the source specification
    :type connection_specification: the connection_specification field of an AirbyteSpecification i.e the JSONSchema definition
    """
    secret_paths = get_secret_paths(connection_specification.get("properties", {}))
    result = []

    # Use local var for .append to accelerate in loop
    append = result.append
    # Avoid try/except in fast path; partition into those present and those missing
    for path in secret_paths:
        try:
            val = _fast_dict_path_get(config, path)
            append(val)
        except KeyError:
            # Field may not exist -- skip, as spec allows
            continue
    return result


__SECRETS_FROM_CONFIG: List[str] = []


def update_secrets(secrets: List[str]) -> None:
    """Update the list of secrets to be replaced"""
    global __SECRETS_FROM_CONFIG
    __SECRETS_FROM_CONFIG = secrets


def add_to_secrets(secret: str) -> None:
    """Add to the list of secrets to be replaced"""
    global __SECRETS_FROM_CONFIG
    __SECRETS_FROM_CONFIG.append(secret)


def filter_secrets(string: str) -> str:
    """Filter secrets from a string by replacing them with ****"""
    # TODO this should perform a maximal match for each secret. if "x" and "xk" are both secret values, and this method is called twice on
    #  the input "xk", then depending on call order it might only obfuscate "*k". This is a bug.
    for secret in __SECRETS_FROM_CONFIG:
        if secret:
            string = string.replace(str(secret), "****")
    return string


def _fast_dict_path_get(dct: Mapping[str, Any], path: List[str]) -> Any:
    """Efficient lookups with graceful fallback if path doesn't exist (raise KeyError)."""
    cur = dct
    for p in path:
        if not isinstance(cur, Mapping):
            raise KeyError
        cur = cur[p]
    return cur
