#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import typing
from typing import Any, Dict, Mapping, Optional

PARAMETERS_STR = "$parameters"


DEFAULT_MODEL_TYPES: Mapping[str, str] = {
    # CompositeErrorHandler
    "CompositeErrorHandler.error_handlers": "DefaultErrorHandler",
    # CursorPagination
    "CursorPagination.decoder": "JsonDecoder",
    # DatetimeBasedCursor
    "DatetimeBasedCursor.end_datetime": "MinMaxDatetime",
    "DatetimeBasedCursor.end_time_option": "RequestOption",
    "DatetimeBasedCursor.start_datetime": "MinMaxDatetime",
    "DatetimeBasedCursor.start_time_option": "RequestOption",
    # CustomIncrementalSync
    "CustomIncrementalSync.end_datetime": "MinMaxDatetime",
    "CustomIncrementalSync.end_time_option": "RequestOption",
    "CustomIncrementalSync.start_datetime": "MinMaxDatetime",
    "CustomIncrementalSync.start_time_option": "RequestOption",
    # DeclarativeSource
    "DeclarativeSource.check": "CheckStream",
    "DeclarativeSource.spec": "Spec",
    "DeclarativeSource.streams": "DeclarativeStream",
    # DeclarativeStream
    "DeclarativeStream.retriever": "SimpleRetriever",
    "DeclarativeStream.schema_loader": "JsonFileSchemaLoader",
    # DynamicDeclarativeStream
    "DynamicDeclarativeStream.stream_template": "DeclarativeStream",
    "DynamicDeclarativeStream.components_resolver": "ConfigComponentResolver",
    # HttpComponentsResolver
    "HttpComponentsResolver.retriever": "SimpleRetriever",
    "HttpComponentsResolver.components_mapping": "ComponentMappingDefinition",
    # ConfigComponentResolver
    "ConfigComponentsResolver.stream_config": "StreamConfig",
    "ConfigComponentsResolver.components_mapping": "ComponentMappingDefinition",
    # DefaultErrorHandler
    "DefaultErrorHandler.response_filters": "HttpResponseFilter",
    # DefaultPaginator
    "DefaultPaginator.decoder": "JsonDecoder",
    "DefaultPaginator.page_size_option": "RequestOption",
    # DpathExtractor
    "DpathExtractor.decoder": "JsonDecoder",
    # HttpRequester
    "HttpRequester.error_handler": "DefaultErrorHandler",
    # ListPartitionRouter
    "ListPartitionRouter.request_option": "RequestOption",
    # ParentStreamConfig
    "ParentStreamConfig.request_option": "RequestOption",
    "ParentStreamConfig.stream": "DeclarativeStream",
    # RecordSelector
    "RecordSelector.extractor": "DpathExtractor",
    "RecordSelector.record_filter": "RecordFilter",
    # SimpleRetriever
    "SimpleRetriever.paginator": "NoPagination",
    "SimpleRetriever.record_selector": "RecordSelector",
    "SimpleRetriever.requester": "HttpRequester",
    # SubstreamPartitionRouter
    "SubstreamPartitionRouter.parent_stream_configs": "ParentStreamConfig",
    # AddFields
    "AddFields.fields": "AddedFieldDefinition",
    # CustomPartitionRouter
    "CustomPartitionRouter.parent_stream_configs": "ParentStreamConfig",
    # DynamicSchemaLoader
    "DynamicSchemaLoader.retriever": "SimpleRetriever",
    # SchemaTypeIdentifier
    "SchemaTypeIdentifier.types_map": "TypesMap",
}

# We retain a separate registry for custom components to automatically insert the type if it is missing. This is intended to
# be a short term fix because once we have migrated, then type and class_name should be requirements for all custom components.
CUSTOM_COMPONENTS_MAPPING: Mapping[str, str] = {
    "CompositeErrorHandler.backoff_strategies": "CustomBackoffStrategy",
    "DeclarativeStream.retriever": "CustomRetriever",
    "DeclarativeStream.transformations": "CustomTransformation",
    "DefaultErrorHandler.backoff_strategies": "CustomBackoffStrategy",
    "DefaultPaginator.pagination_strategy": "CustomPaginationStrategy",
    "HttpRequester.authenticator": "CustomAuthenticator",
    "HttpRequester.error_handler": "CustomErrorHandler",
    "RecordSelector.extractor": "CustomRecordExtractor",
    "SimpleRetriever.partition_router": "CustomPartitionRouter",
}


class ManifestComponentTransformer:
    def propagate_types_and_parameters(
        self,
        parent_field_identifier: str,
        declarative_component: Mapping[str, Any],
        parent_parameters: Mapping[str, Any],
        use_parent_parameters: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Recursively transforms the specified declarative component and subcomponents to propagate parameters and insert the
        default component type if it was not already present. The resulting transformed components are a deep copy of the input
        components, not an in-place transformation.

        :param declarative_component: The current component that is having type and parameters added
        :param parent_field_identifier: The name of the field of the current component coming from the parent component
        :param parent_parameters: The parameters set on parent components defined before the current component
        :param use_parent_parameters: If set, parent parameters will be used as the source of truth when key names are the same
        :return: A deep copy of the transformed component with types and parameters persisted to it
        """
        # Use shallow-copy if possible, else fall back to deepcopy once
        propagated_component = dict(declarative_component)
        propagated_component_type = propagated_component.get("type")

        # Inline fast-path: insert type if missing using lookup,
        # does not copy unnecessarily -- all later recursions share dictionary accesses
        if propagated_component_type is None:
            found_type = (
                CUSTOM_COMPONENTS_MAPPING.get(parent_field_identifier)
                if "class_name" in propagated_component
                else DEFAULT_MODEL_TYPES.get(parent_field_identifier)
            )
            if found_type:
                propagated_component["type"] = found_type
                propagated_component_type = found_type

        # Compose parameters (fast branch)
        component_parameters = propagated_component.pop(PARAMETERS_STR, None)
        if component_parameters:
            if use_parent_parameters:
                current_parameters = {**component_parameters, **parent_parameters}
            else:
                current_parameters = {**parent_parameters, **component_parameters}
        else:
            current_parameters = dict(parent_parameters) if parent_parameters else {}

        # Fast non-component path
        if self._is_json_schema_object(propagated_component):
            return propagated_component

        # Fast path: Nested object handling (e.g., QueryProperties, never process twice, call optimized below)
        if propagated_component_type is None:
            if self._has_nested_components(propagated_component):
                return self._process_nested_components(
                    propagated_component,
                    parent_field_identifier,
                    current_parameters,
                    use_parent_parameters,
                )
            return propagated_component

        # Parameter application: do not overwrite fields, skip if already present (and not falsy)
        if current_parameters:
            for parameter_key, parameter_value in current_parameters.items():
                if (
                    parameter_key not in propagated_component
                    or not propagated_component[parameter_key]
                ):
                    propagated_component[parameter_key] = parameter_value

        # Avoid dict.items() call N times -- convert to list first if dict is mutated inside loop
        items = list(propagated_component.items())
        for field_name, field_value in items:
            # Only pop when field_name is found, so .get() is safe
            if isinstance(field_value, dict):
                excluded_parameter = (
                    current_parameters.pop(field_name, None)
                    if field_name in current_parameters
                    else None
                )
                parent_type_field_identifier = f"{propagated_component_type}.{field_name}"
                propagated_component[field_name] = self.propagate_types_and_parameters(
                    parent_type_field_identifier,
                    field_value,
                    current_parameters,
                    use_parent_parameters=use_parent_parameters,
                )
                if excluded_parameter is not None:
                    current_parameters[field_name] = excluded_parameter
            elif isinstance(field_value, list) or isinstance(field_value, typing.List):
                excluded_parameter = (
                    current_parameters.pop(field_name, None)
                    if field_name in current_parameters
                    else None
                )
                parent_type_field_identifier = f"{propagated_component_type}.{field_name}"
                for i in range(len(field_value)):
                    element = field_value[i]
                    if isinstance(element, dict):
                        field_value[i] = self.propagate_types_and_parameters(
                            parent_type_field_identifier,
                            element,
                            current_parameters,
                            use_parent_parameters=use_parent_parameters,
                        )
                if excluded_parameter is not None:
                    current_parameters[field_name] = excluded_parameter

        # Only if there are current_parameters left do we store them, no use in copying None/empty
        if current_parameters:
            propagated_component[PARAMETERS_STR] = current_parameters
        return propagated_component

    @staticmethod
    def _is_json_schema_object(propagated_component: Mapping[str, Any]) -> bool:
        return propagated_component.get("type") == "object" or propagated_component.get("type") == [
            "null",
            "object",
        ]

    @staticmethod
    def _has_nested_components(propagated_component: Dict[str, Any]) -> bool:
        for k, v in propagated_component.items():
            if isinstance(v, dict) and v.get("type"):
                return True
        return False

    def _process_nested_components(
        self,
        propagated_component: Dict[str, Any],
        parent_field_identifier: str,
        current_parameters: Mapping[str, Any],
        use_parent_parameters: Optional[bool] = None,
    ) -> Dict[str, Any]:
        for field_name, field_value in propagated_component.items():
            if isinstance(field_value, dict) and field_value.get("type"):
                nested_component_with_parameters = self.propagate_types_and_parameters(
                    parent_field_identifier,
                    field_value,
                    current_parameters,
                    use_parent_parameters=use_parent_parameters,
                )
                propagated_component[field_name] = nested_component_with_parameters

        return propagated_component
