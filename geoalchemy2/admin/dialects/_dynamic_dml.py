"""Private helpers for dynamic spatial DML bind expansion."""

from __future__ import annotations

from collections.abc import Mapping


def source_bind_identifier(source_bind):
    return getattr(source_bind, "_identifying_key", source_bind.key)


def source_bind_candidate_keys(source_bind):
    candidate_keys = []
    for candidate_key in (source_bind.key, getattr(source_bind, "_orig_key", None)):
        if candidate_key is not None and candidate_key not in candidate_keys:
            candidate_keys.append(candidate_key)
    return candidate_keys


def dml_column_key(value):
    return getattr(value, "key", value)


def iter_dml_value_pairs(value_container, table):
    if isinstance(value_container, Mapping):
        yield from value_container.items()
        return

    if isinstance(value_container, (list, tuple)):
        if value_container and all(
            isinstance(item, tuple) and len(item) == 2 for item in value_container
        ):
            yield from value_container
            return

        yield from zip(table.columns, value_container, strict=False)


def iter_dml_source_bind_pairs(clauseelement):
    table = getattr(clauseelement, "table", None)

    values = getattr(clauseelement, "_values", None) or {}
    if values:
        yield from iter_dml_value_pairs(values, table)

    ordered_values = getattr(clauseelement, "_ordered_values", None) or ()
    if ordered_values:
        yield from iter_dml_value_pairs(ordered_values, table)

    for multi_values in getattr(clauseelement, "_multi_values", ()) or ():
        for values in multi_values:
            yield from iter_dml_value_pairs(values, table)


def has_dml_multi_values(clauseelement):
    return bool(getattr(clauseelement, "_multi_values", ()) or ())


def wrap_dml_value_container(value_container, table, wrap_value):
    if isinstance(value_container, Mapping):
        changed = False
        wrapped_values = {}
        for column_key, value in value_container.items():
            wrapped_value, value_changed = wrap_value(column_key, value)
            wrapped_values[column_key] = wrapped_value
            changed = changed or value_changed
        return wrapped_values, changed

    if isinstance(value_container, (list, tuple)):
        if value_container and all(
            isinstance(item, tuple) and len(item) == 2 for item in value_container
        ):
            changed = False
            wrapped_values = []
            for column_key, value in value_container:
                wrapped_value, value_changed = wrap_value(column_key, value)
                wrapped_values.append((column_key, wrapped_value))
                changed = changed or value_changed
            return wrapped_values, changed

        changed = False
        wrapped_values = []
        for column, value in zip(table.columns, value_container, strict=False):
            wrapped_value, value_changed = wrap_value(column, value)
            wrapped_values.append(wrapped_value)
            changed = changed or value_changed
        return wrapped_values, changed

    return value_container, False


def wrap_dml_multi_values(clauseelement, spatial_columns, wrap_value):
    multi_values = getattr(clauseelement, "_multi_values", ()) or ()
    if not multi_values or not spatial_columns:
        return clauseelement

    table = getattr(clauseelement, "table", None)
    changed = False
    wrapped_multi_values = []
    for multi_value in multi_values:
        wrapped_multi_value = []
        for value_container in multi_value:
            wrapped_value_container, value_changed = wrap_dml_value_container(
                value_container,
                table,
                wrap_value,
            )
            wrapped_multi_value.append(wrapped_value_container)
            changed = changed or value_changed
        wrapped_multi_values.append(wrapped_multi_value)

    if not changed:
        return clauseelement

    wrapped_clauseelement = clauseelement._generate()
    wrapped_clauseelement._multi_values = tuple(wrapped_multi_values)
    return wrapped_clauseelement


def compile_statement_bind_name_map(clauseelement, dialect, disable_option):
    if not hasattr(clauseelement, "compile"):
        return {}

    if hasattr(clauseelement, "execution_options"):
        clauseelement = clauseelement.execution_options(**{disable_option: True})

    compiled = clauseelement.compile(dialect=dialect)
    bind_name_map = {}
    for bind, compiled_name in compiled.bind_names.items():
        bind_name_map.setdefault(source_bind_identifier(bind), compiled_name)
    return bind_name_map


def iter_parameter_mappings(multiparams, params):
    if isinstance(params, Mapping):
        yield params
    for parameters in multiparams or ():
        if isinstance(parameters, Mapping):
            yield parameters
        elif isinstance(parameters, (list, tuple)):
            for row in parameters:
                if isinstance(row, Mapping):
                    yield row


def collect_present_parameter_keys(multiparams, params, candidate_key_groups):
    present_keys = set()
    for candidate_keys in candidate_key_groups:
        if not candidate_keys:
            continue

        for parameters in iter_parameter_mappings(multiparams, params):
            present_key = next((key for key in candidate_keys if key in parameters), None)
            if present_key is not None:
                present_keys.add(present_key)
                break
    return present_keys


def expand_dynamic_param_mapping(parameters, dynamic_bind_mappings):
    if not isinstance(parameters, Mapping):
        return parameters, False

    expanded_parameters = parameters
    changed = False
    for source_keys, value_key, srid_key in dynamic_bind_mappings:
        source_key = next((key for key in source_keys if key in parameters), None)
        if source_key is None:
            continue

        if value_key in parameters and srid_key in parameters:
            continue

        if expanded_parameters is parameters:
            expanded_parameters = dict(parameters)

        source_value = parameters[source_key]
        expanded_parameters.setdefault(value_key, source_value)
        expanded_parameters.setdefault(srid_key, source_value)
        changed = True

    return expanded_parameters, changed


def expand_dynamic_param_container(parameters, dynamic_bind_mappings):
    if isinstance(parameters, Mapping):
        return expand_dynamic_param_mapping(parameters, dynamic_bind_mappings)

    if not isinstance(parameters, (list, tuple)):
        return parameters, False

    expanded_values = []
    changed = False
    for value in parameters:
        expanded_value, value_changed = expand_dynamic_param_mapping(
            value,
            dynamic_bind_mappings,
        )
        expanded_values.append(expanded_value)
        changed = changed or value_changed

    if not changed:
        return parameters, False
    if isinstance(parameters, tuple):
        return tuple(expanded_values), True
    return expanded_values, True
