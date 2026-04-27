"""This module defines specific functions for CockroachDB dialect."""

import re
import warnings

from sqlalchemy import text
from sqlalchemy.exc import SAWarning
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import functions
from geoalchemy2.admin.dialects import postgresql
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster

_CRDB_SPATIAL_TYPE = re.compile(
    r"^(?P<type>geometry|geography)(?:\((?P<geometry_type>[^,()]+)(?:,\s*(?P<srid>\d+))?\))?$",
    re.IGNORECASE,
)


def _get_spatial_type(type_name):
    match = _CRDB_SPATIAL_TYPE.match(type_name)
    if match is None:
        return None

    spatial_type = Geometry if match.group("type").lower() == "geometry" else Geography
    geometry_type = match.group("geometry_type")
    srid = match.group("srid")
    return spatial_type(
        geometry_type=geometry_type.upper() if geometry_type is not None else "GEOMETRY",
        srid=int(srid) if srid is not None else -1,
    )


def _get_spatial_type_map(bind, table_name, schema):
    try:
        rows = bind.execute(
            text(
                """SELECT column_name, crdb_sql_type
                FROM information_schema.columns
                WHERE table_schema = :schema
                    AND table_name = :table_name"""
            ),
            {"schema": schema, "table_name": table_name},
        )
    except SQLAlchemyError:
        return {}

    spatial_types = {}
    for row in rows:
        spatial_type = _get_spatial_type(row[1])
        if spatial_type is not None:
            spatial_types[row[0]] = spatial_type
    return spatial_types


def _patch_get_columns(cockroachdb_base):
    dialect_cls = getattr(cockroachdb_base, "CockroachDBDialect", None)
    if dialect_cls is None or hasattr(dialect_cls, "_geoalchemy2_get_columns"):
        return

    normal_get_columns = dialect_cls.get_columns

    def get_columns(self, conn, table_name, schema=None, **kw):
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Could not parse type name '(geometry|geography).*",
                category=SAWarning,
            )
            columns = normal_get_columns(self, conn, table_name, schema=schema, **kw)

        spatial_types = _get_spatial_type_map(
            conn,
            table_name,
            schema or self.default_schema_name,
        )
        for column in columns:
            spatial_type = spatial_types.get(column["name"])
            if spatial_type is not None:
                column["type"] = spatial_type

        return columns

    dialect_cls.get_columns = get_columns
    dialect_cls._geoalchemy2_get_columns = normal_get_columns


def _register_cockroachdb_types():
    """Register GeoAlchemy2 spatial types in sqlalchemy-cockroachdb when available."""
    try:
        from sqlalchemy_cockroachdb import base as cockroachdb_base  # type: ignore[import-untyped]
    except ImportError:
        return

    type_map = getattr(cockroachdb_base, "_type_map", None)
    if type_map is not None:
        type_map["geometry"] = Geometry
        type_map["geography"] = Geography

    _patch_get_columns(cockroachdb_base)


_register_cockroachdb_types()


@compiles(functions.ST_GeomFromEWKB, "cockroachdb")  # type: ignore
def _CockroachDB_ST_GeomFromEWKB(element, compiler, **kw):
    clauses = list(element.clauses)
    if kw.get("literal_binds", False):
        wkb_clause = compile_bin_literal(clauses[0])
        prefix = "decode("
        suffix = ", 'hex')"
    else:
        wkb_clause = clauses[0]
        prefix = ""
        suffix = ""

    compiled = compiler.process(wkb_clause, **kw)
    return f"{element.identifier}({prefix}{compiled}{suffix})"


def _resolve_spatial_type(column_type, dialect):
    if isinstance(column_type, TypeDecorator):
        if dialect is not None:
            return column_type.load_dialect_impl(dialect)
        return getattr(column_type, "impl", column_type)
    return column_type


def validate_column(column, dialect):
    """Validate that a spatial column can be used with CockroachDB."""
    spatial_type = _resolve_spatial_type(column.type, dialect)

    if isinstance(spatial_type, Raster):
        raise ArgumentError("CockroachDB dialect does not support Raster columns")

    if not isinstance(spatial_type, (Geometry, Geography)):
        return

    if getattr(spatial_type, "use_typmod", None) is False:
        raise ArgumentError(
            "CockroachDB dialect does not support managed Geometry columns (use_typmod=False)"
        )

    if getattr(spatial_type, "use_N_D_index", False):
        raise ArgumentError("CockroachDB dialect does not support N-D spatial indexes")


def _validate_table(table, bind):
    dialect = bind.dialect
    for column in table.columns:
        validate_column(column, dialect)


def check_management(column):
    """Check if the column should be managed."""
    validate_column(column, None)
    return False


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    dialect = None if bind is None else bind.dialect
    validate_column(col, dialect)
    return postgresql.create_spatial_index(bind, table, col)


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with CockroachDB dialect."""
    if isinstance(column_info.get("type"), NullType):
        spatial_type = _get_spatial_type_map(
            inspector.bind,
            table.name,
            table.schema or inspector.bind.dialect.default_schema_name,
        ).get(column_info["name"])
        if spatial_type is None:
            return
        column_info["type"] = spatial_type

    return postgresql.reflect_geometry_column(inspector, table, column_info)


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    _validate_table(table, bind)
    return postgresql.before_create(table, bind, **kw)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    return postgresql.after_create(table, bind, **kw)


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    _validate_table(table, bind)
    return postgresql.before_drop(table, bind, **kw)


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    return postgresql.after_drop(table, bind, **kw)
