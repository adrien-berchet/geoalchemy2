import re
import sys
import types

import pytest
from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.schema import CreateIndex
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2 import alembic_helpers
from geoalchemy2.admin import select_dialect as select_admin_dialect
from geoalchemy2.admin.dialects import cockroachdb as cockroachdb_admin
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import select_dialect as select_type_dialect
from geoalchemy2.types.dialects import cockroachdb as cockroachdb_type


class CockroachDBDialect(PGDialect):
    name = "cockroachdb"


class Bind:
    dialect = CockroachDBDialect()


def normalize_sql(sql):
    return re.sub(r"\s+", " ", str(sql)).strip()


def test_type_select_dialect():
    assert select_type_dialect("cockroachdb") is cockroachdb_type


def test_admin_select_dialect():
    assert select_admin_dialect("cockroachdb") is cockroachdb_admin


def test_cockroachdb_registers_external_type_map(monkeypatch):
    package = types.ModuleType("sqlalchemy_cockroachdb")
    base = types.ModuleType("sqlalchemy_cockroachdb.base")
    base._type_map = {}
    package.base = base
    monkeypatch.setitem(sys.modules, "sqlalchemy_cockroachdb", package)
    monkeypatch.setitem(sys.modules, "sqlalchemy_cockroachdb.base", base)

    cockroachdb_admin._register_cockroachdb_types()

    assert base._type_map["geometry"] is Geometry
    assert base._type_map["geography"] is Geography


def test_alembic_cockroachdb_get_indexes_normalizes_spatial_index(monkeypatch):
    package = types.ModuleType("sqlalchemy_cockroachdb")
    base = types.ModuleType("sqlalchemy_cockroachdb.base")

    class ExternalCockroachDBDialect:
        default_schema_name = "public"

        def get_indexes(self, connection, table_name, schema=None, **kw):
            return [
                {
                    "name": "idx_lake_geom",
                    "column_names": ["geom"],
                    "column_sorting": {"geom": ("nulls_first",)},
                    "dialect_options": {
                        "postgresql_ops": {"geom": None},
                        "postgresql_using": "inverted",
                    },
                    "unique": False,
                },
                {
                    "name": "idx_lake_geog",
                    "column_names": ["geog"],
                    "column_sorting": {"geog": ("nulls_first",)},
                    "dialect_options": {
                        "postgresql_ops": {"geog": None},
                        "postgresql_using": "gin",
                    },
                    "unique": False,
                },
                {
                    "name": "idx_lake_id",
                    "column_names": ["id"],
                    "column_sorting": {"id": ("nulls_first",)},
                    "unique": False,
                },
            ]

    class Connection:
        def execute(self, statement, params):
            assert params == {"schema": "public", "table_name": "lake"}
            return [
                ("geom", "geometry(LINESTRING,4326)"),
                ("geog", "geography(POINT,4326)"),
                ("id", "INT8"),
            ]

    base.CockroachDBDialect = ExternalCockroachDBDialect
    package.base = base
    monkeypatch.setitem(sys.modules, "sqlalchemy_cockroachdb", package)
    monkeypatch.setitem(sys.modules, "sqlalchemy_cockroachdb.base", base)

    alembic_helpers._monkey_patch_get_indexes_for_cockroachdb()

    indexes = ExternalCockroachDBDialect().get_indexes(Connection(), "lake")

    assert indexes[0] == {
        "name": "idx_lake_geom",
        "column_names": ["geom"],
        "dialect_options": {"postgresql_using": "gist", "_column_flag": True},
        "unique": False,
    }
    assert indexes[1] == {
        "name": "idx_lake_geog",
        "column_names": ["geog"],
        "dialect_options": {"postgresql_using": "gist", "_column_flag": True},
        "unique": False,
    }
    assert indexes[2] == {
        "name": "idx_lake_id",
        "column_names": ["id"],
        "column_sorting": {"id": ("nulls_first",)},
        "unique": False,
    }


@pytest.mark.parametrize(
    ("type_name", "expected_class", "geometry_type", "srid", "dimension"),
    [
        ("GEOMETRY(POINT,4326)", Geometry, "POINT", 4326, 2),
        ("geography(linestring,4326)", Geography, "LINESTRING", 4326, 2),
        ("GEOMETRY(POINTZ,3857)", Geometry, "POINTZ", 3857, 3),
        ("GEOMETRY", Geometry, "GEOMETRY", -1, 2),
    ],
)
def test_get_spatial_type(type_name, expected_class, geometry_type, srid, dimension):
    spatial_type = cockroachdb_admin._get_spatial_type(type_name)

    assert isinstance(spatial_type, expected_class)
    assert spatial_type.geometry_type == geometry_type
    assert spatial_type.srid == srid
    assert spatial_type.dimension == dimension


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _CockroachReflectionBind:
    dialect = CockroachDBDialect()

    def __init__(self, spatial_index, spatial_type_rows=()):
        self.spatial_index = spatial_index
        self.spatial_type_rows = spatial_type_rows
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params))
        if "information_schema.columns" in sql:
            return self.spatial_type_rows
        return _ScalarResult(self.spatial_index)


class _CockroachReflectionInspector:
    def __init__(self, bind, info_cache=None):
        self.bind = bind
        self.info_cache = {} if info_cache is None else info_cache


@pytest.mark.parametrize(
    "spatial_type",
    [
        Geometry(geometry_type="LINESTRING", srid=4326),
        Geography(geometry_type="POINT", srid=4326),
    ],
)
def test_reflect_geometry_column_accepts_cockroach_internal_spatial_index_ams(spatial_type):
    bind = _CockroachReflectionBind(spatial_index=True)
    inspector = _CockroachReflectionInspector(bind)
    table = Table("lake", MetaData(), schema="gis")
    column_info = {"name": "geom", "type": spatial_type}

    cockroachdb_admin.reflect_geometry_column(inspector, table, column_info)

    assert column_info["type"].spatial_index is True
    assert column_info["type"]._spatial_index_reflected is True
    assert bind.calls[-1][1]["schema_name"] == "gis"
    assert {bind.calls[-1][1][f"am_name_{idx}"] for idx in range(3)} == {
        "gist",
        "gin",
        "inverted",
    }


def test_reflect_geometry_column_preserves_false_spatial_index_result():
    bind = _CockroachReflectionBind(spatial_index=False)
    inspector = _CockroachReflectionInspector(bind)
    table = Table("lake", MetaData(), schema="gis")
    column_info = {"name": "geom", "type": Geometry(geometry_type="LINESTRING", srid=4326)}

    cockroachdb_admin.reflect_geometry_column(inspector, table, column_info)

    assert column_info["type"].spatial_index is False
    assert column_info["type"]._spatial_index_reflected is True


def test_reflect_geometry_column_resolves_nulltype_with_schema_before_index_check():
    bind = _CockroachReflectionBind(
        spatial_index=True,
        spatial_type_rows=[("geom", "geometry(LINESTRING,4326)")],
    )
    inspector = _CockroachReflectionInspector(bind)
    table = Table("lake", MetaData(), schema="gis")
    column_info = {"name": "geom", "type": NullType()}

    cockroachdb_admin.reflect_geometry_column(inspector, table, column_info)

    assert isinstance(column_info["type"], Geometry)
    assert column_info["type"].geometry_type == "LINESTRING"
    assert column_info["type"].srid == 4326
    assert column_info["type"].spatial_index is True
    assert column_info["type"]._spatial_index_reflected is True
    assert bind.calls[0][1] == {"schema": "gis", "table_name": "lake"}


def test_reflect_geometry_column_reuses_spatial_type_map_cache():
    bind = _CockroachReflectionBind(
        spatial_index=True,
        spatial_type_rows=[("geom", "geometry(LINESTRING,4326)")],
    )
    inspector = _CockroachReflectionInspector(bind, info_cache={})
    table = Table("lake", MetaData(), schema="gis")

    cockroachdb_admin.reflect_geometry_column(
        inspector,
        table,
        {"name": "geom", "type": NullType()},
    )
    cockroachdb_admin.reflect_geometry_column(
        inspector,
        table,
        {"name": "geom", "type": NullType()},
    )

    information_schema_calls = [
        call for call in bind.calls if "information_schema.columns" in call[0]
    ]
    assert len(information_schema_calls) == 1


def test_spatial_type_map_cache_returns_fresh_type_instances():
    info_cache = {}
    bind = _CockroachReflectionBind(
        spatial_index=True,
        spatial_type_rows=[("geom", "geometry(LINESTRING,4326)")],
    )

    first = cockroachdb_admin._get_spatial_type_map(
        bind,
        "lake",
        "gis",
        info_cache=info_cache,
    )
    second = cockroachdb_admin._get_spatial_type_map(
        bind,
        "lake",
        "gis",
        info_cache=info_cache,
    )

    assert first["geom"] is not second["geom"]
    assert first["geom"].geometry_type == second["geom"].geometry_type == "LINESTRING"
    information_schema_calls = [
        call for call in bind.calls if "information_schema.columns" in call[0]
    ]
    assert len(information_schema_calls) == 1


def test_geometry_and_geography_compile():
    table = Table(
        "lake",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("geom", Geometry(geometry_type="POINT", srid=4326)),
        Column("geog", Geography(geometry_type="POINT", srid=4326)),
    )
    idx = Index("idx_lake_geom", table.c.geom, postgresql_using="gist")

    create_table = normalize_sql(CreateTable(table).compile(dialect=CockroachDBDialect()))
    create_index = normalize_sql(CreateIndex(idx).compile(dialect=CockroachDBDialect()))

    assert "geom geometry(POINT,4326)" in create_table
    assert "geog geography(POINT,4326)" in create_table
    assert create_index == "CREATE INDEX idx_lake_geom ON lake USING gist (geom)"


def test_geom_from_wkb_literal_compile():
    wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
    expr = func.ST_GeomFromWKB(wkb, 4326)

    compiled = normalize_sql(
        expr.compile(dialect=CockroachDBDialect(), compile_kwargs={"literal_binds": True})
    )

    assert (
        compiled
        == "ST_GeomFromWKB(decode('0101000000000000000000f03f0000000000000040', 'hex'), 4326)"
    )


def test_geom_from_ewkb_literal_compile_omits_srid():
    ewkb = bytes.fromhex("0101000020e6100000000000000000f03f0000000000000040")
    expr = func.ST_GeomFromEWKB(ewkb, type_=Geometry(srid=4326))

    compiled = normalize_sql(
        expr.compile(dialect=CockroachDBDialect(), compile_kwargs={"literal_binds": True})
    )

    assert (
        compiled
        == "ST_GeomFromEWKB(decode('0101000020e6100000000000000000f03f0000000000000040', 'hex'))"
    )


def test_geom_from_ewkb_compile_omits_srid_parameter():
    expr = func.ST_GeomFromEWKB(
        bytes.fromhex("0101000020e6100000000000000000f03f0000000000000040"),
        type_=Geometry(srid=4326),
    )

    compiled = normalize_sql(expr.compile(dialect=CockroachDBDialect()))

    assert compiled == "ST_GeomFromEWKB(%(ST_GeomFromEWKB_1)s)"


@pytest.mark.parametrize(
    ("spatial_type", "wkb"),
    [
        (
            Geometry(srid=4326),
            bytes.fromhex("0101000000000000000000f03f0000000000000040"),
        ),
        (
            Geometry(srid=4326),
            memoryview(bytes.fromhex("0101000000000000000000f03f0000000000000040")),
        ),
        (
            Geometry(),
            WKBElement("0101000000000000000000f03f0000000000000040", srid=4326),
        ),
        (
            Geometry(),
            WKBElement("0101000020e6100000000000000000f03f0000000000000040", extended=True),
        ),
    ],
)
def test_bind_processor_converts_wkb_to_ewkt(spatial_type, wkb):
    assert cockroachdb_type.bind_processor_process(spatial_type, wkb) == "SRID=4326;POINT (1 2)"


@pytest.mark.parametrize(
    "bindvalue",
    [
        bytes.fromhex("0101000000000000000000f03f0000000000000040"),
        memoryview(bytes.fromhex("0101000000000000000000f03f0000000000000040")),
        WKBElement(bytes.fromhex("0101000000000000000000f03f0000000000000040")),
        WKBElement("0101000000000000000000f03f0000000000000040"),
    ],
)
def test_bind_processor_preserves_wkb_for_wkb_constructor(bindvalue):
    wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
    spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

    assert cockroachdb_type.bind_processor_process(spatial_type, bindvalue) == wkb


@pytest.mark.parametrize(
    ("spatial_type", "bindvalue", "expected"),
    [
        (Geometry(srid=4326), "POINT(1 2)", "SRID=4326;POINT(1 2)"),
        (Geometry(srid=4326), "SRID=3857;POINT(1 2)", "SRID=3857;POINT(1 2)"),
        (Geometry(), "POINT(1 2)", "POINT(1 2)"),
        (Geometry(srid=4326), WKTElement("POINT(1 2)"), "SRID=4326;POINT(1 2)"),
    ],
)
def test_bind_processor_adds_column_srid_to_wkt(spatial_type, bindvalue, expected):
    assert cockroachdb_type.bind_processor_process(spatial_type, bindvalue) == expected


@pytest.mark.parametrize(
    ("column", "message"),
    [
        (Column("rast", Raster()), "does not support Raster columns"),
        (
            Column("geom", Geometry(geometry_type="POINT", srid=4326, use_typmod=False)),
            "does not support managed Geometry columns",
        ),
        (
            Column("geom", Geometry(geometry_type="POINTZ", srid=4326, use_N_D_index=True)),
            "does not support N-D spatial indexes",
        ),
    ],
)
def test_before_create_rejects_unsupported_column_options(column, message):
    table = Table("lake", MetaData(), Column("id", Integer, primary_key=True), column)

    with pytest.raises(ArgumentError, match=message):
        cockroachdb_admin.before_create(table, Bind())


def test_create_spatial_index_rejects_n_d_index():
    table = Table(
        "lake",
        MetaData(),
        Column("geom", Geometry(geometry_type="POINTZ", srid=4326, use_N_D_index=True)),
    )

    with pytest.raises(ArgumentError, match="does not support N-D spatial indexes"):
        cockroachdb_admin.create_spatial_index(Bind(), table, table.c.geom)


def test_alembic_add_geospatial_column_uses_postgresql_path():
    class Impl:
        def __init__(self):
            self.calls = []

        def add_column(self, table_name, column, schema=None):
            self.calls.append((table_name, column.name, schema))

    class Operations:
        def __init__(self):
            self.impl = Impl()

        def get_bind(self):
            return Bind()

    operations = Operations()
    operation = alembic_helpers.AddGeospatialColumnOp(
        "lake",
        Column("geom", Geometry(geometry_type="POINT", srid=4326)),
        schema="gis",
    )

    alembic_helpers.add_geospatial_column(operations, operation)

    assert operations.impl.calls == [("lake", "geom", "gis")]


@pytest.mark.parametrize(
    ("column", "message"),
    [
        (Column("rast", Raster()), "does not support Raster columns"),
        (
            Column("geom", Geometry(geometry_type="POINT", srid=4326, use_typmod=False)),
            "does not support managed Geometry columns",
        ),
        (
            Column("geom", Geometry(geometry_type="POINTZ", srid=4326, use_N_D_index=True)),
            "does not support N-D spatial indexes",
        ),
    ],
)
def test_alembic_add_geospatial_column_rejects_unsupported_options(column, message):
    class Operations:
        def get_bind(self):
            return Bind()

    operation = alembic_helpers.AddGeospatialColumnOp("lake", column)

    with pytest.raises(ArgumentError, match=message):
        alembic_helpers.add_geospatial_column(Operations(), operation)


def test_alembic_create_geospatial_index_rejects_n_d_index():
    class Operations:
        migration_context = None

        def get_bind(self):
            return Bind()

    operation = alembic_helpers.CreateGeospatialIndexOp(
        "idx_lake_geom",
        "lake",
        [Column("geom", Geometry(geometry_type="POINTZ", srid=4326, use_N_D_index=True))],
    )

    with pytest.raises(ArgumentError, match="does not support N-D spatial indexes"):
        alembic_helpers.create_geospatial_index(Operations(), operation)
