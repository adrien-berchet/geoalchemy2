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


@pytest.mark.parametrize(
    "wkb",
    [
        WKBElement("0101000000000000000000f03f0000000000000040", srid=4326),
        WKBElement("0101000020e6100000000000000000f03f0000000000000040", extended=True),
    ],
)
def test_bind_processor_converts_wkb_to_ewkt(wkb):
    assert cockroachdb_type.bind_processor_process(Geometry(), wkb) == "SRID=4326;POINT (1 2)"


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
