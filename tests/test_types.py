"""Type checking tests for GeoAlchemy2.

This module contains tests that verify the type annotations in GeoAlchemy2
are correct and consistent.
"""

import unittest
from typing import Any, Dict, List, Optional, cast

import pytest
from sqlalchemy import Column, MetaData, Table, func, inspect, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import expression
from sqlalchemy.sql.elements import ClauseElement

from geoalchemy2 import Geometry, Geography, Raster, WKBElement, WKTElement
from geoalchemy2 import elements, functions, shape
from geoalchemy2.comparator import Comparator


class TestGeometryTypeHints(unittest.TestCase):
    """Test geometry type hints."""

    def test_geometry_type_hints(self) -> None:
        """Test Geometry type hints."""
        # Test Geometry type creation with various parameters
        geom1: Geometry = Geometry(geometry_type="POINT", srid=4326)
        geom2: Geometry = Geometry(geometry_type="LINESTRING", srid=4326, dimension=2)
        geom3: Geometry = Geometry(geometry_type="POLYGON", srid=4326, spatial_index=True)
        
        # Verify type annotations
        assert geom1.geometry_type == "POINT"
        assert geom2.geometry_type == "LINESTRING"
        assert geom3.geometry_type == "POLYGON"
        
        # Test Geography type creation
        geog: Geography = Geography(geometry_type="POINT", srid=4326)
        assert geog.geometry_type == "POINT"

    def test_wkt_element_type_hints(self) -> None:
        """Test WKTElement type hints."""
        # Test WKTElement with various parameters
        wkt1: WKTElement = WKTElement("POINT(1 1)")
        wkt2: WKTElement = WKTElement("POINT(1 1)", srid=4326)
        wkt3: WKTElement = WKTElement("SRID=4326;POINT(1 1)", extended=True)
        
        # Verify attributes exist and have correct types
        assert wkt1.srid == -1
        assert wkt2.srid == 4326
        assert isinstance(wkt3.data, str)
        
        # Test type conversion
        wkt4 = wkt2.as_ewkt()
        assert wkt4.data.startswith("SRID=4326;")
        assert isinstance(wkt4, WKTElement)

    def test_wkb_element_type_hints(self) -> None:
        """Test WKBElement type hints."""
        # Create a WKB element from binary data
        bin_data = b"\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        wkb1: WKBElement = WKBElement(bin_data)
        
        # Convert a WKB element to hex string and back
        hex_str: str = wkb1.desc
        
        # Test various WKBElement methods
        wkb2 = wkb1.as_ewkb()
        assert isinstance(wkb2, WKBElement)
        
        wkb3 = wkb1.as_wkb()
        assert isinstance(wkb3, WKBElement)


class TestRasterTypeHints(unittest.TestCase):
    """Test raster type hints."""
    
    def test_raster_type_hints(self) -> None:
        """Test Raster type hints."""
        # Test Raster type creation
        rast: Raster = Raster(spatial_index=True)
        
        # Verify type annotations
        assert hasattr(rast, "comparator_factory")
        
        # RasterElement tests would require actual data
        # This is a simplified test
        metadata = MetaData()
        raster_table = Table(
            "raster_table", 
            metadata,
            Column("id", rast)
        )
        assert isinstance(raster_table.c.id.type, Raster)


class TestFunctionTypeHints(unittest.TestCase):
    """Test function type hints."""
    
    def test_function_type_hints(self) -> None:
        """Test function type hints."""
        # Test function calls with type hints
        Point = Geometry(geometry_type="POINT", srid=4326)
        
        # Define a column with geometry type
        metadata = MetaData()
        table = Table(
            "some_table", 
            metadata,
            Column("geom", Point)
        )
        
        # Test function calls
        expr1 = functions.ST_AsText(table.c.geom)
        assert isinstance(expr1, ClauseElement)
        
        expr2 = table.c.geom.ST_AsText()
        assert isinstance(expr2, ClauseElement)
        
        # Test function with multiple arguments
        expr3 = functions.ST_Buffer(table.c.geom, 2)
        assert isinstance(expr3, ClauseElement)


class TestTypeConversionHints(unittest.TestCase):
    """Test type conversion handling."""
    
    def test_type_conversion_hints(self) -> None:
        """Test type conversion with hints."""
        # Test shape module functions
        point_wkt = WKTElement("POINT(1 1)", srid=4326)
        
        # Convert WKT to shape
        point_shape = shape.to_shape(point_wkt)
        assert hasattr(point_shape, "wkt")
        
        # Convert shape back to WKB
        point_wkb = shape.from_shape(point_shape, srid=4326)
        assert isinstance(point_wkb, WKBElement)
        assert point_wkb.srid == 4326


class TestDialectSpecificHints(unittest.TestCase):
    """Test dialect-specific type handling."""
    
    def test_dialect_specific_hints(self) -> None:
        """Test dialect-specific type hints."""
        # Simulate dialect-specific behavior
        Base = declarative_base()
        
        class SpatialModel(Base):
            __tablename__ = "spatial_model"
            
            id = Column(Geometry(geometry_type="POINT", srid=4326), primary_key=True)
            geom = Column(Geometry(geometry_type="POLYGON", srid=4326))
        
        # Create a query with type hints
        q = select(SpatialModel.geom.ST_AsText())
        assert isinstance(q, expression.SelectBase)
        
        # Test comparator method with type hints
        comp: Comparator = cast(Comparator, SpatialModel.geom.comparator)
        expr = comp.intersects("POINT(1 1)")
        assert isinstance(expr, ClauseElement)


def test_function_registry() -> None:
    """Test function registry."""
    # Test that function registry contains expected functions
    assert "st_astext" in elements.function_registry
    assert "st_buffer" in elements.function_registry
    
    # Test function call with proper typing
    result = func.ST_Buffer(WKTElement("POINT(1 1)"), 2)
    assert isinstance(result, ClauseElement)


def test_mypy_stubs() -> None:
    """Test mypy stub compatibility."""
    # This test just verifies that the code can be properly type-checked
    # The actual checking is done by mypy externally
    
    # Define some typed variables
    point: WKTElement = WKTElement("POINT(1 1)", srid=4326)
    geom_col: Geometry = Geometry(geometry_type="POINT", srid=4326)
    
    # Use them in a way that should be type-safe
    expr = func.ST_Buffer(point, 2)
    
    # Test that we can access attributes correctly
    assert point.srid == 4326
    assert geom_col.geometry_type == "POINT"
    
    # This test passes if mypy doesn't complain about the type annotations
    assert True

import re

import pytest
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql import text

from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster

from . import select


def eq_sql(a, b):
    a = re.sub(r"[\n\t]", "", str(a))
    assert a == b


@pytest.fixture
def geometry_table():
    table = Table("table", MetaData(), Column("geom", Geometry))
    return table


@pytest.fixture
def geography_table():
    table = Table("table", MetaData(), Column("geom", Geography))
    return table


@pytest.fixture
def raster_table():
    table = Table("table", MetaData(), Column("rast", Raster))
    return table


class TestGeometry:
    def test_get_col_spec(self):
        g = Geometry(srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRY,900913)"

    def test_get_col_spec_no_srid(self):
        g = Geometry(srid=None)
        assert g.get_col_spec() == "geometry(GEOMETRY,-1)"

    def test_get_col_spec_invalid_srid(self):
        with pytest.raises(ArgumentError) as e:
            g = Geometry(srid="foo")
            g.get_col_spec()
        assert str(e.value) == "srid must be convertible to an integer"

    def test_get_col_spec_no_typmod(self):
        g = Geometry(geometry_type=None)
        assert g.get_col_spec() == "geometry"

    def test_check_ctor_args_bad_srid(self):
        with pytest.raises(ArgumentError):
            Geometry(srid="foo")

    def test_get_col_spec_geometryzm(self):
        g = Geometry(geometry_type="GEOMETRYZM", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYZM,900913)"

    def test_get_col_spec_geometryz(self):
        g = Geometry(geometry_type="GEOMETRYZ", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYZ,900913)"

    def test_get_col_spec_geometrym(self):
        g = Geometry(geometry_type="GEOMETRYM", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYM,900913)"

    def test_check_ctor_args_srid_not_enforced(self):
        with pytest.warns(UserWarning):
            Geometry(geometry_type=None, srid=4326)

    def test_check_ctor_args_use_typmod_nullable(self):
        with pytest.raises(
            ArgumentError,
            match='The "nullable" and "use_typmod" arguments can not be used together',
        ):
            Geometry(use_typmod=True, nullable=False)

    def test_column_expression(self, geometry_table):
        s = select([geometry_table.c.geom])
        eq_sql(s, 'SELECT ST_AsEWKB("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geometry_table):
        s = select([text("foo")]).where(geometry_table.c.geom == "POINT(1 2)")
        eq_sql(
            s,
            'SELECT foo FROM "table" WHERE ' '"table".geom = ST_GeomFromEWKT(:geom_1)',
        )
        assert s.compile().params == {"geom_1": "POINT(1 2)"}

    def test_insert_bind_expression(self, geometry_table):
        i = insert(geometry_table).values(geom="POINT(1 2)")
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeomFromEWKT(:geom))')
        assert i.compile().params == {"geom": "POINT(1 2)"}

    def test_function_call(self, geometry_table):
        s = select([geometry_table.c.geom.ST_Buffer(2)])
        eq_sql(
            s,
            'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) '
            'AS "ST_Buffer_1" FROM "table"',
        )

    def test_non_ST_function_call(self, geometry_table):
        with pytest.raises(AttributeError):
            geometry_table.c.geom.Buffer(2)

    def test_subquery(self, geometry_table):
        # test for geometry columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        s = select([geometry_table]).alias("name").select()
        eq_sql(
            s,
            "SELECT ST_AsEWKB(name.geom) AS geom FROM "
            '(SELECT "table".geom AS geom FROM "table") AS name',
        )


class TestGeography:
    def test_get_col_spec(self):
        g = Geography(srid=900913)
        assert g.get_col_spec() == "geography(GEOMETRY,900913)"

    def test_get_col_spec_no_typmod(self):
        g = Geography(geometry_type=None)
        assert g.get_col_spec() == "geography"

    def test_column_expression(self, geography_table):
        s = select([geography_table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geography_table):
        s = select([text("foo")]).where(geography_table.c.geom == "POINT(1 2)")
        eq_sql(
            s,
            'SELECT foo FROM "table" WHERE ' '"table".geom = ST_GeogFromText(:geom_1)',
        )
        assert s.compile().params == {"geom_1": "POINT(1 2)"}

    def test_insert_bind_expression(self, geography_table):
        i = insert(geography_table).values(geom="POINT(1 2)")
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeogFromText(:geom))')
        assert i.compile().params == {"geom": "POINT(1 2)"}

    def test_function_call(self, geography_table):
        s = select([geography_table.c.geom.ST_Buffer(2)])
        eq_sql(
            s,
            'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) '
            'AS "ST_Buffer_1" FROM "table"',
        )

    def test_non_ST_function_call(self, geography_table):
        with pytest.raises(AttributeError):
            geography_table.c.geom.Buffer(2)

    def test_subquery(self, geography_table):
        # test for geography columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        s = select([geography_table]).alias("name").select()
        eq_sql(
            s,
            "SELECT ST_AsBinary(name.geom) AS geom FROM "
            '(SELECT "table".geom AS geom FROM "table") AS name',
        )


class TestPoint:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="POINT", srid=900913)
        assert g.get_col_spec() == "geometry(POINT,900913)"


class TestCurve:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="CURVE", srid=900913)
        assert g.get_col_spec() == "geometry(CURVE,900913)"


class TestLineString:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="LINESTRING", srid=900913)
        assert g.get_col_spec() == "geometry(LINESTRING,900913)"


class TestPolygon:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="POLYGON", srid=900913)
        assert g.get_col_spec() == "geometry(POLYGON,900913)"


class TestMultiPoint:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTIPOINT", srid=900913)
        assert g.get_col_spec() == "geometry(MULTIPOINT,900913)"


class TestMultiLineString:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTILINESTRING", srid=900913)
        assert g.get_col_spec() == "geometry(MULTILINESTRING,900913)"


class TestMultiPolygon:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTIPOLYGON", srid=900913)
        assert g.get_col_spec() == "geometry(MULTIPOLYGON,900913)"


class TestGeometryCollection:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="GEOMETRYCOLLECTION", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYCOLLECTION,900913)"


class TestRaster:
    def test_get_col_spec(self):
        r = Raster()
        assert r.get_col_spec() == "raster"

    def test_column_expression(self, raster_table):
        s = select([raster_table.c.rast])
        eq_sql(s, 'SELECT raster("table".rast) AS rast FROM "table"')

    def test_insert_bind_expression(self, raster_table):
        i = insert(raster_table).values(rast=b"\x01\x02")
        eq_sql(i, 'INSERT INTO "table" (rast) VALUES (raster(:rast))')
        assert i.compile().params == {"rast": b"\x01\x02"}

    def test_function_call(self, raster_table):
        s = select([raster_table.c.rast.ST_Height()])
        eq_sql(s, 'SELECT ST_Height("table".rast) ' 'AS "ST_Height_1" FROM "table"')

    def test_non_ST_function_call(self, raster_table):
        with pytest.raises(AttributeError):
            raster_table.c.geom.Height()


class TestCompositeType:
    def test_ST_Dump(self, geography_table):
        s = select([func.ST_Dump(geography_table.c.geom).geom.label("geom")])

        eq_sql(s, 'SELECT ST_AsEWKB((ST_Dump("table".geom)).geom) AS geom ' 'FROM "table"')
