import pytest
import shapely
from sqlalchemy import Column
from sqlalchemy import Integer

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape

from .. import test_only_with_dialects


ROUNDS = 5


@pytest.fixture(
    params=[
        pytest.param(True, id="Extended"),
        pytest.param(False, id="Not extended")
    ]
)
def is_extended(request):
    """Fixture to determine if the test is for extended or not."""
    return request.param


@pytest.fixture(
    params=[
        pytest.param("WKT"),
        pytest.param("WKB"),
    ]
)
def representation(request):
    """Fixture to determine the representation type."""
    return request.param


@pytest.fixture
def WktTable(base, schema):
    from_text_func = "ST_GeomFromEWKB" if request.param else "ST_GeomFromWKB"

    class WktTable(base):
        __tablename__ = "wkt_table"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="POINTZM", from_text=from_text_func, srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return WktTable


@pytest.fixture
def GeomTable(base, schema, is_extended, representation):
    if representation == "WKB":
        from_text_func = "ST_GeomFromEWKB" if is_extended else "ST_GeomFromWKB"
        to_text_func = "ST_AsEWKB" if is_extended else "ST_AsBinary"
    else:
        from_text_func = "ST_GeomFromEWKT" if is_extended else "ST_GeomFromText"
        to_text_func = "ST_AsEWKT" if is_extended else "ST_AsText"

    class GeomTable(base):
        __tablename__ = "wkb_table"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="POINTZM", from_text=from_text_func, to_text=to_text_func, srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return GeomTable


def create_points(N=50):
    """Create a list of points for benchmarking."""
    points = []
    for i in range(N):
        for j in range(N):
            for k in range(N):
                wkt = f"POINT({i} {j} {k} {i + j + k})"
                points.append(wkt)
    return points


def insert_all_points(conn, table, points):
    """Insert all points into the database."""
    query = table.insert().values(
        [
            {
                "geom": point,
            }
            for point in points
        ]
    )
    return conn.execute(query)


def select_all_points(conn, table):
    """Select all points from the database."""
    query = table.select()
    return conn.execute(query).fetchall()


def insert_and_select_all_points(conn, table, points):
    """Insert all points into the database and select them."""
    insert_all_points(conn, table, points)
    return select_all_points(conn, table)


def _benchmark_setup(conn, table_class, metadata, convert_wkb=False, N=50):
    """Setup the database for benchmarking."""
    # Create the points to insert
    points = create_points(N)
    print(f"Number of points to insert: {len(points)}")

    if convert_wkb:
        # Convert WKT to WKB
        points = [shapely.io.to_wkb(to_shape(WKTElement(point)), flavor="iso") for point in points]
        print(f"Converted points to WKB: {len(points)}")

    # Create the table in the database
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    print(f"Table {table_class.__tablename__} created")

    return points


def _benchmark_insert(conn, table_class, metadata, benchmark, convert_wkb=False, N=50, rounds=5):
    """Benchmark the insert operation."""
    points = _benchmark_setup(conn, table_class, metadata, convert_wkb=convert_wkb, N=N)

    table = table_class.__table__
    return benchmark.pedantic(insert_all_points, args=(conn, table, points), iterations=1, rounds=rounds)


def _benchmark_insert_select(conn, table_class, metadata, benchmark, convert_wkb=False, N=50, rounds=5):
    """Benchmark the insert and select operations."""
    points = _benchmark_setup(conn, table_class, metadata, convert_wkb=convert_wkb, N=N)

    table = table_class.__table__
    return benchmark.pedantic(insert_and_select_all_points, args=(conn, table, points), iterations=1, rounds=rounds)


@pytest.mark.parametrize(
    "N",
    [2, 10, 50],
)
@test_only_with_dialects("postgresql")
def test_insert(benchmark, GeomTable, conn, metadata, N):
    """Benchmark the insert operation."""

    _benchmark_insert(conn, GeomTable, metadata, benchmark, convert_wkb=True, N=N, rounds=ROUNDS)

    assert (
        conn.execute(
            GeomTable.__table__.select().where(GeomTable.__table__.c.geom.is_not(None))
        ).rowcount
        == N * N * N * ROUNDS
    )


@pytest.mark.parametrize(
    "N",
    [2, 10, 50],
)
@test_only_with_dialects("postgresql")
def test_insert_select(benchmark, GeomTable, conn, metadata, N, representation):
    """Benchmark the insert operation."""

    convert_wkb = (representation == "WKB")
    all_points = _benchmark_insert_select(conn, GeomTable, metadata, benchmark, convert_wkb=convert_wkb, N=N, rounds=ROUNDS)

    assert (
        conn.execute(
            GeomTable.__table__.select().where(GeomTable.__table__.c.geom.is_not(None))
        ).rowcount
        == N * N * N * ROUNDS
    )
    assert len(all_points) == N * N * N * ROUNDS
