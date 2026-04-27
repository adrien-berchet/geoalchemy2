.. _cockroachdb_dialect:

CockroachDB Tutorial
====================

GeoAlchemy 2 supports CockroachDB through the external
``sqlalchemy-cockroachdb`` SQLAlchemy dialect. CockroachDB's spatial SQL support is largely
PostGIS-compatible for ``Geometry`` and ``Geography`` columns, so GeoAlchemy 2 reuses its
PostgreSQL bind processing and function compilation for this dialect.

Connect to the DB
-----------------

Install the SQLAlchemy dialect version that matches the SQLAlchemy version in your environment.
For SQLAlchemy 2.x use the current ``sqlalchemy-cockroachdb`` release. For SQLAlchemy 1.4 use a
``sqlalchemy-cockroachdb`` release below 2.0.

An engine can be created with the GeoAlchemy 2 plugin in the same way as other server dialects::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine(
    ...     "cockroachdb://user@host:26257/dbname?sslmode=disable",
    ...     echo=True,
    ...     plugins=["geoalchemy2"],
    ... )

Supported Spatial Types
-----------------------

``Geometry`` and ``Geography`` columns are supported, including standard spatial indexes using
``USING gist``. CockroachDB documents these spatial types and indexes in its spatial data and
spatial index documentation.

CockroachDB does not provide PostGIS raster support, so ``Raster`` columns are not supported by
the CockroachDB dialect.

Unsupported PostgreSQL/PostGIS Options
--------------------------------------

Some PostgreSQL/PostGIS-specific options are not available with CockroachDB:

* ``Raster`` columns are rejected.
* Managed geometry columns using ``use_typmod=False`` are rejected because CockroachDB does not
  support PostGIS ``AddGeometryColumn`` / ``DropGeometryColumn`` management functions.
* N-D spatial indexes using ``use_N_D_index=True`` are rejected because CockroachDB does not
  provide the PostgreSQL ``gist_geometry_ops_nd`` operator class.

Use normal typmod-based ``Geometry`` / ``Geography`` columns and standard spatial indexes for
portable CockroachDB usage.

See also:

* `CockroachDB spatial data documentation <https://www.cockroachlabs.com/docs/stable/query-spatial-data>`_
* `CockroachDB spatial index documentation <https://www.cockroachlabs.com/docs/stable/spatial-indexes>`_
