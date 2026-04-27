"""This module defines specific functions for CockroachDB dialect."""

from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape

__all__ = ["bind_processor_process"]


def _get_srid(spatial_type):
    return getattr(spatial_type, "srid", -1)


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return f"{bindvalue.data}"
        else:
            srid = bindvalue.srid if bindvalue.srid >= 0 else _get_srid(spatial_type)
            if srid >= 0:
                return f"SRID={srid};{bindvalue.data}"
            return f"{bindvalue.data}"
    elif isinstance(bindvalue, WKBElement):
        shape = to_shape(bindvalue)
        srid = bindvalue.srid if bindvalue.srid >= 0 else _get_srid(spatial_type)
        if srid >= 0:
            return f"SRID={srid};{shape.wkt}"
        return shape.wkt
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, str):
        if bindvalue.startswith("SRID=") or _get_srid(spatial_type) < 0:
            return bindvalue
        return f"SRID={_get_srid(spatial_type)};{bindvalue}"
    else:
        return bindvalue
