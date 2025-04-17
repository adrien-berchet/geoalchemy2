from __future__ import annotations

import binascii
import re
import struct
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import TypeVar
from typing import cast

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import functions
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.types import to_instance

from geoalchemy2.exc import ArgumentError

BinasciiError = binascii.Error

function_registry: Set[str] = set()


class _SpatialElement:
    """The base class for public spatial elements.

    Args:
        data: The first argument passed to the constructor is the data wrapped
            by the ``_SpatialElement`` object being constructed.
        srid: An integer representing the spatial reference system. E.g. ``4326``.
            Default value is ``-1``, which means no/unknown reference system.
        extended: A boolean indicating whether the extended format (EWKT or EWKB)
            is used. Default is ``None``.

    """

    def __init__(self, data, srid: int = -1, extended: Optional[bool] = None) -> None:
        self.srid = srid
        self.data = data
        self.extended = extended

    def __str__(self) -> str:
        return self.desc

    def __repr__(self) -> str:
        return "<%s at 0x%x; %s>" % (
            self.__class__.__name__,
            id(self),
            self,
        )  # pragma: no cover

    def __eq__(self, other) -> bool:
        try:
            return (
                self.extended == other.extended
                and self.srid == other.srid
                and self.desc == other.desc
            )
        except AttributeError:
            return False

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.desc, self.srid, self.extended))

    def __getattr__(self, name: str) -> Any:
        #
        # This is how things like lake.geom.ST_Buffer(2) creates
        # SQL expressions of this form:
        #
        # ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)
        #

        # Raise an AttributeError when the attribute name doesn't start
        # with st_. This is to be nice with other libraries that use
        # some ducktyping (e.g. hasattr(element, "copy")) to determine
        # the type of the element.

        if name.lower() not in function_registry:
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also GenericFunction above.
        func_ = functions._FunctionGenerator(expr=self)
        return getattr(func_, name)

    def __getstate__(self) -> Dict[str, Any]:
        state = {
            "srid": self.srid,
            "data": str(self),
            "extended": self.extended,
        }
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.srid = state["srid"]
        self.extended = state["extended"]
        self.data = self._data_from_desc(state["data"])

    @staticmethod
    def _data_from_desc(desc: str) -> Any:
        """Convert description string to data format.
        
        Must be implemented by subclasses.
        
        Args:
            desc: Description string
            
        Returns:
            The data in appropriate format
        """
        raise NotImplementedError()  # pragma: no cover


class WKTElement(_SpatialElement):
    """Instances of this class wrap a WKT or EWKT value.

    Usage examples::

        wkt_element_1 = WKTElement('POINT(5 45)')
        wkt_element_2 = WKTElement('POINT(5 45)', srid=4326)
        wkt_element_3 = WKTElement('SRID=4326;POINT(5 45)', extended=True)
    """

    _REMOVE_SRID = re.compile("(SRID=([0-9]+); ?)?(.*)")
    SPLIT_WKT_PATTERN = re.compile(r"((SRID=\d+) *; *)?([\w ]+) *(\([-\d\. ,\(\)]+\))")

    geom_from: str = "ST_GeomFromText"
    geom_from_extended_version: str = "ST_GeomFromEWKT"

    def __init__(self, data: str, srid: int = -1, extended: Optional[bool] = None) -> None:
        if extended is None:
            extended = data.startswith("SRID=")
        if extended and srid == -1:
            # read srid from EWKT
            data_s = data.split(";")
            if len(data_s) != 2:
                raise ArgumentError("invalid EWKT string {}".format(data))
            header = data_s[0]
            try:
                srid = int(header[5:])
            except ValueError:
                raise ArgumentError("invalid EWKT string {}".format(data))
        _SpatialElement.__init__(self, data, srid, extended)

    @property
    def desc(self) -> str:
        """This element's description string."""
        return self.data

    @staticmethod
    def _data_from_desc(desc: str) -> str:
        """Convert description string to data format.
        
        For WKTElement, description is the data.
        
        Args:
            desc: WKT string
            
        Returns:
            The WKT string unchanged
        """
        return desc

    def as_wkt(self) -> WKTElement:
        if self.extended:
            srid_match = self._REMOVE_SRID.match(self.data)
            assert srid_match is not None
            return WKTElement(srid_match.group(3), self.srid, extended=False)
        return WKTElement(self.data, self.srid, self.extended)

    def as_ewkt(self) -> WKTElement:
        if not self.extended and self.srid != -1:
            data = f"SRID={self.srid};" + self.data
            return WKTElement(data, extended=True)
        return WKTElement(self.data, self.srid, self.extended)


class WKBElement(_SpatialElement):
    """Instances of this class wrap a WKB or EWKB value.

    Geometry values read from the database are converted to instances of this
    type. In most cases you won't need to create ``WKBElement`` instances
    yourself.

    If ``extended`` is ``True`` and ``srid`` is ``-1`` at construction time
    then the SRID will be read from the EWKB data.

    Note: you can create ``WKBElement`` objects from Shapely geometries
    using the :func:`geoalchemy2.shape.from_shape` function.
    """

    geom_from: str = "ST_GeomFromWKB"
    geom_from_extended_version: str = "ST_GeomFromEWKB"

    def __init__(
        self, data: str | bytes | memoryview, srid: int = -1, extended: Optional[bool] = None
    ) -> None:
        if srid == -1 or extended is None or extended:
            # read srid from the EWKB
            #
            # WKB struct {
            #    byte    byteOrder;
            #    uint32  wkbType;
            #    uint32  SRID;
            #    struct  geometry;
            # }
            # byteOrder enum {
            #     WKB_XDR = 0,  // Most Significant Byte First
            #     WKB_NDR = 1,  // Least Significant Byte First
            # }
            # See https://trac.osgeo.org/postgis/browser/branches/3.0/doc/ZMSgeoms.txt
            # for more details about WKB/EWKB specifications.
            if isinstance(data, str):
                # SpatiaLite case
                # assume that the string is an hex value
                header = binascii.unhexlify(data[:18])
            else:
                header = data[:9]
            byte_order, wkb_type, wkb_srid = header[0], header[1:5], header[5:]
            byte_order_marker = "<I" if byte_order else ">I"
            wkb_type_int = (
                int(struct.unpack(byte_order_marker, wkb_type)[0]) if len(wkb_type) == 4 else 0
            )
            if extended is None:
                if not wkb_type_int:
                    extended = False
                else:
                    extended = extended or bool(wkb_type_int & 536870912)  # Check SRID bit
            if extended and srid == -1:
                wkb_srid = struct.unpack(byte_order_marker, wkb_srid)[0]
                srid = int(wkb_srid)
        _SpatialElement.__init__(self, data, srid, extended)

    @staticmethod
    def _wkb_to_hex(data: str | bytes | memoryview) -> str:
        """Convert WKB to hex string."""
        if isinstance(data, str):
            # SpatiaLite case
            return data.lower()
        return str(binascii.hexlify(data), encoding="utf-8").lower()

    @property
    def desc(self) -> str:
        """This element's description string."""
        return self._wkb_to_hex(self.data)

    @staticmethod
    def _data_from_desc(desc: str) -> bytes:
        """Convert description string to binary data.
        
        For WKBElement, description is a hex string.
        
        Args:
            desc: Hex string
            
        Returns:
            Binary data decoded from hex string
        """
        desc = desc.encode(encoding="utf-8")
        return binascii.unhexlify(desc)

    def as_wkb(self) -> WKBElement:
        if self.extended:
            if isinstance(self.data, str):
                # SpatiaLite case
                # assume that the string is an hex value
                is_hex = True
                header = binascii.unhexlify(self.data[:10])
                byte_order, wkb_type = header[0], header[1:5]
            else:
                is_hex = False
                byte_order, wkb_type = self.data[0], self.data[1:5]

            byte_order_marker = "<I" if byte_order else ">I"
            wkb_type_int = (
                int(struct.unpack(byte_order_marker, wkb_type)[0]) if len(wkb_type) == 4 else 0
            )
            wkb_type_int &= 3758096383  # Set SRID bit to 0 and keep all other bits

            if is_hex:
                wkb_type_hex = binascii.hexlify(
                    wkb_type_int.to_bytes(4, "little" if byte_order else "big")
                )
                data = self.data[:2] + wkb_type_hex.decode("ascii") + self.data[18:]
            else:
                buffer = bytearray()
                buffer.extend(self.data[:1])
                buffer.extend(struct.pack(byte_order_marker, wkb_type_int))
                buffer.extend(self.data[9:])
                data = memoryview(buffer)
            return WKBElement(data, self.srid, extended=False)
        return WKBElement(self.data, self.srid)

    def as_ewkb(self) -> WKBElement:
        if not self.extended and self.srid != -1:
            if isinstance(self.data, str):
                # SpatiaLite case
                # assume that the string is an hex value
                header = binascii.unhexlify(self.data[:10])
                byte_order, wkb_type = header[0], header[1:5]
            else:
                byte_order, wkb_type = self.data[0], self.data[1:5]
            byte_order_marker = "<I" if byte_order else ">I"
            wkb_type_int = int(
                struct.unpack(byte_order_marker, wkb_type)[0] if len(wkb_type) == 4 else 0
            )
            wkb_type_int |= 536870912  # Set SRID bit to 1 and keep all other bits

            data: str | memoryview
            if isinstance(self.data, str):
                wkb_type_hex = binascii.hexlify(
                    wkb_type_int.to_bytes(4, "little" if byte_order else "big")
                )
                wkb_srid_hex = binascii.hexlify(
                    self.srid.to_bytes(4, "little" if byte_order else "big")
                )
                data = (
                    self.data[:2]
                    + wkb_type_hex.decode("ascii")
                    + wkb_srid_hex.decode("ascii")
                    + self.data[10:]
                )
            else:
                buffer = bytearray()
                buffer.extend(self.data[:1])
                buffer.extend(struct.pack(byte_order_marker, wkb_type_int))
                buffer.extend(struct.pack(byte_order_marker, self.srid))
                buffer.extend(self.data[5:])
                data = memoryview(buffer)

            return WKBElement(data, self.srid, extended=True)
        return WKBElement(self.data, self.srid)

    def __str__(self) -> str:
        return self.desc

    def _compiler_dispatch(self, visitor: Any, **kw: Any) -> Any:
        return visitor.process(self.bind_expression())


class RasterElement(_SpatialElement):
    """Instances of this class wrap a ``raster`` value.

    Raster values read from the database are converted to instances of this type. In
    most cases you won't need to create ``RasterElement`` instances yourself.

    Args:
        data: The raster data in either binary (bytes/memoryview) or
            hexadecimal string format.
    """

    geom_from_extended_version: str = "raster"

    def __init__(self, data: str | bytes | memoryview) -> None:
        # read srid from the WKB (binary or hexadecimal format)
        # The WKB structure is documented in the file
        # raster/doc/RFC2-WellKnownBinaryFormat of the PostGIS sources.
        # Header format:
        # - byte 0: endianness (0=big, 1=little)
        # - bytes 1-2: version (16-bit unsigned)
        # - bytes 3-4: number of bands (16-bit unsigned)
        # - bytes 5-12: scale X (double)
        # - bytes 13-20: scale Y (double)
        # - bytes 21-28: IP X (double)
        # - bytes 29-36: IP Y (double)
        # - bytes 37-44: skew X (double)
        # - bytes 45-52: skew Y (double)
        # - bytes 53-56: SRID (32-bit signed)
        # - bytes 57-58: width (16-bit unsigned)
        # - bytes 59-60: height (16-bit unsigned)

        # We need at least 61 bytes for a minimal raster header
        MIN_HEADER_SIZE = 61
        
        # Convert hex string to binary if needed
        bin_data: bytes | memoryview
        hex_input = False
        
        if isinstance(data, str):
            hex_input = True
            try:
                # Need at least MIN_HEADER_SIZE*2 hex chars for a valid header
                if len(data) < MIN_HEADER_SIZE * 2:
                    raise ValueError(f"Raster hex data too short: {len(data)} chars, need at least {MIN_HEADER_SIZE * 2}")
                bin_data = binascii.unhexlify(data[:MIN_HEADER_SIZE * 2])
            except BinasciiError as e:
                raise ValueError(f"Invalid hex data for raster: {e}") from e
        else:
            # Handle binary input (bytes or memoryview)
            if len(data) < MIN_HEADER_SIZE:
                raise ValueError(f"Raster binary data too short: {len(data)} bytes, need at least {MIN_HEADER_SIZE}")
            bin_data = data
            # Convert binary to hex string for storage
            data_str = str(binascii.hexlify(data if isinstance(data, bytes) else bytes(data)).decode(encoding="utf-8"))

        # Extract endianness
        try:
            byte_order = bin_data[0]
            if byte_order not in (0, 1):
                raise ValueError(f"Invalid byte order in raster: {byte_order}, must be 0 or 1")
                
            # Determine the format string for struct.unpack
            endian_fmt = "<" if byte_order == 1 else ">"
            
            # Extract SRID (bytes 53-56)
            srid_bytes = bin_data[53:57]
            if len(srid_bytes) != 4:
                raise ValueError(f"Could not extract SRID bytes from raster data, got {len(srid_bytes)} bytes")
                
            srid = struct.unpack(f"{endian_fmt}i", srid_bytes)[0]
            
            # Store the data
            if hex_input:
                # If input was hex, use the original string
                _SpatialElement.__init__(self, data, int(srid), True)
            else:
                # If input was binary, use our hex string
                _SpatialElement.__init__(self, data_str, int(srid), True)
                
        except (IndexError, struct.error) as e:
            raise ValueError(f"Error parsing raster data: {e}") from e

    @property
    def desc(self) -> str:
        """This element's description string."""
        return self.data

    @staticmethod
    def _data_from_desc(desc: str) -> str:
        """Convert description string to data format.
        
        For RasterElement, description is the data (hex format).
        
        Args:
            desc: Raster data in hex format
            
        Returns:
            The hex string unchanged
        """
        return desc


class CompositeElement(FunctionElement):
    """Instances of this class wrap a Postgres composite type."""

    inherit_cache: bool = False
    """The cache is disabled for this class."""

    def __init__(self, base: Any, field: str, type_: Any) -> None:
        """Initialize CompositeElement.
        
        Args:
            base: Base SQL element
            field: Field name to access
            type_: SQLAlchemy type
        """
        self.name = field
        self.type = to_instance(type_)

        super(CompositeElement, self).__init__(base)


@compiles(CompositeElement)
def _compile_pgelem(expr: CompositeElement, compiler: Any, **kw: Any) -> str:
    """Compile CompositeElement to SQL.
    
    Args:
        expr: CompositeElement to compile
        compiler: SQLAlchemy SQL compiler
        **kw: Additional keyword arguments
        
    Returns:
        Compiled SQL string
    """
    return "(%s).%s" % (compiler.process(expr.clauses, **kw), expr.name)


__all__: List[str] = [
    "_SpatialElement",
    "CompositeElement",
    "RasterElement",
    "WKBElement",
    "WKTElement",
]


def __dir__() -> List[str]:
    return __all__
