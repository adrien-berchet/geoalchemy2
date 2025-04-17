"""
Decipher Raster
===============

The `RasterElement` objects store the Raster data in WKB form. When using rasters it is
usually better to convert them into TIFF, PNG, JPEG or whatever. Nevertheless, it is
possible to decipher the WKB to get a 2D list of values.
This example uses SQLAlchemy ORM queries.
"""

import binascii
import struct

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

from geoalchemy2 import Raster
from geoalchemy2 import WKTElement

# Tests imports
from tests import test_only_with_dialects

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Ocean(Base):  # type: ignore
    __tablename__ = "ocean"
    id = Column(Integer, primary_key=True)
    rast = Column(Raster)

    def __init__(self, rast):
        self.rast = rast


def _format_e(endianness, struct_format):
    """Format endianness prefix for struct.
    
    Args:
        endianness: 0 for big endian, 1 for little endian
        struct_format: Format string for struct.unpack
        
    Returns:
        Format string with endianness prefix
    """
    if endianness not in (0, 1):
        raise ValueError(f"Invalid endianness value: {endianness}, must be 0 or 1")
    return _ENDIANNESS[endianness] + struct_format


def wkbHeader(raw):
    """Function to decipher the WKB header.
    
    See http://trac.osgeo.org/postgis/browser/trunk/raster/doc/RFC2-WellKnownBinaryFormat
    
    Args:
        raw: Raw binary data of the raster
        
    Returns:
        Dictionary containing the parsed header values
        
    Raises:
        ValueError: If the data is invalid or too short
    """
    # We need at least 61 bytes for a complete header
    MIN_HEADER_SIZE = 61
    
    if len(raw) < MIN_HEADER_SIZE:
        raise ValueError(f"Raster data too short: {len(raw)} bytes, need at least {MIN_HEADER_SIZE}")
    
    header = {}

    try:
        # The first byte indicates endianness (0=big, 1=little)
        header["endianness"] = struct.unpack("b", raw[0:1])[0]
        
        if header["endianness"] not in (0, 1):
            raise ValueError(f"Invalid endianness value: {header['endianness']}, must be 0 or 1")

        e = header["endianness"]
        header["version"] = struct.unpack(_format_e(e, "H"), raw[1:3])[0]
        header["nbands"] = struct.unpack(_format_e(e, "H"), raw[3:5])[0]
        header["scaleX"] = struct.unpack(_format_e(e, "d"), raw[5:13])[0]
        header["scaleY"] = struct.unpack(_format_e(e, "d"), raw[13:21])[0]
        header["ipX"] = struct.unpack(_format_e(e, "d"), raw[21:29])[0]
        header["ipY"] = struct.unpack(_format_e(e, "d"), raw[29:37])[0]
        header["skewX"] = struct.unpack(_format_e(e, "d"), raw[37:45])[0]
        header["skewY"] = struct.unpack(_format_e(e, "d"), raw[45:53])[0]
        header["srid"] = struct.unpack(_format_e(e, "i"), raw[53:57])[0]
        header["width"] = struct.unpack(_format_e(e, "H"), raw[57:59])[0]
        header["height"] = struct.unpack(_format_e(e, "H"), raw[59:61])[0]
        
        # Validate reasonable width and height
        if header["width"] == 0 or header["height"] == 0:
            raise ValueError(f"Invalid dimensions: width={header['width']}, height={header['height']}")
            
        # Validate band count
        if header["nbands"] == 0:
            raise ValueError("Raster must have at least one band")
            
        return header
        
    except struct.error as e:
        raise ValueError(f"Failed to parse raster header: {e}")


def read_band(data, offset, pixtype, height, width, endianness=1):
    """Read a band of pixel data from the raster.
    
    Args:
        data: The raw binary data
        offset: Offset in bytes to the start of the band
        pixtype: Pixel type code (0-11)
        height: Height of the raster in pixels
        width: Width of the raster in pixels
        endianness: 0 for big endian, 1 for little endian
        
    Returns:
        2D list of pixel values
        
    Raises:
        ValueError: If pixel type is invalid or data is insufficient
    """
    if pixtype not in _PTYPE:
        raise ValueError(f"Invalid pixel type: {pixtype}")
        
    ptype, _, psize = _PTYPE[pixtype]
    
    # Calculate required data size
    required_size = offset + 1 + (width * height * psize)
    if len(data) < required_size:
        raise ValueError(
            f"Raster data too short for band: len={len(data)}, required={required_size} "
            f"(offset={offset}, width={width}, height={height}, pixelsize={psize})"
        )
        
    try:
        pix_data = data[offset + 1 : offset + 1 + width * height * psize]
        band = [
            [
                struct.unpack(
                    _format_e(endianness, ptype),
                    pix_data[(i * width + j) * psize : (i * width + j + 1) * psize],
                )[0]
                for j in range(width)
            ]
            for i in range(height)
        ]
        return band
    except struct.error as e:
        raise ValueError(f"Failed to unpack pixel data: {e}")


def read_band_numpy(data, offset, pixtype, height, width, endianness=1):
    """Read a band of pixel data from the raster using numpy.
    
    Args:
        data: The raw binary data
        offset: Offset in bytes to the start of the band
        pixtype: Pixel type code (0-11)
        height: Height of the raster in pixels
        width: Width of the raster in pixels
        endianness: 0 for big endian, 1 for little endian
        
    Returns:
        Numpy 2D array of pixel values
        
    Raises:
        ValueError: If pixel type is invalid or data is insufficient
    """
    import numpy as np  # noqa
    
    if pixtype not in _PTYPE:
        raise ValueError(f"Invalid pixel type: {pixtype}")
        
    _, dtype, psize = _PTYPE[pixtype]
    
    # Calculate required data size
    required_size = offset + 1 + (width * height * psize)
    if len(data) < required_size:
        raise ValueError(
            f"Raster data too short for numpy band: len={len(data)}, required={required_size} "
            f"(offset={offset}, width={width}, height={height}, pixelsize={psize})"
        )
    
    try:
        dt = np.dtype(dtype)
        dt = dt.newbyteorder(_ENDIANNESS[endianness])
        band = np.frombuffer(data, dtype=dt, count=height * width, offset=offset + 1)
        band = np.reshape(band, ((height, width)))
        return band
    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to create numpy array from raster data: {e}")


# Pixel type codes from PostGIS documentation
# Format: [struct format char, numpy dtype string, size in bytes]
_PTYPE = {
    0: ["?", "?", 1],     # 1BB - 1-bit boolean
    1: ["B", "B", 1],     # 2BUI - 2-bit unsigned integer
    2: ["B", "B", 1],     # 4BUI - 4-bit unsigned integer
    3: ["b", "b", 1],     # 8BSI - 8-bit signed integer
    4: ["B", "B", 1],     # 8BUI - 8-bit unsigned integer
    5: ["h", "i2", 2],    # 16BSI - 16-bit signed integer
    6: ["H", "u2", 2],    # 16BUI - 16-bit unsigned integer
    7: ["i", "i4", 4],    # 32BSI - 32-bit signed integer
    8: ["I", "u4", 4],    # 32BUI - 32-bit unsigned integer
    10: ["f", "f4", 4],   # 32BF - 32-bit float
    11: ["d", "f8", 8],   # 64BF - 64-bit float
    # 9: Not used in PostGIS
}

# Endianness markers for struct.unpack
_ENDIANNESS = {
    0: ">",  # Big endian
    1: "<",  # Little endian
}
def wkbImage(raster_data, use_numpy=False):
    """Function to decipher a raster WKB into a list of bands.
    
    Args:
        raster_data: Raster data in WKB/EWKB hex format
        use_numpy: Whether to use NumPy for band reading (faster)
        
    Returns:
        List of bands, each containing a 2D representation of the raster
        
    Raises:
        ValueError: If the data is invalid or cannot be decoded
    """
    # Get binary data
    try:
        if isinstance(raster_data, str):
            raw = binascii.unhexlify(raster_data)
        else:
            raw = bytes(raster_data)
    except (TypeError, binascii.Error) as e:
        raise ValueError(f"Failed to decode raster data: {e}")

    # Read header
    h = wkbHeader(raw)
    e = h["endianness"]

    img = []  # array to store image bands
    offset = 61  # header raw length in bytes
    
    # Process each band
    for i in range(h["nbands"]):
        # Check if we have enough data for the band header
        if offset + 1 >= len(raw):
            raise ValueError(f"Raster data too short for band {i+1} header")
            
        # Determine pixtype for this band
        # In PostGIS, band types are encoded as (pixtype + 64)
        try:
            band_type = struct.unpack(_format_e(e, "b"), raw[offset : offset + 1])[0]
            pixtype = band_type - 64  # Subtract 64 to get the actual pixel type
            
            if pixtype not in _PTYPE:
                raise ValueError(f"Invalid pixel type {pixtype} (band type {band_type}) for band {i+1}")
        except struct.error as e:
            raise ValueError(f"Failed to read band {i+1} type: {e}")
            
        # Get pixel size for calculating band data size
        _, _, psize = _PTYPE[pixtype]
        
        # Read data with either pure Python or Numpy
        try:
            if use_numpy:
                band = read_band_numpy(raw, offset, pixtype, h["height"], h["width"], e)
            else:
                band = read_band(raw, offset, pixtype, h["height"], h["width"], e)
            
            # Store the result
            img.append(band)
        except ValueError as e:
            raise ValueError(f"Failed to read band {i+1}: {e}")
            
        # Move to the next band
        band_data_size = h["width"] * h["height"] * psize
        offset = offset + 1 + band_data_size
        
    return img


@test_only_with_dialects("postgresql")
class TestDecipherRaster:
    @pytest.mark.parametrize(
        "pixel_type",
        [
            "1BB",
            "2BUI",
            "4BUI",
            "8BSI",
            "8BUI",
            "16BSI",
            "16BUI",
            "32BSI",
            "32BUI",
            "32BF",
            "64BF",
        ],
    )
    def test_decipher_raster(self, pixel_type, session, conn):
        """Create a raster and decipher it"""
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        # Create a new raster
        polygon = WKTElement("POLYGON((0 0,1 1,0 1,0 0))", srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6, pixel_type))
        session.add(o)
        session.flush()

        # Define expected result - this is the polygon rasterized to a 5x6 grid
        expected = [
            [0, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            [0, 1, 1, 1, 0],
            [0, 1, 1, 0, 0],
            [0, 1, 0, 0, 0],
            [0, 0, 0, 0, 0],
        ]

        try:
            # Decipher data from the raster
            image = wkbImage(o.rast.data)
            
            # Check results - first band should match the expected pattern
            band = image[0]
            
            # For floating point types, we need to convert to 0/1 for comparison
            if pixel_type in ('32BF', '64BF'):
                # Convert floats to 0/1 based on whether they're close to 0 or 1
                band = [[round(cell) for cell in row] for row in band]
                
            assert band == expected, f"Failed for pixel type {pixel_type}"
        except Exception as e:
            pytest.fail(f"Failed to process {pixel_type} raster: {e}")
