#!/usr/bin/env python3
"""Self-check for raster_digest: layout-independent, still corruption-sensitive.

The bug this guards against: hashing each file in its own block order made a
striped source and its tiled rewrite hash differently, so every rewrite of a
warp intermediate was reported CORRUPT and silently left as LZW.

    python scripts/test_recompress_digest.py
"""
import sys
import tempfile
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
from recompress_zstd import raster_digest  # noqa: E402

W, H = 300, 700
DATA = (np.arange(W * H, dtype="float32").reshape(H, W) % 97) - 99999.0


def write(path, data, **profile_extra):
    profile = dict(driver="GTiff", width=W, height=H, count=1, dtype="float32",
                   crs="EPSG:3857", transform=from_origin(0, 0, 100, 100),
                   nodata=-99999.0, **profile_extra)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def main():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        striped = d / "striped.tif"        # what gdalwarp emits
        tiled = d / "tiled.tif"            # what the ZSTD rewrite emits
        altered = d / "altered.tif"        # one pixel off

        write(striped, DATA, compress="LZW")
        write(tiled, DATA, compress="ZSTD", tiled=True, blockxsize=256, blockysize=256)
        bad = DATA.copy()
        bad[H - 1, W - 1] += 1.0
        write(altered, bad, compress="ZSTD", tiled=True, blockxsize=256, blockysize=256)

        with rasterio.open(striped) as s, rasterio.open(tiled) as t:
            assert s.block_shapes != t.block_shapes, "test is vacuous: same layout"

        assert raster_digest(striped) == raster_digest(tiled), \
            "same pixels, different block layout must hash the same"
        assert raster_digest(striped) != raster_digest(altered), \
            "a changed pixel must still be caught"
    print("ok")


if __name__ == "__main__":
    main()
