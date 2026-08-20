#!/usr/bin/env python3
"""Recompress WorldPop GeoTIFFs from LZW to ZSTD in place, losslessly.

Measured on this dataset (see docstring table below), ZSTD with NO predictor beats
every other lossless option GDAL 3.6 offers here:

    agesex raster (LZW+PREDICTOR=2, the WorldPop upstream default)
        as-is                99,333,325
        ZSTD                 59,541,321   -40.1%   <-- winner
        LERC_ZSTD (exact)    61,401,398   -38.2%
        DEFLATE              64,626,734   -34.9%
        ZSTD + PREDICTOR=2   69,334,631   -30.2%
        ZSTD + PREDICTOR=3   79,537,126   -19.9%

    combined mosaic window (LZW, no predictor)
        LZW               1,213,034,697
        ZSTD                905,592,773   -25.3%   <-- winner
        DEFLATE             917,464,972   -24.4%
        LERC_ZSTD (exact)   924,815,386   -23.8%
        LZMA                989,148,142   -18.5%   (and 4x slower)
        ZSTD + PREDICTOR=3 1,070,773,784  -11.7%

Predictors LOSE on this data: it is mostly long runs of NoData (-99999) and 0.0,
which plain LZ eats trivially. Differencing shreds those runs. PREDICTOR=2 is for
integer data anyway -- applying it to Float32 (as WorldPop upstream does) is the
single biggest waste in the pipeline.

Every rewrite is verified bit-exact (SHA-256 over all decoded blocks, plus CRS /
geotransform / nodata / dtype) BEFORE the original is replaced. A file that fails
verification is left untouched and reported.

Usage:
    # dry run -- measures savings, writes nothing
    python recompress_zstd.py /opt/airflow/data/agesex --dry-run

    # do it
    python recompress_zstd.py /opt/airflow/data/agesex
    python recompress_zstd.py /opt/airflow/data --no-recurse
"""
import argparse
import hashlib
import os
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path

import rasterio

# ponytail: no config file, no class hierarchy. Two profiles is the whole domain.
GTIFF_OPTS = [
    "-of", "GTiff",
    "-co", "TILED=YES", "-co", "BLOCKXSIZE=512", "-co", "BLOCKYSIZE=512",
    "-co", "COMPRESS=ZSTD", "-co", "ZSTD_LEVEL=9",
    "-co", "BIGTIFF=YES", "-co", "NUM_THREADS=ALL_CPUS",
]
COG_OPTS = [
    "-of", "COG",
    "-co", "COMPRESS=ZSTD", "-co", "LEVEL=9",
    # Re-encode the source's OWN overviews rather than recomputing them. Recomputing
    # would apply this driver's default resampling, which is not necessarily what
    # built the file -- and overviews are exactly what titiler serves at low zoom,
    # so a silent resampling change would alter the rendered map.
    "-co", "OVERVIEWS=FORCE_USE_EXISTING",
    "-co", "BIGTIFF=YES", "-co", "NUM_THREADS=ALL_CPUS",
]


def human(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def profile(path):
    """(needs_work, gdal_translate opts). COG layout must stay COG."""
    with rasterio.open(path) as src:
        tags = src.tags(ns="IMAGE_STRUCTURE")
        compression = tags.get("COMPRESSION", "NONE")
        is_cog = tags.get("LAYOUT") == "COG" or bool(src.overviews(1))
    # ZSTD without a predictor is already the target state.
    done = compression == "ZSTD" and "PREDICTOR" not in tags
    return (not done), (COG_OPTS if is_cog else GTIFF_OPTS)


def _hash_dataset(src, h):
    for bidx in range(1, src.count + 1):
        for _, window in src.block_windows(bidx):
            h.update(src.read(bidx, window=window, masked=False).tobytes())


def raster_digest(path):
    """SHA-256 over every decoded block, plus the georeferencing that must survive.

    Overviews are hashed too, not just full resolution: they are what titiler
    serves at low zoom, so "lossless" has to mean the rendered map is unchanged
    at every zoom level, not just at 1:1.
    """
    h = hashlib.sha256()
    with rasterio.open(path) as src:
        meta = (src.width, src.height, src.count, tuple(src.dtypes),
                tuple(src.transform), str(src.crs), tuple(src.nodatavals))
        h.update(repr(meta).encode())
        n_overviews = len(src.overviews(1))
        _hash_dataset(src, h)
    for level in range(n_overviews):
        with rasterio.open(path, overview_level=level) as ov:
            _hash_dataset(ov, h)
    return h.hexdigest()


def recompress(path, dry_run=False):
    """-> (status, before, after). Original is replaced only after verification."""
    before = path.stat().st_size
    needs_work, opts = profile(path)
    if not needs_work:
        return "skip", before, before

    tmp = path.with_suffix(path.suffix + ".zstd.tmp")
    try:
        subprocess.run(["gdal_translate", "-q", *opts, str(path), str(tmp)],
                       check=True, capture_output=True, text=True)
        after = tmp.stat().st_size

        if raster_digest(path) != raster_digest(tmp):
            tmp.unlink()
            return "CORRUPT", before, before

        if dry_run:
            tmp.unlink()
            return "would-shrink", before, after

        # Same directory, so this is an atomic rename -- no window where the
        # original is gone but the replacement is not yet in place.
        os.replace(tmp, path)
        return "ok", before, after
    except subprocess.CalledProcessError as e:
        tmp.unlink(missing_ok=True)
        print(f"    gdal_translate failed: {e.stderr.strip()[:200]}", file=sys.stderr)
        return "FAILED", before, before
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path)
    ap.add_argument("--dry-run", action="store_true", help="measure only, write nothing")
    ap.add_argument("--no-recurse", action="store_true", help="top level only")
    ap.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                    help="skip filenames matching this glob (repeatable)")
    args = ap.parse_args()

    pattern = "*.tif" if args.no_recurse else "**/*.tif"
    files = sorted(p for p in args.root.glob(pattern)
                   if p.is_file() and not p.name.endswith(".zstd.tmp")
                   and not any(fnmatch(p.name, g) for g in args.exclude))
    if not files:
        sys.exit(f"no .tif found under {args.root}")

    total_before = total_after = 0
    counts = {}
    for i, path in enumerate(files, 1):
        status, before, after = recompress(path, args.dry_run)
        counts[status] = counts.get(status, 0) + 1
        total_before += before
        total_after += after
        if status not in ("skip",):
            pct = (1 - after / before) * 100 if before else 0
            print(f"[{i}/{len(files)}] {status:12s} {pct:5.1f}%  {path.name}", flush=True)
        if status in ("CORRUPT", "FAILED"):
            print(f"    !! left untouched: {path}", file=sys.stderr, flush=True)

    saved = total_before - total_after
    print("\n" + "-" * 60)
    print(f"files    : {len(files)}  " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print(f"before   : {human(total_before):>10}")
    print(f"after    : {human(total_after):>10}")
    print(f"saved    : {human(saved):>10}  ({saved / total_before * 100 if total_before else 0:.1f}%)")
    if args.dry_run:
        print("(dry run -- nothing written)")


if __name__ == "__main__":
    main()
