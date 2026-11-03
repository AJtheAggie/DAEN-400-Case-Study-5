#!/usr/bin/env python3
"""Batch ingestion for the housing case study.

    python run_pipeline.py                      # full run, all sources
    python run_pipeline.py --sources fred       # refresh one source
    python run_pipeline.py --force-download     # ignore today's cache
    python run_pipeline.py --offline            # rebuild from cached raw files

Outputs land in data/processed/ (see README for the table list).
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from pipeline import build, census, fred, realtor, validate, zillow
from pipeline.utils import RunContext, load_config, load_env, setup_logging

log = logging.getLogger("pipeline")
ALL_SOURCES = ["zillow", "realtor", "fred", "census"]


def _load_existing(ctx: RunContext, name: str, parse=("date",)) -> pd.DataFrame | None:
    p = ctx.processed_dir / f"{name}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, dtype={"zip": str})
    for c in parse:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    return df


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES)
    ap.add_argument("--force-download", action="store_true")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--config", default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    setup_logging(args.verbose)
    load_env()
    cfg = load_config(args.config) if args.config else load_config()
    ctx = RunContext(cfg, force_download=args.force_download, offline=args.offline)
    ctx.manifest["sources_requested"] = args.sources
    tables: dict[str, pd.DataFrame] = {}

    # 1. ZIP universe (always rebuilt when Zillow runs; else reuse last one)
    if "zillow" in args.sources:
        zip_dim = zillow.build_zip_universe(ctx)
    else:
        zip_dim = _load_existing(ctx, "zip_dim", parse=())
        if zip_dim is None:
            log.error("No zip_dim.csv yet - run with zillow included first.")
            return 2

    # 2. Sources. Each is independent; one failing doesn't block the others.
    def attempt(name, fn):
        try:
            return fn()
        except Exception as e:
            ctx.warn(f"{name}: FAILED - {e}")
            ctx.record(name, status="failed", error=str(e))
            return None

    z = attempt("zillow", lambda: zillow.ingest(ctx, zip_dim)) if "zillow" in args.sources \
        else _load_existing(ctx, "zillow_zip_monthly")
    r = attempt("realtor", lambda: realtor.ingest(ctx, zip_dim)) if "realtor" in args.sources \
        else ({"realtor_zip_monthly": t} if (t := _load_existing(ctx, "realtor_zip_monthly")) is not None else None)
    if "fred" in args.sources:
        fr = attempt("fred", lambda: fred.ingest(ctx))
        f_obs, f_meta = fr if fr else (None, None)
    else:
        f_obs = _load_existing(ctx, "fred_observations")
        f_meta = _load_existing(ctx, "fred_series_meta", parse=())
    c = attempt("census", lambda: census.ingest(ctx, zip_dim)) if "census" in args.sources \
        else _load_existing(ctx, "census_zcta", parse=())

    # 3. Panel + checks
    if z is None:
        ctx.warn("panel: skipped (no Zillow data)")
    else:
        tables["zip_month_panel"] = build.build_panel(ctx, zip_dim, z, r, f_obs, f_meta, c)
    tables.update({"zillow_zip_monthly": z, "fred_observations": f_obs,
                   **({"realtor_zip_monthly": r["realtor_zip_monthly"]} if r else {})})
    passed = validate.run_checks(ctx, zip_dim, {k: v for k, v in tables.items() if v is not None})

    out = ctx.write_manifest()
    failed = [s for s, i in ctx.manifest["sources"].items() if i.get("status") == "failed"]
    log.info("done. manifest: %s | failed sources: %s | warnings: %d",
             out.name, failed or "none", len(ctx.manifest["warnings"]))
    return 0 if passed and not failed else 1


if __name__ == "__main__":
    sys.exit(main())
