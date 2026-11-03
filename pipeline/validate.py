"""Post-run data quality checks. Failures are recorded in the manifest;
`hard` failures make the run exit non-zero so a bad batch is noticed."""
from __future__ import annotations

import logging

import pandas as pd

from .utils import RunContext

log = logging.getLogger("pipeline.validate")


def run_checks(ctx: RunContext, zip_dim: pd.DataFrame, tables: dict[str, pd.DataFrame]) -> bool:
    results, ok = [], True

    def check(name, passed, detail="", hard=False):
        nonlocal ok
        results.append({"check": name, "passed": bool(passed), "detail": detail})
        if not passed:
            (log.error if hard else log.warning)("CHECK FAILED: %s %s", name, detail)
            ok = ok and not hard

    # every metro has ZIPs, and each metro's ZIPs are in the right state
    for key, m in ctx.cfg["metros"].items():
        sub = zip_dim[zip_dim.metro_key == key]
        check(f"{key}: has ZIPs", len(sub) > 0, f"n={len(sub)}", hard=True)
        bad = sub[sub.state.notna() & (sub.state != m["state"])]
        check(f"{key}: all ZIPs in {m['state']}", bad.empty, f"{len(bad)} off-state", hard=True)
    dup = zip_dim.zip.duplicated().sum()
    check("zip_dim: zip unique", dup == 0, f"{dup} ZIPs assigned to two metros", hard=True)

    # primary-key uniqueness + freshness per table
    keys = {"zillow_zip_monthly": ["zip", "dataset", "date"],
            "realtor_zip_monthly": ["zip", "date"],
            "fred_observations": ["series_id", "date"],
            "zip_month_panel": ["zip", "month"]}
    for name, pk in keys.items():
        df = tables.get(name)
        if df is None or df.empty:
            check(f"{name}: present", False, "missing/empty")
            continue
        dupes = df.duplicated(pk).sum()
        check(f"{name}: unique on {pk}", dupes == 0, f"{dupes} dupes", hard=True)
        dcol = "month" if "month" in df.columns else "date"
        age_days = (pd.Timestamp.today() - df[dcol].max()).days
        check(f"{name}: fresh (<120 days)", age_days < 120, f"latest={df[dcol].max().date()}")

    # value sanity
    z = tables.get("zillow_zip_monthly")
    if z is not None and not z.empty:
        check("zillow: values positive", (z.value > 0).all())
    r = tables.get("realtor_zip_monthly")
    if r is not None and "median_listing_price_per_square_foot" in r:
        ppsf = r["median_listing_price_per_square_foot"].dropna()
        check("realtor: $/sqft in [20, 5000]", ppsf.between(20, 5000).mean() > 0.99,
              f"{(~ppsf.between(20, 5000)).sum()} outliers")

    # coverage per metro in the panel (the thin-market issue for College Station)
    p = tables.get("zip_month_panel")
    if p is not None and "rdc_active_listing_count" in p:
        recent = p[p.month >= p.month.max() - pd.DateOffset(months=12)]
        low = (recent.groupby("metro_key")["rdc_active_listing_count"]
               .apply(lambda s: (s < 10).mean()).round(2).to_dict())
        results.append({"check": "share of ZIP-months with <10 active listings (last 12m)",
                        "passed": True, "detail": low})

    ctx.manifest["checks"] = results
    n_fail = sum(not r["passed"] for r in results)
    log.info("validation: %d checks, %d failed", len(results), n_fail)
    return ok
