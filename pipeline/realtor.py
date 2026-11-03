"""Realtor.com Research: monthly inventory core metrics + hotness (ZIP & metro)."""
from __future__ import annotations

import logging

import pandas as pd

from .utils import RunContext, fetch_file, write_table

log = logging.getLogger("pipeline.realtor")

# columns we keep if present (Realtor adds/renames occasionally; missing ones are skipped)
INVENTORY_COLS = [
    "median_listing_price", "average_listing_price", "median_listing_price_per_square_foot",
    "median_square_feet", "active_listing_count", "total_listing_count",
    "new_listing_count", "pending_listing_count", "pending_ratio",
    "price_reduced_count", "price_increased_count", "median_days_on_market",
    "quality_flag",
]
HOTNESS_COLS = ["hotness_score", "hotness_rank", "supply_score", "demand_score",
                "median_days_on_market", "ldp_unique_viewers_per_property_vs_us",
                "median_listing_price"]


def _read(path, key_col: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={key_col: str, "month_date_yyyymm": str},
                     low_memory=False)
    if key_col not in df.columns or "month_date_yyyymm" not in df.columns:
        raise ValueError(f"{path.name}: Realtor schema changed (no {key_col}/month_date_yyyymm)")
    # files end with a free-text disclaimer row; keep only real yyyymm rows
    df = df[df["month_date_yyyymm"].str.fullmatch(r"\d{6}", na=False)].copy()
    df["date"] = pd.to_datetime(df["month_date_yyyymm"], format="%Y%m")
    return df


def _numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns and c != "quality_flag":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _zip_table(ctx, path, zip_dim, cols, name) -> pd.DataFrame:
    df = _read(path, "postal_code")
    df["zip"] = df["postal_code"].str.zfill(5)
    df = df.merge(zip_dim[["zip", "metro_key"]], on="zip", how="inner")
    present = [c for c in cols if c in df.columns]
    absent = sorted(set(cols) - set(present))
    if absent:
        ctx.warn(f"realtor/{name}: columns not in file, skipped: {absent}")
    df = _numeric(df, present)
    out = (df[["zip", "metro_key", "date"] + present]
           .sort_values(["zip", "date"]).reset_index(drop=True))
    return out


def ingest(ctx: RunContext, zip_dim: pd.DataFrame) -> dict[str, pd.DataFrame]:
    cfg = ctx.cfg["realtor"]
    start = pd.Timestamp(ctx.cfg["start_date"])
    ds = cfg["datasets"]
    url = lambda k: f'{cfg["base_url"]}/{ds[k]}'
    results: dict[str, pd.DataFrame] = {}

    # ---- ZIP inventory --------------------------------------------------- #
    p = fetch_file(ctx, "realtor", url("inventory_zip"), "inventory_zip.csv")
    inv = _zip_table(ctx, p, zip_dim, INVENTORY_COLS, "inventory_zip")
    inv = inv[inv.date >= start]
    sqft = cfg["target_sqft"]
    if "median_listing_price_per_square_foot" in inv.columns:
        # Option B: price of a target-size home ~= ZIP median $/sqft * target sqft.
        # This is a LIST-price estimate; listing mix shifts month to month.
        inv[f"est_list_price_{sqft}sqft"] = inv["median_listing_price_per_square_foot"] * sqft
    results["realtor_zip_monthly"] = inv

    # ---- ZIP hotness ----------------------------------------------------- #
    try:
        p = fetch_file(ctx, "realtor", url("hotness_zip"), "hotness_zip.csv")
        hot = _zip_table(ctx, p, zip_dim, HOTNESS_COLS, "hotness_zip")
        results["realtor_hotness_zip_monthly"] = hot[hot.date >= start]
    except Exception as e:
        ctx.warn(f"realtor: hotness_zip failed ({e})")

    # ---- Metro inventory (benchmark for ZIP aggregates) ------------------- #
    try:
        p = fetch_file(ctx, "realtor", url("inventory_metro"), "inventory_metro.csv")
        met = _read(p, "cbsa_code")
        cbsa_to_key = {str(m["cbsa"]): k for k, m in ctx.cfg["metros"].items()}
        met["metro_key"] = met["cbsa_code"].str.strip().map(cbsa_to_key)
        met = met[met.metro_key.notna() & (met.date >= start)]
        present = [c for c in INVENTORY_COLS if c in met.columns]
        met = _numeric(met, present)
        results["realtor_metro_monthly"] = (met[["metro_key", "date"] + present]
                                            .sort_values(["metro_key", "date"])
                                            .reset_index(drop=True))
    except Exception as e:
        ctx.warn(f"realtor: inventory_metro failed ({e})")

    for name, df in results.items():
        write_table(ctx, df, name)
        log.info("realtor: %-28s %6d rows", name, len(df))
    ctx.record("realtor", **{f"{k}_rows": len(v) for k, v in results.items()},
               last_month=str(inv.date.max().date()) if len(inv) else None,
               zips_with_data=int(inv.zip.nunique()))
    # ZIPs in the universe with no Realtor coverage are worth knowing about
    missing = sorted(set(zip_dim.zip) - set(inv.zip))
    if missing:
        ctx.record("realtor", zips_without_data=len(missing))
    return results
