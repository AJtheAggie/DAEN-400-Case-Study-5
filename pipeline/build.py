"""Join the per-source tables into one modeling-ready ZIP x month panel."""
from __future__ import annotations

import logging

import pandas as pd

from .utils import RunContext, write_table

log = logging.getLogger("pipeline.build")

REALTOR_PANEL_COLS = ["median_listing_price", "median_listing_price_per_square_foot",
                      "median_square_feet", "active_listing_count",
                      "median_days_on_market", "price_reduced_count", "pending_ratio"]


def _generic(sid: str) -> str:
    """Geography-specific FRED IDs -> generic names so areas stack in one column."""
    if sid.startswith("ATNHPIUS"):           return "fhfa_hpi"
    if sid.startswith("MHI"):                return "county_median_hh_income"
    if sid.startswith("MEDLISPRIPERSQUFEE"): return "metro_list_ppsf"
    if sid.endswith("XRSA"):                 return "case_shiller_sa"
    return sid


def _widen(df: pd.DataFrame, index) -> pd.DataFrame:
    w = df.pivot_table(index=index, columns="series_id", values="value")
    w.columns.name = None
    return w.reset_index()


def _ffill_monthly(w: pd.DataFrame, key: str, end: pd.Timestamp) -> pd.DataFrame:
    """Forward-fill quarterly/annual values onto a monthly grid per area, out to
    `end`, so each month carries the latest value published so far."""
    if w.empty:
        return w
    grids = []
    for k, g in w.groupby(key):
        idx = pd.date_range(g.month.min(), max(g.month.max(), end), freq="MS")
        grids.append(g.drop(columns=key).set_index("month").reindex(idx).ffill()
                      .assign(**{key: k}).rename_axis("month").reset_index())
    return pd.concat(grids, ignore_index=True)


def _fred_monthly(fred: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (national, metro, county) monthly wide frames. Weekly -> monthly mean;
    quarterly/annual -> forward-filled within the period they describe."""
    f = fred.copy()
    f["month"] = f["date"].dt.to_period("M").dt.to_timestamp()
    f = f.groupby(["series_id", "scope", "month"], as_index=False)["value"].mean()
    end = f["month"].max()

    nat = _widen(f[f.scope == "national"], "month")

    is_county = f.scope.str.startswith("county:")
    metro = f[(f.scope != "national") & ~is_county].rename(columns={"scope": "metro_key"})
    metro["series_id"] = metro["series_id"].map(_generic)
    metro_w = _ffill_monthly(_widen(metro, ["metro_key", "month"]), "metro_key", end)

    county = f[is_county].copy()
    county["county"] = county.pop("scope").str.removeprefix("county:")
    county["series_id"] = county["series_id"].map(_generic)
    county_w = _ffill_monthly(_widen(county, ["county", "month"]), "county", end) \
        if not county.empty else pd.DataFrame()
    return nat, metro_w, county_w


def build_panel(ctx: RunContext, zip_dim, zillow, realtor, fred, fred_meta, census) -> pd.DataFrame:
    # Zillow: one column per dataset
    z = zillow.pivot_table(index=["zip", "metro_key", "date"], columns="dataset",
                           values="value").reset_index()
    z.columns.name = None
    panel = z.rename(columns={"date": "month"})

    if realtor is not None:
        inv = realtor.get("realtor_zip_monthly")
        if inv is not None:
            extra = [c for c in inv.columns if c.startswith("est_list_price_")]
            cols = [c for c in REALTOR_PANEL_COLS if c in inv.columns] + extra
            r = inv[["zip", "metro_key", "date"] + cols].rename(
                columns={"date": "month", **{c: f"rdc_{c}" for c in cols}})
            panel = panel.merge(r, on=["zip", "metro_key", "month"], how="outer")

    if census is not None:
        static = census.drop(columns=["metro_key"], errors="ignore")
        panel = panel.merge(static, on="zip", how="left")

    panel = panel.merge(zip_dim[["zip", "city", "county"]], on="zip", how="left")

    if fred is not None:
        nat, metro, county = _fred_monthly(fred)
        panel = panel.merge(nat, on="month", how="left")
        if not metro.empty:
            panel = panel.merge(metro, on=["metro_key", "month"], how="left")
        if not county.empty:
            # county-level value wins over the metro-level one for the same column
            panel = panel.merge(county, on=["county", "month"], how="left",
                                suffixes=("", "__cty"))
            for col in [c for c in county.columns if c not in ("county", "month")]:
                if f"{col}__cty" in panel.columns:
                    panel[col] = panel.pop(f"{col}__cty").combine_first(panel[col])
    lead = ["zip", "metro_key", "city", "county", "month"]
    panel = panel[lead + [c for c in panel.columns if c not in lead]]
    panel = panel.sort_values(["metro_key", "zip", "month"]).reset_index(drop=True)

    write_table(ctx, panel, "zip_month_panel")
    ctx.record("panel", rows=len(panel), cols=len(panel.columns),
               zips=int(panel.zip.nunique()))
    return panel
