"""Zillow Research: ZHVI / ZORI at ZIP level.

Also defines the ZIP universe (zip_dim) that every other source is filtered to.
"""
from __future__ import annotations

import logging
import re

import pandas as pd

from .utils import RunContext, fetch_file, write_table

log = logging.getLogger("pipeline.zillow")

ID_COLS = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName",
           "State", "City", "Metro", "CountyName"]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _read(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"RegionName": str, "RegionID": str})
    missing = {"RegionName", "State", "Metro"} - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: Zillow schema changed, missing {missing}")
    df["RegionName"] = df["RegionName"].str.zfill(5)
    return df


def _filter_to_metros(df: pd.DataFrame, metros: dict) -> pd.DataFrame:
    """Tag rows with metro_key using exact Metro + State match."""
    parts = []
    for key, m in metros.items():
        mask = (df["Metro"] == m["zillow_metro"]) & (df["State"] == m["state"])
        if m.get("counties"):                       # optional: narrow metro to counties
            mask &= df["CountyName"].isin(m["counties"])
        sub = df.loc[mask].copy()
        sub["metro_key"] = key
        parts.append(sub)
    return pd.concat(parts, ignore_index=True)


def build_zip_universe(ctx: RunContext) -> pd.DataFrame:
    cfg, metros = ctx.cfg["zillow"], ctx.cfg["metros"]
    name = cfg["universe_dataset"]
    path = fetch_file(ctx, "zillow", f'{cfg["base_url"]}/{cfg["datasets"][name]}', f"{name}.csv")
    df = _filter_to_metros(_read(path), metros)

    dim = (df.rename(columns={"RegionName": "zip", "City": "city",
                              "CountyName": "county", "State": "state",
                              "SizeRank": "zillow_size_rank"})
             [["zip", "metro_key", "city", "county", "state", "zillow_size_rank"]])

    # manual overrides from config
    for key, m in metros.items():
        drop = set(map(str, m.get("exclude_zips", [])))
        dim = dim[~((dim.metro_key == key) & dim.zip.isin(drop))]
        extra = [z for z in map(lambda z: str(z).zfill(5), m.get("extra_zips", []))
                 if z not in set(dim.zip)]
        if extra:
            dim = pd.concat([dim, pd.DataFrame({"zip": extra, "metro_key": key,
                                                "state": m["state"]})])

    for key, m in metros.items():
        n = (dim.metro_key == key).sum()
        if n == 0:
            ctx.warn(f"zillow: 0 ZIPs matched metro '{m['zillow_metro']}' ({m['state']}) "
                     f"- check the exact Metro string in the Zillow CSV")
        else:
            log.info("universe: %-16s %4d ZIPs", key, n)

    dim = dim.sort_values(["metro_key", "zip"]).reset_index(drop=True)
    ctx.record("zillow", universe_zips={k: int((dim.metro_key == k).sum()) for k in metros})
    write_table(ctx, dim, "zip_dim")
    return dim


def ingest(ctx: RunContext, zip_dim: pd.DataFrame) -> pd.DataFrame:
    cfg = ctx.cfg["zillow"]
    start = pd.Timestamp(ctx.cfg["start_date"])
    keep = zip_dim[["zip", "metro_key"]]
    frames = []

    for name, rel in cfg["datasets"].items():
        try:
            path = fetch_file(ctx, "zillow", f'{cfg["base_url"]}/{rel}', f"{name}.csv")
        except Exception as e:                       # one bad file shouldn't kill the run
            ctx.warn(f"zillow: {name} failed to download ({e})")
            continue
        df = _read(path).rename(columns={"RegionName": "zip"})
        df = df.merge(keep, on="zip", how="inner")
        date_cols = [c for c in df.columns if DATE_RE.match(str(c))]
        long = df.melt(id_vars=["zip", "metro_key"], value_vars=date_cols,
                       var_name="date", value_name="value")
        # Zillow stamps month-END; normalise to month-START so every source joins
        long["date"] = pd.to_datetime(long["date"]).dt.to_period("M").dt.to_timestamp()
        long = long[(long.date >= start) & long.value.notna()]
        long["dataset"] = name
        frames.append(long)
        log.info("zillow: %-15s %7d rows, %d ZIPs", name, len(long), long.zip.nunique())

    out = (pd.concat(frames, ignore_index=True)
             [["zip", "metro_key", "date", "dataset", "value"]]
             .sort_values(["dataset", "zip", "date"]).reset_index(drop=True))
    ctx.record("zillow", rows=len(out), datasets=sorted(out.dataset.unique()),
               last_month=str(out.date.max().date()))
    write_table(ctx, out, "zillow_zip_monthly")
    return out
