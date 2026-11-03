"""Census: ACS 5-year ZCTA demographics + Gazetteer land area -> population density.

Note: ZCTAs (Census) approximate USPS ZIPs; a few PO-box / campus ZIPs
(e.g. 77843 at TAMU) have no ZCTA and will show NaN density.
"""
from __future__ import annotations

import json
import logging
import os
import zipfile

import numpy as np
import pandas as pd
import requests

from .utils import RunContext, fetch_file, http_get, save_json, write_table

log = logging.getLogger("pipeline.census")
ZCTA_COL = "zip code tabulation area"


def _fetch_acs(ctx: RunContext) -> tuple[pd.DataFrame, int]:
    cfg = ctx.cfg["census"]
    variables = cfg["acs_variables"]
    key = os.environ.get("CENSUS_API_KEY", "").strip()
    cache_dir = ctx.raw_dir / "census"

    errors = []
    for year in cfg["acs_years"]:
        fname = f"acs5_{year}_zcta.json"
        cached = sorted(cache_dir.glob(f"*/{fname}"))
        if ctx.offline or (cached and not ctx.force_download):
            if not cached:
                errors.append(f"{year}: no cached file (offline)")
                continue
            rows = json.loads(cached[-1].read_text())
        else:
            rows = None
            # try with the key first; if the key is the problem, retry without it
            for use_key in ([True, False] if key else [False]):
                rows, err = _acs_request(year, variables, key if use_key else None)
                if rows is not None:
                    if use_key is False and key:
                        ctx.warn("census: request with CENSUS_API_KEY failed but worked "
                                 "without it - the key is probably not activated yet "
                                 "(check your email for the activation link)")
                    break
                errors.append(f"{year} ({'with' if use_key else 'without'} key): {err}")
            if rows is None:
                continue
            save_json(ctx, "census", fname, rows)
        return pd.DataFrame(rows[1:], columns=rows[0]), year
    raise RuntimeError("census: no ACS year responded. Details: " + " | ".join(errors))


def _acs_request(year: int, variables: dict, key: str | None):
    """Return (rows, None) on success or (None, reason) on failure.
    The Census API answers bad keys / bad queries with an HTML page (sometimes
    with HTTP 200), so a non-JSON body is treated as an error and summarised."""
    params = {"get": ",".join(["NAME", *variables]), "for": f"{ZCTA_COL}:*"}
    if key:
        params["key"] = key
    try:
        r = http_get(f"https://api.census.gov/data/{year}/acs/acs5",
                     params=params, timeout=180, retries=2)
    except requests.RequestException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        return None, f"HTTP {status}" if status else type(e).__name__
    try:
        rows = r.json()
        if not isinstance(rows, list) or len(rows) < 2:
            return None, "empty response"
        return rows, None
    except ValueError:
        body = " ".join(r.text.split())[:160]
        if "invalid key" in body.lower():
            return None, "invalid or inactive API key"
        return None, f"non-JSON response: {body!r}"


def _fetch_gazetteer(ctx: RunContext) -> pd.DataFrame:
    cfg = ctx.cfg["census"]
    last_err = None
    for year in cfg["gazetteer_years"]:
        try:
            path = fetch_file(ctx, "census", cfg["gazetteer_url"].format(year=year),
                              f"gazetteer_zcta_{year}.zip")
        except Exception as e:
            last_err = e
            continue
        with zipfile.ZipFile(path) as zf:
            member = next(n for n in zf.namelist() if n.endswith(".txt"))
            with zf.open(member) as f:
                gaz = pd.read_csv(f, sep="\t", dtype={"GEOID": str})
        gaz.columns = gaz.columns.str.strip()        # last header has trailing spaces
        gaz["zip"] = gaz["GEOID"].str.zfill(5)
        return gaz[["zip", "ALAND_SQMI", "INTPTLAT", "INTPTLONG"]].rename(columns={
            "ALAND_SQMI": "land_area_sqmi", "INTPTLAT": "lat", "INTPTLONG": "lon"})
    raise RuntimeError(f"census: gazetteer download failed ({last_err})")


def ingest(ctx: RunContext, zip_dim: pd.DataFrame) -> pd.DataFrame:
    variables = ctx.cfg["census"]["acs_variables"]
    acs, year = _fetch_acs(ctx)
    acs = acs.rename(columns={ZCTA_COL: "zip", **variables})
    acs["zip"] = acs["zip"].str.zfill(5)
    for c in variables.values():
        acs[c] = pd.to_numeric(acs[c], errors="coerce")
        acs.loc[acs[c] < 0, c] = np.nan      # Census sentinels like -666666666

    gaz = _fetch_gazetteer(ctx)
    out = (zip_dim[["zip", "metro_key"]]
           .merge(acs[["zip", *variables.values()]], on="zip", how="left")
           .merge(gaz, on="zip", how="left"))
    out["pop_density_per_sqmi"] = out["population"] / out["land_area_sqmi"].replace(0, np.nan)
    if {"owner_occupied_units", "occupied_units"} <= set(out.columns):
        out["owner_occupied_share"] = out["owner_occupied_units"] / out["occupied_units"]
    out["acs_year"] = year

    no_match = out["population"].isna().sum()
    if no_match:
        ctx.warn(f"census: {no_match} ZIPs have no ACS ZCTA match (PO-box/campus ZIPs)")
    write_table(ctx, out, "census_zcta")
    ctx.record("census", acs_year=year, rows=len(out), zips_without_acs=int(no_match))
    log.info("census: ACS %d, %d ZIPs, median density by metro:\n%s", year, len(out),
             out.groupby("metro_key")["pop_density_per_sqmi"].median().round(0).to_string())
    return out
