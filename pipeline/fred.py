"""FRED API: national macro series + per-metro house price / income series."""
from __future__ import annotations

import json
import logging
import os

import pandas as pd
import requests

from .utils import RunContext, http_get, save_json, write_table

log = logging.getLogger("pipeline.fred")


def _api_key() -> str:
    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FRED_API_KEY not set. Get a free key at "
            "https://fred.stlouisfed.org/docs/api/api_key.html and put it in .env")
    return key


def _get(ctx: RunContext, endpoint: str, series_id: str, **params) -> dict:
    """Call FRED (or read today's cached JSON in offline mode)."""
    fname = f"{series_id}_{endpoint.split('/')[-1]}.json"
    cache_dir = ctx.raw_dir / "fred"
    if ctx.offline:
        cached = sorted(cache_dir.glob(f"*/{fname}"))
        if not cached:
            raise FileNotFoundError(f"--offline: no cached {fname}")
        return json.loads(cached[-1].read_text())
    today = cache_dir / ctx.run_date / fname
    if today.exists() and not ctx.force_download:
        return json.loads(today.read_text())
    r = http_get(f'{ctx.cfg["fred"]["base_url"]}/{endpoint}',
                 params={"series_id": series_id, "api_key": _api_key(),
                         "file_type": "json", **params}, timeout=60)
    payload = r.json()
    save_json(ctx, "fred", fname, payload)
    return payload


def _series_plan(cfg: dict) -> list[tuple[str, str, str]]:
    """(series_id, scope, description) for every configured series."""
    plan = [(sid, "national", desc) for sid, desc in cfg["fred"]["national"].items()]
    for metro, series in cfg["fred"]["by_metro"].items():
        plan += [(sid, metro, desc) for sid, desc in series.items()]
    for county, series in (cfg["fred"].get("by_county") or {}).items():
        plan += [(sid, f"county:{county}", desc) for sid, desc in series.items()]
    return plan


def ingest(ctx: RunContext) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not ctx.offline:
        _api_key()                                   # fail fast with a clear message
    obs_frames, meta_rows, failed = [], [], []

    for sid, scope, desc in _series_plan(ctx.cfg):
        try:
            meta = _get(ctx, "series", sid)["seriess"][0]
            data = _get(ctx, "series/observations", sid,
                        observation_start=ctx.cfg["start_date"])
        except (requests.HTTPError, KeyError, IndexError, FileNotFoundError) as e:
            # FRED returns 400 for unknown series IDs
            failed.append(sid)
            ctx.warn(f"fred: {sid} ({scope}) could not be fetched: {e}")
            continue

        df = pd.DataFrame(data.get("observations", []))
        if df.empty:
            ctx.warn(f"fred: {sid} returned no observations")
            continue
        df = df[["date", "value"]].assign(
            series_id=sid, scope=scope,
            date=lambda d: pd.to_datetime(d["date"]),
            value=lambda d: pd.to_numeric(d["value"], errors="coerce"))  # '.' -> NaN
        obs_frames.append(df.dropna(subset=["value"]))
        meta_rows.append({
            "series_id": sid, "scope": scope, "description": desc,
            "title": meta.get("title"), "frequency": meta.get("frequency_short"),
            "units": meta.get("units_short"),
            "seasonal_adjustment": meta.get("seasonal_adjustment_short"),
            "observation_end": meta.get("observation_end"),
            "last_updated": meta.get("last_updated"),
        })
        log.info("fred: %-26s %-16s %5d obs  (%s)", sid, scope, len(df),
                 meta.get("frequency_short"))

    if not obs_frames:
        raise RuntimeError("fred: no series fetched; check API key / network")
    obs = (pd.concat(obs_frames, ignore_index=True)
             [["series_id", "scope", "date", "value"]]
             .sort_values(["series_id", "date"]).reset_index(drop=True))
    meta = pd.DataFrame(meta_rows)
    write_table(ctx, obs, "fred_observations")
    write_table(ctx, meta, "fred_series_meta")
    ctx.record("fred", series_ok=len(meta_rows), series_failed=failed, rows=len(obs))
    return obs, meta
