"""Shared helpers: config/env loading, logging, cached downloads, run manifest."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml

log = logging.getLogger("pipeline")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------- #
# Config / env
# --------------------------------------------------------------------------- #
def load_env(path: Path = PROJECT_ROOT / ".env") -> None:
    """Minimal .env loader (KEY=VALUE lines). Real env vars take precedence."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def load_config(path: Path = PROJECT_ROOT / "config.yaml") -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    for k in ("raw", "processed"):
        p = Path(cfg["paths"][k])
        cfg["paths"][k] = p if p.is_absolute() else PROJECT_ROOT / p
    return cfg


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


# --------------------------------------------------------------------------- #
# Run context: carries config, run date, and a manifest of what happened
# --------------------------------------------------------------------------- #
@dataclass
class RunContext:
    cfg: dict
    run_date: str = field(default_factory=lambda: dt.date.today().isoformat())
    force_download: bool = False
    offline: bool = False                     # use latest cached raw files only
    manifest: dict = field(default_factory=lambda: {"sources": {}, "warnings": []})

    @property
    def raw_dir(self) -> Path:
        return self.cfg["paths"]["raw"]

    @property
    def processed_dir(self) -> Path:
        p = self.cfg["paths"]["processed"]
        p.mkdir(parents=True, exist_ok=True)
        return p

    def warn(self, msg: str) -> None:
        msg = scrub(msg)
        log.warning(msg)
        self.manifest["warnings"].append(msg)

    def record(self, source: str, **info) -> None:
        self.manifest["sources"].setdefault(source, {}).update(info)

    def write_manifest(self) -> Path:
        self.manifest["run_date"] = self.run_date
        self.manifest["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
        out = self.processed_dir / "run_manifest.json"
        out.write_text(scrub(json.dumps(self.manifest, indent=2, default=str)))
        return out


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "tamu-daen-housing-case-study/1.0"})


def http_get(url: str, *, params: dict | None = None, stream: bool = False,
             retries: int = 4, timeout: int = 120) -> requests.Response:
    """GET with exponential backoff on network errors / 429 / 5xx."""
    for attempt in range(1, retries + 1):
        try:
            r = _SESSION.get(url, params=params, stream=stream, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise                                   # client error: don't retry
            if attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("GET %s failed (%s); retry %d/%d in %ds",
                        _redact(url), scrub(str(e)), attempt, retries - 1, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


_SECRET_RE = re.compile(r"((?:api_)?key=)[^&\s\"']+", re.IGNORECASE)


def scrub(text: str) -> str:
    """Mask API keys that appear in URLs inside error messages."""
    return _SECRET_RE.sub(r"\1***", text)


def _redact(url: str) -> str:
    return url.split("?")[0]


def fetch_file(ctx: RunContext, source: str, url: str, filename: str) -> Path:
    """Download url to data/raw/<source>/<run_date>/<filename>, with caching.

    - If today's copy exists and --force-download isn't set, reuse it.
    - In --offline mode, return the most recent cached copy from any date.
    """
    src_dir = ctx.raw_dir / source
    target = src_dir / ctx.run_date / filename

    if ctx.offline:
        cached = sorted(src_dir.glob(f"*/{filename}"))
        if not cached:
            raise FileNotFoundError(f"--offline: no cached copy of {source}/{filename}")
        log.info("[%s] offline: using %s", source, cached[-1].relative_to(ctx.raw_dir))
        return cached[-1]

    if target.exists() and not ctx.force_download:
        log.info("[%s] cached: %s", source, filename)
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    log.info("[%s] downloading %s", source, filename)
    with http_get(url, stream=True) as r, open(tmp, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    tmp.replace(target)                      # atomic: never leave half-files
    log.info("[%s] saved %s (%.1f MB)", source, filename, target.stat().st_size / 1e6)
    return target


def save_json(ctx: RunContext, source: str, filename: str, payload) -> Path:
    target = ctx.raw_dir / source / ctx.run_date / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload))
    return target


def write_table(ctx: RunContext, df, name: str) -> Path:
    out = ctx.processed_dir / f"{name}.csv"
    df.to_csv(out, index=False)
    log.info("wrote %s (%d rows)", out.relative_to(PROJECT_ROOT) if out.is_relative_to(PROJECT_ROOT) else out, len(df))
    return out
