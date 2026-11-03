"""End-to-end test on synthetic raw files that mimic each source's real schema
(including its quirks), run through the real pipeline in --offline mode.

    pytest -q
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import run_pipeline
from pipeline.utils import PROJECT_ROOT

RUN_DATE = "2026-11-01"
METROS = {  # zip -> (Metro, State, City, County)
    "90012": ("Los Angeles-Long Beach-Anaheim, CA", "CA", "Los Angeles", "Los Angeles County"),
    "90210": ("Los Angeles-Long Beach-Anaheim, CA", "CA", "Beverly Hills", "Los Angeles County"),
    "92701": ("Los Angeles-Long Beach-Anaheim, CA", "CA", "Santa Ana", "Orange County"),
    "95112": ("San Jose-Sunnyvale-Santa Clara, CA", "CA", "San Jose", "Santa Clara County"),
    "95125": ("San Jose-Sunnyvale-Santa Clara, CA", "CA", "San Jose", "Santa Clara County"),
    "77840": ("College Station-Bryan, TX", "TX", "College Station", "Brazos County"),
    "77845": ("College Station-Bryan, TX", "TX", "College Station", "Brazos County"),
    "77802": ("College Station-Bryan, TX", "TX", "Bryan", "Brazos County"),
    # decoys: must NOT end up in the universe
    "75001": ("Dallas-Fort Worth-Arlington, TX", "TX", "Addison", "Dallas County"),
    "77001": ("San Jose-Sunnyvale-Santa Clara, CA", "TX", "Fake", "Fake County"),  # wrong state
}
MONTHS = pd.date_range("2018-01-31", "2026-09-30", freq="ME")   # Zillow uses month-end


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    rng = np.random.default_rng(0)
    raw = tmp_path / "raw"
    cfg = yaml.safe_load((PROJECT_ROOT / "config.yaml").read_text())
    cfg["paths"] = {"raw": str(raw), "processed": str(tmp_path / "processed")}
    cfg["census"]["acs_years"] = [2024]
    cfg["census"]["gazetteer_years"] = [2024]
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(cfg))

    # ---------------- Zillow (wide, month-end date columns) ---------------- #
    zdir = raw / "zillow" / RUN_DATE
    zdir.mkdir(parents=True)
    for name in cfg["zillow"]["datasets"]:
        rows = []
        for i, (z, (metro, st, city, county)) in enumerate(METROS.items()):
            base = 1.2e6 if st == "CA" else 3.0e5
            path = base * np.exp(np.cumsum(rng.normal(0.004, 0.01, len(MONTHS))))
            row = {"RegionID": str(1000 + i), "SizeRank": i, "RegionName": int(z),
                   "RegionType": "zip", "StateName": st, "State": st, "City": city,
                   "Metro": metro, "CountyName": county}
            row.update({d.strftime("%Y-%m-%d"): v for d, v in zip(MONTHS, path)})
            if name == "zhvi_4bed" and z == "90012":
                row.update({d.strftime("%Y-%m-%d"): np.nan for d in MONTHS[:24]})
            rows.append(row)
        pd.DataFrame(rows).to_csv(zdir / f"{name}.csv", index=False)

    # ---------------- Realtor (yyyymm, disclaimer footer) ------------------ #
    rdir = raw / "realtor" / RUN_DATE
    rdir.mkdir(parents=True)
    rmonths = pd.date_range("2016-07-01", "2026-09-01", freq="MS")
    inv = []
    for z, (metro, st, city, _) in METROS.items():
        ppsf = 900 if st == "CA" else 180
        for d in rmonths:
            inv.append({"month_date_yyyymm": d.strftime("%Y%m"), "postal_code": z,
                        "zip_name": f"{city.lower()}, {st.lower()}",
                        "median_listing_price": ppsf * 1900,
                        "active_listing_count": rng.integers(2, 200),
                        "median_days_on_market": rng.integers(10, 90),
                        "median_listing_price_per_square_foot": ppsf * rng.uniform(.9, 1.1),
                        "median_square_feet": 1900, "price_reduced_count": 3,
                        "pending_ratio": 0.4, "quality_flag": 0})
    footer = {"month_date_yyyymm": "Note: data revised regularly"}
    pd.DataFrame(inv + [footer]).to_csv(rdir / "inventory_zip.csv", index=False)
    hot = pd.DataFrame(inv)[["month_date_yyyymm", "postal_code", "zip_name"]].assign(
        hotness_score=50.0, supply_score=40.0, demand_score=60.0)
    hot.to_csv(rdir / "hotness_zip.csv", index=False)
    met = [{"month_date_yyyymm": d.strftime("%Y%m"), "cbsa_code": c, "cbsa_title": t,
            "median_listing_price": 8e5, "median_listing_price_per_square_foot": 500}
           for d in rmonths for c, t in [("31080", "LA"), ("41940", "SJ"),
                                         ("17780", "CS"), ("19100", "DFW")]]
    pd.DataFrame(met).to_csv(rdir / "inventory_metro.csv", index=False)

    # ---------------- FRED JSON ('.' = missing) ---------------------------- #
    fdir = raw / "fred" / RUN_DATE
    fdir.mkdir(parents=True)
    series = (list(cfg["fred"]["national"])
              + [s for m in cfg["fred"]["by_metro"].values() for s in m]
              + [s for c in cfg["fred"]["by_county"].values() for s in c])
    for sid in series:
        if sid == "SFXRSA":                     # simulate a series that failed to fetch
            continue
        if sid.startswith("MORTGAGE"):
            freq, dates = "W", pd.date_range("2018-01-04", "2026-10-29", freq="W-THU")
        elif sid.startswith("ATNHPI"):
            freq, dates = "Q", pd.date_range("2000-01-01", "2026-04-01", freq="QS")
        elif sid.startswith("MHI"):
            freq, dates = "A", pd.date_range("2000-01-01", "2024-01-01", freq="YS")
        else:
            freq, dates = "M", pd.date_range("2016-07-01", "2026-09-01", freq="MS")
        fixed = {"ATNHPIUS31084Q": 111.0, "ATNHPIUS11244Q": 222.0, "ATNHPIUS41940Q": 333.0}
        obs = [{"date": d.strftime("%Y-%m-%d"),
                "value": f"{fixed.get(sid, rng.uniform(1, 10)):.2f}"} for d in dates]
        obs[3]["value"] = "."
        (fdir / f"{sid}_series.json").write_text(json.dumps(
            {"seriess": [{"id": sid, "title": sid, "frequency_short": freq,
                          "units_short": "x", "seasonal_adjustment_short": "NSA"}]}))
        (fdir / f"{sid}_observations.json").write_text(json.dumps({"observations": obs}))

    # ---------------- Census ACS JSON + gazetteer zip ---------------------- #
    cdir = raw / "census" / RUN_DATE
    cdir.mkdir(parents=True)
    vars_ = list(cfg["census"]["acs_variables"])
    acs = [["NAME", *vars_, "zip code tabulation area"]]
    for z in METROS:
        if z == "77845":
            continue                             # simulate a ZIP with no ZCTA
        vals = [str(rng.integers(1000, 60000)) for _ in vars_]
        if z == "90210":
            vals[1] = "-666666666"               # Census sentinel
        acs.append([f"ZCTA5 {z}", *vals, z])
    (cdir / "acs5_2024_zcta.json").write_text(json.dumps(acs))
    gaz = "GEOID\tALAND\tAWATER\tALAND_SQMI\tAWATER_SQMI\tINTPTLAT\tINTPTLONG                 \n"
    gaz += "".join(f"{z}\t1\t0\t{rng.uniform(1, 50):.3f}\t0\t34.0\t-118.0\n" for z in METROS)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("2024_Gaz_zcta_national.txt", gaz)
    (cdir / "gazetteer_zcta_2024.zip").write_bytes(buf.getvalue())
    return tmp_path


def _read(project, name):
    return pd.read_csv(project / "processed" / f"{name}.csv", dtype={"zip": str})


def test_end_to_end_offline(project):
    rc = run_pipeline.main(["--offline", "--config", str(project / "config.yaml")])
    manifest = json.loads((project / "processed" / "run_manifest.json").read_text())

    # universe: decoys excluded, San Jose is the CA one only
    dim = _read(project, "zip_dim")
    assert set(dim.zip) == set(METROS) - {"75001", "77001"}
    assert (dim[dim.metro_key == "san_jose"].state == "CA").all()

    # zillow: month-start dates, NaNs dropped, all 5 datasets
    z = _read(project, "zillow_zip_monthly")
    assert z.dataset.nunique() == 5
    assert pd.to_datetime(z.date).dt.day.eq(1).all()
    assert z.value.notna().all()

    # realtor: footer dropped, option-B column present and = ppsf * 2500
    r = _read(project, "realtor_zip_monthly")
    assert r.date.str.match(r"\d{4}-\d{2}-01").all()
    assert np.allclose(r.est_list_price_2500sqft, r.median_listing_price_per_square_foot * 2500)

    # fred: '.' dropped, the missing series reported but run still completes
    f = _read(project, "fred_observations")
    assert f.value.notna().all()
    assert "SFXRSA" in manifest["sources"]["fred"]["series_failed"]

    # census: sentinel -> NaN, missing ZCTA kept as NaN row, density computed
    c = _read(project, "census_zcta")
    assert pd.isna(c.loc[c.zip == "90210", "median_household_income"]).all()
    assert pd.isna(c.loc[c.zip == "77845", "population"]).all()
    assert (c.pop_density_per_sqmi.dropna() > 0).all()

    # panel: one row per zip-month, macro + metro series attached
    p = _read(project, "zip_month_panel")
    assert not p.duplicated(["zip", "month"]).any()
    for col in ["zhvi_sfr", "rdc_est_list_price_2500sqft", "pop_density_per_sqmi",
                "MORTGAGE30US", "fhfa_hpi", "county_median_hh_income"]:
        assert col in p.columns, col
    sep = p[p.month == "2026-09-01"]
    assert sep.MORTGAGE30US.notna().all()
    assert sep.fhfa_hpi.notna().all()           # quarterly forward-filled to monthly
    # LA metro has no single FHFA index: each county gets its own division's index
    hpi = sep.set_index("zip").fhfa_hpi
    assert hpi["90012"] == 111.0 and hpi["92701"] == 222.0 and hpi["95112"] == 333.0
    assert not any(c.endswith("__cty") for c in p.columns)

    assert rc == 0, manifest["warnings"]


# --------------------------------------------------------------------------- #
# Census request failure modes + secret scrubbing
# --------------------------------------------------------------------------- #
class _Resp:
    def __init__(self, text):
        self.text = text
    def json(self):
        return json.loads(self.text)


def test_acs_invalid_key_is_reported(monkeypatch):
    from pipeline import census
    html = "<html><body><h1>Invalid Key</h1>A valid key must be included</body></html>"
    monkeypatch.setattr(census, "http_get", lambda *a, **k: _Resp(html))
    rows, err = census._acs_request(2024, {"B01003_001E": "population"}, "badkey")
    assert rows is None and err == "invalid or inactive API key"


def test_acs_falls_back_to_no_key(monkeypatch, tmp_path):
    from pipeline import census
    from pipeline.utils import RunContext
    good = json.dumps([["NAME", "B01003_001E", census.ZCTA_COL], ["ZCTA5 90012", "100", "90012"]])
    monkeypatch.setattr(census, "http_get",
                        lambda url, params, **k: _Resp("Invalid Key" if "key" in params else good))
    monkeypatch.setenv("CENSUS_API_KEY", "notactivated")
    cfg = {"paths": {"raw": tmp_path, "processed": tmp_path},
           "census": {"acs_years": [2024], "acs_variables": {"B01003_001E": "population"}}}
    ctx = RunContext(cfg)
    df, year = census._fetch_acs(ctx)
    assert year == 2024 and len(df) == 1
    assert any("not activated" in w for w in ctx.manifest["warnings"])


def test_api_keys_scrubbed_from_manifest(tmp_path):
    from pipeline.utils import RunContext
    ctx = RunContext({"paths": {"raw": tmp_path, "processed": tmp_path}})
    ctx.warn("400 for url: https://x/fred/series?series_id=A&api_key=SECRET123&file_type=json")
    ctx.record("census", error="https://api.census.gov/data?get=x&key=SECRET456")
    text = ctx.write_manifest().read_text()
    assert "SECRET" not in text and "api_key=***" in text
