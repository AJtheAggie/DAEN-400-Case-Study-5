# DAEN-400-Case-Study-5
DAEN 400 Case Study #4 

In this case study we take a look at housing prices, crime rates, demographics, etc.. on 3 particular cities; San Jose, Los Angeles, and College Station. We will be looking at data trends for these features in each city to decide whether a bank should be established(if the economy and general sector is doing well). 

Some features to explore are:

*Housing Features*

1) House size
2) Historical Pricing Index
3)Number of bedrooms and bathrooms
4)Family types

*Location*
1) Crime rates
2) Schools nearby
3) Environment risks

*Demographics*
1)Population growth
2)Migration patterns(CENSUS)


# Analysis script (FOR GRADER)
The critical piece of analysis is conducted in the data_analysis_corrected file (version control duplicated the 2 and we wanted to keep the source file here for reference).

# Housing Case Study – Batch Ingestion Pipeline

Pulls Zillow, Realtor.com, FRED and Census data for **Los Angeles, San Jose (CA) and
College Station-Bryan** at ZIP level and builds one modeling-ready ZIP × month panel.

## Setup (once)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                 # then paste your keys into .env
```

| Key | Needed? | Where |
|---|---|---|
| `FRED_API_KEY` | **Required** | https://fred.stlouisfed.org/docs/api/api_key.html (free account → "Request API Key") |
| `CENSUS_API_KEY` | Optional, avoids rate limits | https://api.census.gov/data/key_signup.html (emailed, activate via link) |

Zillow and Realtor.com publish bulk CSVs, so they need no key.

## Run

```bash
python run_pipeline.py                    # full batch (first run downloads ~400 MB)
python run_pipeline.py --sources fred     # refresh just one source
python run_pipeline.py --force-download   # re-download even if today's copy exists
python run_pipeline.py --offline          # rebuild tables from cached raw files only
python -m pytest -q                       # end-to-end test on synthetic data, no network
```

Exit code is `0` on success, `1` if a source failed or a hard data-quality check failed.
Everything that happened — row counts, latest month per source, FRED series that didn't
resolve, failed checks — is in `data/processed/run_manifest.json`. **Read it after each run.**

Refresh cadence: Zillow updates around the 16th of each month, Realtor.com early in the month.

## How it works

```
config.yaml ─► zillow.build_zip_universe ─► zip_dim (the ZIP set for each metro)
                                   │
          ┌──────────┬─────────────┼─────────────┐
       zillow     realtor        fred         census        (each filtered to zip_dim)
          └──────────┴──────┬──────┴─────────────┘
                     build.build_panel ─► validate.run_checks ─► run_manifest.json
```

* **One ZIP universe.** ZIPs come from Zillow's `Metro` + `State` columns, and every other
  source is filtered to that same set. The state check is how San Jose is guaranteed to be
  the CA metro; validation fails hard if any San Jose ZIP is not in CA.
* **Raw is immutable.** Downloads go to `data/raw/<source>/<run-date>/`, untouched, so any
  processed table can be rebuilt with `--offline` and runs from different dates can be compared.
* **Sources fail independently.** A dead Zillow link doesn't stop FRED. The failure is
  logged in the manifest, and the run exits `1`.
* **Dates are normalised to month-start.** Zillow stamps month-end and Realtor uses
  `yyyymm`; both are converted to the first of the month so joins line up.
* **Adding or removing a metro** (e.g. dropping San Jose) is a config-only change: delete
  its block under `metros:` and `fred.by_metro:`.

## Output tables (`data/processed/`)

| Table | Grain | What |
|---|---|---|
| `zip_dim` | zip | metro, city, county, state |
| `zillow_zip_monthly` | zip × month × dataset | ZHVI (all homes, SFR, 3-bed, 4-bed) and ZORI rent, long format |
| `realtor_zip_monthly` | zip × month | list price, **$/sqft**, median sqft, inventory, DOM, price cuts, **`est_list_price_2500sqft`** |
| `realtor_hotness_zip_monthly` | zip × month | supply, demand, hotness scores |
| `realtor_metro_monthly` | metro × month | metro benchmark for the ZIP aggregates |
| `fred_observations` / `fred_series_meta` | series × date | raw FRED values, plus title, units and frequency |
| `census_zcta` | zip | population, income, home value, land area, **pop density**, owner share |
| `zip_month_panel` | zip × month | **everything joined; this is the table to model from** |

In the panel, FRED series are aligned to months as follows. Weekly mortgage rates are
averaged to monthly. Quarterly FHFA and annual county income are forward-filled. Metro
series get generic names (`fhfa_hpi`, `county_median_hh_income`, `metro_list_ppsf`,
`case_shiller_sa`) so all three metros stack in one column.


## Hotness Metric 
Summary of supply and demand calculated by Realtor.com

## Known caveats (put these in your report)

1. **`est_list_price_2500sqft` is a list-price estimate**: ZIP median $/sqft × 2500.
   $/sqft usually *falls* as homes get larger, so for ZIPs where the median home is much
   smaller than 2,500 sqft this overstates the price. Compare against `median_square_feet`.
   Also, list prices are not sale prices.
2. **Thin markets.** College Station ZIPs can have fewer than 10 active listings in a month,
   which makes monthly medians noisy. The manifest reports the share of such ZIP-months
   per metro. Weight or filter by `rdc_active_listing_count` before computing volatility.
3. **ZIP ≠ ZCTA.** Census density uses ZCTAs. Campus and PO-box ZIPs (e.g. 77843) have no
   ZCTA and get NaN density.
4. **Metro boundaries.** The LA metro includes Orange County, and the San Jose metro
   includes San Benito County. Set `counties:` in `config.yaml` to narrow either one.
5. **Census `median_home_value` is a 5-year average of owner self-reports.** Use it as a
   cross-check, not as a price target.
6. **San Jose has no Case-Shiller index.** `SFXRSA` (San Francisco) is included as a proxy;
   drop it if you don't want it.
