# Solution Spec

> Write this document before writing any code. Use your AI tool to help draft it;
> we expect that. What we're assessing is whether you can think through a problem
> clearly and defend the decisions you made.

## Asset graph

How data flows from the sources through the DuckDB layers to the two marts. Arrows point from an asset to the assets that depend on it.

```text
SOURCE   raw_locations.csv  generate_tasks.py       Open-Meteo API            category_weather_sensitivity.csv
               │                 │                        │                                 │
               ▼                 ▼                        ▼                                 ▼
RAW      raw_locations       raw_tasks            raw_weather_forecast*       category_weather_sensitivity
               │                 │                        │                                 │
               ▼                 ▼                        ▼                                 │
STG      stg_locations ────▶ stg_tasks            stg_weather_forecast                      │
           │   │                 │                        │                                 │
           │   │                 │                        ▼                                 │
INT        │   │                 │           int_hourly_weather_forecast                    │
           │   │                 │                │               │                         │
           │   │                 │                │               ▼                         │
           │   │                 │                │   int_daily_weather_forecast            │
           │   │                 │                │               │                         │
           │   │                 ▼                ▼               │                         │
MART       │   └────────────▶ fct_task_weather_risk ◀─────────────┼─────────────────────────┘
           │                            │                         │
           │                            ▼                         │
           └────────────────▶ fct_location_daily_weather ◀────────┘
```

`*` `raw_weather_forecast` reads `stg_locations` at run time to get each location's coordinates and timezone. This isn't declared as a Dagster dependency yet, so the graph in the Dagster UI doesn't show it.

| Asset | Built by | Depends on |
|---|---|---|
| `raw_locations`, `category_weather_sensitivity` | dbt seed | CSV files in `dbt_project/seeds` |
| `raw_tasks` | `generate_tasks.py` (startup script) | — |
| `raw_weather_forecast` | Dagster asset (Python) | Open-Meteo API, `stg_locations` |
| `stg_locations` | dbt view | `raw_locations` |
| `stg_tasks` | dbt view | `raw_tasks`, `stg_locations` (orphan check) |
| `stg_weather_forecast` | dbt view | `raw_weather_forecast` |
| `int_hourly_weather_forecast` | dbt view | `stg_weather_forecast` |
| `int_daily_weather_forecast` | dbt view | `int_hourly_weather_forecast` |
| `fct_task_weather_risk` | dbt table | `stg_tasks`, `stg_locations`, `int_hourly_weather_forecast`, `category_weather_sensitivity` |
| `fct_location_daily_weather` | dbt table | `int_daily_weather_forecast`, `stg_locations`, `fct_task_weather_risk` |

## Data model

| Layer | DuckDB schema | Materialised as | Purpose |
|---|---|---|---|
| RAW | `raw` | tables | Data exactly as loaded. Tasks and locations are text; forecasts are the full API response as JSON |
| STG | `stg` | views | One model per raw table: cast types, rename, dedupe, fix known data issues |
| INT | `int` | views | Weather reshaped to the grains the marts join on |
| MART | `main` | tables | The outputs Product and Operations use |

| Model | Grain | Key | What is calculated here |
|---|---|---|---|
| `raw_weather_forecast` | One row per location per fetch (append-only) | — | Nothing. Every response is kept with `fetched_at` and the request URL |
| `stg_tasks` | One row per `task_id` | `task_id` | Text to UTC timestamps, category trim and casing, budget cast. Dedupe keeps the latest `posted_at`. Orphaned locations become `'Missing Location'` |
| `stg_locations` | One row per `location_id` | `location_id` | Renames and casts |
| `stg_weather_forecast` | One row per location per forecast hour (UTC) | `location_id`, `forecasted_at` | Unnests the hourly arrays. Keeps the latest fetch for each hour |
| `int_hourly_weather_forecast` | One row per location per forecast hour | `forecast_hour_id` | Local time and local date from the location's IANA timezone |
| `int_daily_weather_forecast` | One row per location per local day | `forecast_day_id` | Aggregates hourly to daily, matching Open-Meteo's own daily variables (see below) |
| `fct_task_weather_risk` | One row per task with upcoming scheduled work | `task_id` | Factor scores, `weather_risk_score`, `weather_risk_label` |
| `fct_location_daily_weather` | One row per location per local day | `location_day_id` | Scheduled and high-risk task counts, taken from `fct_task_weather_risk` |

Decisions behind this layout:

- **Hourly, not daily, from the API.** The asset fetches 16 days of hourly data with `timeformat=unixtime`. Every hour is then an unambiguous UTC instant, so DST changes can't shift values onto the wrong day. Open-Meteo ignores DST inside a response for ISO local times. Daily figures are rebuilt in dbt.
- **Daily aggregation matches the API.** Days are cut at local midnight using the IANA timezone of each row, not the response's fixed `utc_offset_seconds`. A daily value is null if any hour of that variable is null, which is what the API does. Checked against the API's daily output for four cities: weather code, precipitation sum, max precipitation probability and max gusts match exactly. Max/min temperature is a few tenths off, because the API derives it from sub-hourly model data.
- **One source of truth for high risk.** `fct_location_daily_weather` counts tasks from `fct_task_weather_risk` instead of rescoring them. The singular test `assert_daily_task_counts_match_task_risk` fails if the two marts ever disagree.

## Risk model

**Which tasks.** Tasks with upcoming scheduled work: `state = 'Assigned'` and `scheduled_at` in the future. Cancelled tasks that still have a `scheduled_at` are excluded, because the work won't happen.

**Category sensitivity.** Stored in the `category_weather_sensitivity` seed, so Product can change it without a code change.

| Weight | Categories | Why |
|---|---|---|
| 1.0 | Gardening, Roof Cleaning, Lawn Mowing, Pool Cleaning, Exterior Painting | Fully outdoor. Rain or wind stops the work or makes it unsafe |
| 0.5 | Event Setup, Moving & Removals, Photography | Partly outdoor, or can often move indoors |
| 0.0 | Tax Return, Online Tutoring, Graphic Design, Bookkeeping, Cleaning | Indoor or remote |

**Which forecast hour.** The hour that contains the scheduled start: the single forecast row with `scheduled_at < forecasted_at <= scheduled_at + 1h`. Precipitation, precipitation probability and gusts describe the hour *before* their timestamp, so work starting at 09:00 uses the 10:00 row. A range join is used instead of an exact match because Adelaide and Darwin have forecast rows at :30 UTC.

**Weather factors.** Each one is scaled to 0–100.

| Factor | Variable | Scale |
|---|---|---|
| Rain chance | `precipitation_probability` | Used as is (%) |
| Rain amount | `precipitation` | 0 mm/h → 0, 5 mm/h or more → 100, linear |
| Wind | `wind_gusts_10m` | 20 km/h or less → 0, 70 km/h or more → 100, linear |
| Conditions | `weather_code` | Thunderstorm (95–99) → 100, snow (71–77, 85, 86) → 80, otherwise 0 |

**Score and label.** `weather_risk_score = category weight × the worst factor`, rounded to one decimal, range 0–100. The worst factor is used instead of a weighted sum so that one severe factor isn't diluted, and so each score is easy to explain ("high because gusts reach 60 km/h"). The factor scores are kept as columns.

| Label | Rule |
|---|---|
| `high` | score ≥ 60 |
| `medium` | 30 ≤ score < 60 |
| `low` | score < 30, including every indoor task (score 0) |
| `unknown` | no forecast row for the task |

The thresholds are dbt vars (`weather_risk_medium_threshold`, `weather_risk_high_threshold`) in `dbt_project.yml`, so Product can tune them. Both marts read the label from the same column, so changing a threshold changes both together.

**Ambiguous cases.**

| Case | Handling |
|---|---|
| Orphaned location (e.g. T058) | Kept in `fct_task_weather_risk` with a null score and label `unknown`. It is never counted as high risk, and it doesn't appear in the daily mart because it has no location or local day |
| Scheduled beyond the forecast window | `unknown`, same as above. All sample tasks fall inside the 16-day window |
| One factor missing (e.g. UK rain chance more than ~12 days out) | The score uses the factors that are available (`greatest()` skips nulls) |
| Category not in the seed | Weight defaults to 0 (`low`). A new category is assumed not weather-sensitive until it is added to the seed |
| Forecast changes between runs | The marts use the latest fetch. Every fetch is kept in `raw_weather_forecast`, so run-to-run comparisons can be built from raw |

## Data quality and assumptions

1. Issue with the latitude/longitude coordinates for Parth
2. Leeds and Glasgow have country code `UK`, while the other UK cities use `GB`, the ISO code. It doesn’t break the API call, but you’ll probably want to normalise it to `GB` in `stg_locations` and add an `accepted_values` test for country codes.
3. The category column has trailling space for some of the records generate by the entrypoint script