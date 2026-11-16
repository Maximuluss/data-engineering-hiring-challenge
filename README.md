# Data Engineering Take-Home Challenge

## Overview

You're building a data pipeline for a marketplace. People post tasks on the marketplace, such as "Roof Cleaning in Sydney" or "Tax Return in London", and each task belongs to a city.

The product team wants a **weather risk score** for tasks with upcoming scheduled work. Given a task's location and the time the work is scheduled, the score says how much the weather forecast puts the work at risk.

Two teams will use the output:

- **Product** will warn posters about risky tasks through a CRM message. Product also wants to see how a task's risk changed between pipeline runs.
- **Operations** wants a daily weather summary for each location, with each day cut at midnight in the location's own timezone. The summary shows how many tasks are scheduled that day and how many of them are high risk.

Product will tune the risk thresholds after launch. Both teams must always report the same number of high-risk tasks for the same location and day.

In production this pipeline would cover about 1M tasks across 40,000 locations. You're building it against a small sample, but be ready to talk about what changes at that size.

## The stack

- **Dagster** handles orchestration and asset definitions.
- **dbt Core** handles transformation, connected to Dagster through `dagster-dbt`.
- **DuckDB** is the local warehouse.
- **Python** handles ingestion and any business logic you choose to keep outside SQL.
- **Open-Meteo** provides weather forecasts. It's free and needs no API key.

The repo has Docker Compose and a Dagster project scaffold.

```bash
docker compose up   # or: make up
```

Once the startup sequence finishes, open **http://localhost:3000** for the Dagster UI.

### Querying DuckDB

The startup script writes the DuckDB file to `./data/challenge.duckdb`, and you can query it from your machine once the pipeline has run.

```bash
duckdb ./data/challenge.duckdb
SHOW ALL TABLES;
SELECT * FROM <schema>.<table> LIMIT 10;
```

```python
import duckdb
con = duckdb.connect("./data/challenge.duckdb")
con.sql("SELECT * FROM ...").show()
```

## Data provided

The startup script loads two raw tables into DuckDB. Both arrive as text, exactly as the source system wrote them.

### `raw_tasks`

| Column | Description |
| --- | --- |
| `task_id` | Unique task identifier |
| `title` | Task title |
| `category` | Task category |
| `location_id` | Foreign key to `raw_locations` |
| `posted_at` | When the task was posted |
| `state` | `open`, `assigned`, `completed` or `cancelled` |
| `budget` | Task budget |
| `assigned_at` | When the task was assigned. Empty if it isn't assigned. |
| `scheduled_at` | When the work is scheduled to happen. Present on assigned tasks. |

The categories are Gardening, Roof Cleaning, Lawn Mowing, Pool Cleaning, Exterior Painting, Tax Return, Online Tutoring, Graphic Design, Bookkeeping, Photography, Event Setup, Moving & Removals and Cleaning.

### `raw_locations`

| Column | Description |
| --- | --- |
| `location_id` | Unique location identifier |
| `city` | City name |
| `country` | Country code |
| `lat` | Latitude |
| `lng` | Longitude |
| `timezone` | IANA timezone name |

The locations are in Australia, the United States and the United Kingdom.

### Known data quality issues

A real ticket would flag these, so we're flagging them too. There are other issues we haven't listed.

- **Duplicate task IDs.** Some tasks appear more than once with different `posted_at` values because of a replication glitch. Keep the most recent record.
- **Orphaned locations.** Some tasks reference a `location_id` that doesn't exist in `raw_locations`. Your pipeline should handle them gracefully.

## Weather API

Use the Open-Meteo forecast endpoint, `GET https://api.open-meteo.com/v1/forecast`. The documentation is at https://open-meteo.com/en/docs. Read it, because the parameters you choose affect whether your output is correct.

## What to build

Work through these in order. Incomplete P1 work scores better than complete P3 work.

### P1: core pipeline (required)

1. A Dagster asset that fetches forecasts from Open-Meteo and stores them in DuckDB. Keep every forecast you fetch.
2. dbt staging models for tasks and locations.
3. `fct_task_weather_risk`, with one row per task with upcoming scheduled work. It needs a `weather_risk_score` and a `weather_risk_label` such as `low`, `medium` or `high`. You define both, and your spec documents them.
4. `fct_location_daily_weather`, with one row per location per local day. It needs the number of scheduled tasks, the number of high-risk tasks and a summary of that day's weather.

### P2: quality (expected)

5. At least 2 dbt schema tests on your staging models.
6. At least 1 dbt unit test for your risk scoring logic.
7. Error handling and retries in the ingestion asset.

### P3: stretch

8. A `DECISIONS.md` covering the trade-offs you made and what you'd do differently in production.
9. A job or schedule that runs the whole pipeline in the right order.

## Your spec

**Write `SPEC.md` before you write any code.** We read it first.

We expect you to write it with your AI tool, and we encourage that. We're assessing whether you can think a problem through clearly and defend the decisions you made.

Your spec should cover at least these four things:

- **The asset graph.** Show which assets exist and what depends on what. A text diagram is fine.
- **The data model.** Give the grain of each table and say where each calculation lives.
- **The risk model.** Say which categories are weather-sensitive and why, which weather variables you use, what range the score takes and how you handle ambiguous cases.
- **Data quality and assumptions.** List the issues you found, how you handled each one, and anything you assumed about the data, the API or the business.

Half a page of clear reasoning beats two pages of vague generalities. Be ready to defend every decision in a follow-up conversation.

## Submission

- Commit your work to the repo, using branches and pull requests. Your git history is part of the assessment.
- Submit a pull request from a branch named `solution/<your-name>`.
- Put `SPEC.md` at the repo root, along with `DECISIONS.md` if you write one.
- Don't commit the DuckDB file, any `.env` file, credentials or large generated files.

## What we're looking for

- The pipeline runs end to end.
- Your risk model is documented and defensible.
- You handled the data quality issues correctly.
- Your tests test the right things.
- Your git history shows incremental, intentional progress.
- You can explain every part of what you built.

We aren't looking for Kubernetes or cloud deployment knowledge, Snowflake-specific syntax, a perfect risk model, full test coverage, or a pipeline written entirely by AI with no sign of your own judgement.
