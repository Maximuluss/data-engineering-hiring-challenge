"""
Seed script — generates raw.raw_tasks in DuckDB on container startup.

This script is infrastructure. Candidates should treat raw_tasks as a
given data source and not modify this file.
"""
import duckdb
import sys
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DUCKDB_PATH = sys.argv[1]

# Fixed random seed for reproducibility
random.seed(42)

# posted_at base: all tasks within the last 30 days, in UTC
NOW = datetime.now(timezone.utc)


def days_ago(n, hour=9, minute=0):
    """Return a UTC timestamp N days ago at a given time."""
    return (NOW - timedelta(days=n)).replace(
        hour=hour, minute=minute, second=0, microsecond=0
    )


def days_ahead(n, hour=9, minute=0):
    """Return a UTC timestamp N days from now at a given time."""
    return (NOW + timedelta(days=n)).replace(
        hour=hour, minute=minute, second=0, microsecond=0
    )


def pick_timestamps(state, posted_at):
    """Return (assigned_at, scheduled_at) based on task state.

    Business rules:
    - open tasks: not yet assigned, no scheduled date
    - assigned tasks: assigned and scheduled for a future date
    - completed tasks: were assigned; scheduled date is in the past
    - cancelled tasks: ~50% were assigned before cancellation
    """
    SCHEDULED_HOURS = [8, 9, 10, 11, 13, 14, 15]

    if state == "open":
        return None, None

    assigned_at = posted_at + timedelta(hours=random.randint(2, 48))

    if state == "assigned":
        days_ahead = random.randint(1, 14)
        scheduled_at = (NOW + timedelta(days=days_ahead)).replace(
            hour=random.choice(SCHEDULED_HOURS), minute=0, second=0, microsecond=0
        )
        return assigned_at, scheduled_at

    if state == "completed":
        days_after = random.randint(1, 7)
        scheduled_at = assigned_at + timedelta(days=days_after)
        if scheduled_at > NOW:
            scheduled_at = NOW - timedelta(days=random.randint(1, 3))
        scheduled_at = scheduled_at.replace(
            hour=random.choice(SCHEDULED_HOURS), minute=0, second=0, microsecond=0
        )
        return assigned_at, scheduled_at

    if state == "cancelled":
        if random.random() < 0.5:
            days_ahead = random.randint(1, 7)
            scheduled_at = (NOW + timedelta(days=days_ahead)).replace(
                hour=random.choice(SCHEDULED_HOURS), minute=0, second=0, microsecond=0
            )
            return assigned_at, scheduled_at
        return None, None

    return None, None


# ---------------------------------------------------------------------------
# Location pools (matching locations_raw.csv exactly)
# AU ~60%, US ~25%, UK ~15%
# ---------------------------------------------------------------------------
AU_LOCATIONS = [
    "LOC_AU_SYD", "LOC_AU_MEL", "LOC_AU_BNE",
    "LOC_AU_PER", "LOC_AU_ADL", "LOC_AU_CBR", "LOC_AU_DAR",
]
US_LOCATIONS = [
    "LOC_US_NYC", "LOC_US_LAX", "LOC_US_CHI",
    "LOC_US_HOU", "LOC_US_PHX", "LOC_US_SEA", "LOC_US_DEN",
]
UK_LOCATIONS = [
    "LOC_UK_LON", "LOC_UK_MCR", "LOC_UK_BIR",
    "LOC_UK_LEE", "LOC_UK_GLA", "LOC_UK_EDI",
]

# Weighted pool: AU 60%, US 25%, UK 15%
LOCATION_POOL = (
    AU_LOCATIONS * 9   # 63 slots
    + US_LOCATIONS * 3  # 21 slots
    + UK_LOCATIONS * 2  # 12 slots
)
random.shuffle(LOCATION_POOL)


# ---------------------------------------------------------------------------
# Category distribution: 100 distinct task_ids
# ---------------------------------------------------------------------------
CATEGORY_DIST = [
    ("Gardening",         12),
    ("Roof Cleaning",      8),
    ("Lawn Mowing",        8),
    ("Pool Cleaning",      6),
    ("Exterior Painting",  5),
    ("Tax Return",        10),
    ("Online Tutoring",    8),
    ("Graphic Design",     7),
    ("Bookkeeping",        5),
    ("Photography",        9),
    ("Event Setup",        8),
    ("Moving & Removals",  7),
    ("Cleaning",           7),
]

TITLES = {
    "Gardening":         ["Garden tidy and maintenance", "Backyard garden cleanup", "Garden bed weeding and mulching"],
    "Roof Cleaning":     ["Roof moss removal and clean", "High-pressure roof wash", "Roof cleaning and inspection"],
    "Lawn Mowing":       ["Lawn mow front and back", "Weekly lawn mowing service", "Lawn mowing and edging"],
    "Pool Cleaning":     ["Pool clean and chemical balance", "Weekly pool maintenance", "Pool vacuum and filter clean"],
    "Exterior Painting": ["Exterior house repaint", "Fence painting and prep", "Garage exterior repaint"],
    "Tax Return":        ["Individual tax return 2024-25", "Self-employed tax return", "Investment property tax return"],
    "Online Tutoring":   ["Year 10 maths tutoring", "English essay help", "HSC exam preparation"],
    "Graphic Design":    ["Logo design for small business", "Social media graphics pack", "Business card design"],
    "Bookkeeping":       ["Monthly bookkeeping reconciliation", "BAS preparation", "Payroll bookkeeping setup"],
    "Photography":       ["Real estate photography shoot", "Family portrait session", "Event photography coverage"],
    "Event Setup":       ["Birthday party setup and pack down", "Corporate event setup", "Wedding venue decoration"],
    "Moving & Removals": ["2-bedroom apartment move", "Office furniture relocation", "Single room move"],
    "Cleaning":          ["End of lease cleaning", "Deep house clean", "Regular weekly clean"],
}

STATES = ["open", "assigned", "completed", "cancelled"]
STATE_WEIGHTS = [0.2, 0.5, 0.2, 0.1]


def pick_state():
    return random.choices(STATES, weights=STATE_WEIGHTS, k=1)[0]


def pick_budget(category):
    budget_ranges = {
        "Gardening":         (80, 300),
        "Roof Cleaning":     (200, 600),
        "Lawn Mowing":       (60, 180),
        "Pool Cleaning":     (80, 200),
        "Exterior Painting": (400, 1500),
        "Tax Return":        (150, 500),
        "Online Tutoring":   (30, 100),
        "Graphic Design":    (100, 500),
        "Bookkeeping":       (100, 400),
        "Photography":       (200, 800),
        "Event Setup":       (150, 600),
        "Moving & Removals": (200, 1000),
        "Cleaning":          (100, 400),
    }
    lo, hi = budget_ranges[category]
    return round(random.uniform(lo, hi), 2)


# ---------------------------------------------------------------------------
# Build the 100 regular tasks
# ---------------------------------------------------------------------------
PINNED_IDS = {"T003", "T017", "T041", "T058", "T079"}

task_id_counter = 1
location_idx = 0
regular_tasks = []

for category, count in CATEGORY_DIST:
    titles = TITLES[category]
    for _ in range(count):
        while True:
            tid = f"T{task_id_counter:03d}"
            task_id_counter += 1
            if tid not in PINNED_IDS:
                break

        location_id = LOCATION_POOL[location_idx % len(LOCATION_POOL)]
        location_idx += 1

        days_back = random.randint(1, 28)

        is_au = location_id.startswith("LOC_AU")
        is_outdoor = category in ("Gardening", "Roof Cleaning", "Lawn Mowing",
                                   "Pool Cleaning", "Exterior Painting")
        if is_au and is_outdoor and random.random() < 0.25:
            posted_at = days_ago(days_back, hour=23, minute=45)
        else:
            hour = random.randint(7, 17)
            minute = random.choice([0, 15, 30, 45])
            posted_at = days_ago(days_back, hour=hour, minute=minute)

        state = pick_state()
        assigned_at, scheduled_at = pick_timestamps(state, posted_at)

        regular_tasks.append((
            tid, random.choice(titles), category, location_id,
            posted_at, state, pick_budget(category), assigned_at, scheduled_at,
        ))


PINNED_PAIRS = [
    ("T003", "Lawn mowing - front and back", "Lawn Mowing", "LOC_AU_SYD",
        days_ago(23, hour=8, minute=0), "open", 120.0, None, None),
    ("T003", "Lawn mowing - front and back", "Lawn Mowing", "LOC_AU_SYD",
        days_ago(23, hour=10, minute=23), "open", 120.0, None, None),

    ("T017", "End of lease cleaning", "Cleaning", "LOC_UK_LON",
        days_ago(16, hour=13, minute=0), "open", 280.0, None, None),
    ("T017", "End of lease cleaning", "Cleaning", "LOC_UK_LON",
        days_ago(16, hour=14, minute=47), "open", 280.0, None, None),

    ("T041", "Tax return 2024-25", "Tax Return", "LOC_US_NYC",
        days_ago(9, hour=22, minute=0), "open", 350.0, None, None),
    ("T041", "Tax return 2024-25", "Tax Return", "LOC_US_NYC",
        days_ago(9, hour=22, minute=31), "open", 350.0, None, None),
]

PAIR_FORMATS = ["iso_z", "local_offset"]

PINNED_TASKS = [
    ("T058", "Garden cleanup and tidy", "Gardening", "LOC_MISSING_1",
        days_ago(12, hour=9, minute=0), "assigned", 200.0,
        days_ago(11, hour=14, minute=0), days_ahead(6, hour=9)),
    ("T079", "House photography shoot", "Photography", "LOC_MISSING_2",
        days_ago(5, hour=11, minute=0), "open", 450.0, None, None),
]

# ---------------------------------------------------------------------------
# Render rows as text, the way the source system writes them
# ---------------------------------------------------------------------------
LOCATION_TIMEZONES = {
    "LOC_AU_SYD": "Australia/Sydney", "LOC_AU_MEL": "Australia/Melbourne",
    "LOC_AU_BNE": "Australia/Brisbane", "LOC_AU_PER": "Australia/Perth",
    "LOC_AU_ADL": "Australia/Adelaide", "LOC_AU_CBR": "Australia/Sydney",
    "LOC_AU_DAR": "Australia/Darwin",
    "LOC_US_NYC": "America/New_York", "LOC_US_LAX": "America/Los_Angeles",
    "LOC_US_CHI": "America/Chicago", "LOC_US_HOU": "America/Chicago",
    "LOC_US_PHX": "America/Phoenix", "LOC_US_SEA": "America/Los_Angeles",
    "LOC_US_DEN": "America/Denver",
    "LOC_UK_LON": "Europe/London", "LOC_UK_MCR": "Europe/London",
    "LOC_UK_BIR": "Europe/London", "LOC_UK_LEE": "Europe/London",
    "LOC_UK_GLA": "Europe/London", "LOC_UK_EDI": "Europe/London",
}


def render_timestamp(value, location_id, fmt=None):
    """Write a UTC datetime as text in one of the source system's formats."""
    if value is None:
        return "" if random.random() < 0.3 else None
    tz_name = LOCATION_TIMEZONES.get(location_id)
    if fmt is None:
        roll = random.random()
        fmt = "iso_z" if roll < 0.7 else "space_utc" if roll < 0.85 else "local_offset"
    if fmt == "local_offset" and tz_name:
        return value.astimezone(ZoneInfo(tz_name)).isoformat(sep=" ")
    if fmt == "space_utc":
        return value.isoformat(sep=" ")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def render_category(category):
    roll = random.random()
    if roll < 0.05:
        return category.lower()
    if roll < 0.08:
        return category.upper()
    if roll < 0.12:
        return category + " "
    return category


def render_state(state):
    return state.capitalize() if random.random() < 0.1 else state


def render_budget(budget):
    roll = random.random()
    if roll < 0.3:
        return f"${budget:.2f}"
    if roll < 0.4:
        return str(round(budget))
    return f"{budget:.2f}"


def render_row(row, timestamp_format=None):
    tid, title, category, location_id, posted_at, state, budget, assigned_at, scheduled_at = row
    return (
        tid, title, render_category(category), location_id,
        render_timestamp(posted_at, location_id, timestamp_format),
        render_state(state), render_budget(budget),
        render_timestamp(assigned_at, location_id),
        render_timestamp(scheduled_at, location_id),
    )


rendered_rows = (
    [render_row(row) for row in regular_tasks]
    + [
        render_row(row, PAIR_FORMATS[i % 2])
        for i, row in enumerate(PINNED_PAIRS)
    ]
    + [render_row(row) for row in PINNED_TASKS]
)

# ---------------------------------------------------------------------------
# Write to DuckDB
# ---------------------------------------------------------------------------
conn = duckdb.connect(DUCKDB_PATH)
conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
conn.execute("""
    CREATE OR REPLACE TABLE raw.raw_tasks (
        task_id      VARCHAR,
        title        VARCHAR,
        category     VARCHAR,
        location_id  VARCHAR,
        posted_at    VARCHAR,
        state        VARCHAR,
        budget       VARCHAR,
        assigned_at  VARCHAR,
        scheduled_at VARCHAR
    )
""")
conn.executemany("INSERT INTO raw.raw_tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rendered_rows)

count = conn.execute("SELECT COUNT(*) FROM raw.raw_tasks").fetchone()[0]
distinct = conn.execute("SELECT COUNT(DISTINCT task_id) FROM raw.raw_tasks").fetchone()[0]
print(f"Inserted {count} rows, {distinct} distinct task_ids into raw.raw_tasks")

conn.close()
