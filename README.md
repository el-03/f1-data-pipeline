# F1 Data Pipeline

Automated ETL pipeline for Formula 1 race data. Efficiently syncs F1 statistics from the [Jolpica F1 API](https://api.jolpi.ca/ergast/f1) to PostgreSQL with smart loading strategies.

## Features

- **Smart Loading**: Automatically determines what data needs updating based on race calendar
- **Two Modes**: Pre-season bulk loading + post-race incremental updates
- **Comprehensive**: Drivers, teams, circuits, results, qualifying, sprints, standings
- **Metadata Tracking**: Prevents duplicates with watermark-based incremental loading
- **GitHub Actions**: Automated scheduling with manual triggers
- **Idempotent**: Safe to re-run without duplication

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env  # Edit with your database credentials

# Initialize database
psql -U user -d dbname -f infra/schema_sql/formula_one.sql
psql -U user -d dbname -f infra/schema_sql/metadata.sql

# Run pre-season load
python main.py --mode pre_season --year 2025

# Run post-race load
python main.py --mode post_race --year 2025
```

## Usage

### CLI Commands

```bash
# Load all pre-season data
python main.py --mode pre_season

# Load post-race results
python main.py --mode post_race

# Load specific table
python main.py --table qualifying_result --year 2024 --round 5

# Force reload (bypass metadata checks)
python main.py --mode pre_season --force
```

### Pipeline Modes

| Mode | Schedule | Tables | Purpose |
|------|----------|--------|---------|
| **pre_season** | Feb 20 annually | circuit, season, team, driver, round, session, team_driver | Bulk load season data |
| **post_race** | Daily (Mar-Dec) | qualifying_result, sprint_result, race_result, driver_championship, team_championship | Load race results |

## Database Schema

### Core Tables
- `driver` - Driver information (name, nationality, car number)
- `team` - Constructor teams
- `circuit` - Race tracks
- `season` - Season metadata
- `round` - Race calendar
- `session` - Practice, qualifying, race sessions
- `race_result` - Race results and fastest laps
- `qualifying_result` - Qualifying positions and times
- `sprint_result` - Sprint race results
- `driver_championship` - Driver standings
- `team_championship` - Constructor standings

### Metadata Tables
- `sync_status` - Tracks loading status and watermarks
- `sync_log` - Audit log of all sync operations

## GitHub Actions

### Automated Schedule
- **Pre-Season**: Feb 20 @ 00:00 UTC
- **Post-Race**: Daily @ 10:00 AM UTC (March-December)

### Required Secrets
```
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
SCHEMA, SCHEMA_METADATA
```

### Manual Trigger
Run via GitHub Actions UI with parameters:
- Mode: `pre_season` or `post_race`
- Year: Season year
- Force: Bypass metadata checks

## Configuration

### Environment Variables (`.env`)
```env
USER_SB=your_db_user
PASSWORD=your_db_password
HOST=your_db_host
PORT=5432
DBNAME=your_db_name
SCHEMA=formula_one
SCHEMA_METADATA=formula_one_pipeline_metadata
```

### API Settings (`config.py`)
```python
JOLPICA_API_BASE = "https://api.jolpi.ca/ergast/f1"
API_TIMEOUT = 30
API_MAX_RETRIES = 3
```

## Architecture

```
Extract (API) → Transform (Clean) → Load (DB) → Update Metadata
```

### Loading Strategies
- **PRE_SEASON**: Load once per season (static data like circuits, calendar)
- **POST_RACE**: Load after each race (3-day buffer for penalties)

### Smart Loading
Pipeline checks metadata to avoid unnecessary API calls:
- Only loads if race occurred 3+ days ago
- Tracks last loaded season/round per table
- Skips already-loaded data

## Development

### Adding a New Loader

1. Create loader in `loaders/__init__.py`:
```python
class NewLoader(BaseLoader):
    def get_entity_name(self) -> str:
        return "new_table"
    
    def extract(self, **kwargs):
        return self.api.get_data()
    
    def transform(self, raw_data):
        return records
    
    def load(self, records):
        return count
```

2. Add config in `config.py`:
```python
"new_table": TableConfig(
    name="new_table",
    strategy=LoadStrategy.POST_RACE,
    dependencies=["round"]
)
```

3. Register in `main.py`:
```python
'new_table': NewLoader(conn, api, metadata)
```

## Data Source

- **API**: [Jolpica F1 API](https://api.jolpi.ca/ergast/f1) (Ergast-compatible)
- **Coverage**: 1950-present
- **Format**: JSON responses