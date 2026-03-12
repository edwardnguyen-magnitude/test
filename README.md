# Reg Demo

Example dbt projects for the medallion pipeline — regulatory compliance and warranty claims data.

## Projects

- **projects/reg/** — Regulatory compliance: substances, products, surveys, regulatory lists
- **projects/claims/** — Warranty claims: claim lifecycle, dealer performance, anomaly detection

Each project is self-contained with its own `dbt_project.yml`, `profiles.yml`, seeds, models, and Cube views.

## Quick Start

```bash
# 1. Install dbt
uv sync

# 2. Start Postgres + Cube
docker compose up -d

# 3. Install dbt packages
uv run dbt deps --project-dir projects/reg --profiles-dir projects/reg

# 4. Seed + run
uv run dbt seed --project-dir projects/reg --profiles-dir projects/reg
uv run dbt run --project-dir projects/reg --profiles-dir projects/reg
```

## Project Structure

```
projects/
├── reg/                            # Regulatory compliance dbt project
│   ├── models/
│   │   ├── staging/                # Raw → typed models
│   │   ├── intermediate/           # Business logic joins
│   │   └── marts/                  # Dashboard-ready tables
│   ├── seeds/                      # Sample data CSVs
│   ├── cube/                       # Cube.js view definitions
│   └── profiles.yml
├── claims/                         # Warranty claims dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── seeds/
│   ├── cube/
│   └── profiles.yml

config/
├── airbyte_setup.yml               # Airbyte connection definitions
└── data_sources.yml                # Data source configuration

datacube/                           # Cube.js semantic layer (shared)
├── cube.py                         # Cube config
├── model/
│   ├── globals.py                  # dbt manifest → Cube model bridge
│   └── cubes/marts.yml.jinja       # Auto-generated cube definitions from dbt marts

scripts/
└── generate_seeds.py               # Regenerate seed CSVs
```

## Configuration

Copy `.env.example` to `.env`. Key variables:

| Variable | Purpose |
|----------|---------|
| `DB_*` | PostgreSQL connection |
| `CUBEJS_*` | Cube.js API |

## Using with the Medallion Pipeline

These dbt projects are designed to be used with the medallion pipeline. Point the medallion image at this repo's `projects/` and `config/` directories.
