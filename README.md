# Snowflake Data Engineering Project

End-to-end data engineering project using Snowflake, dbt, and Python for data warehousing, ETL pipelines, and analytics.

## Project Overview

This project demonstrates a modern data engineering stack with:
- **Snowflake** - Cloud data warehouse
- **dbt** - Data transformation and modeling
- **Python** - ETL scripts and data loading

## Project Structure

```
snowflake-data-engineering/
|-- dbt/
|   |-- models/
|       |-- staging/        # Staging models (data cleaning)
|       |-- intermediate/   # Business logic transformations
|       |-- marts/          # Final analytics tables
|-- scripts/
|   |-- etl/               # Python ETL scripts
|-- sql/
|   |-- setup/             # Database setup scripts
|-- requirements.txt       # Python dependencies
|-- README.md
```

## Getting Started

### Prerequisites

- Python 3.9+
- Snowflake account
- dbt CLI

### Installation

1. Clone the repository:
```bash
git clone https://github.com/ksarveshvenkatachalam-lang/snowflake-data-engineering.git
cd snowflake-data-engineering
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=DATA_ENGINEERING_WH
export SNOWFLAKE_DATABASE=DATA_ENGINEERING_DB
```

### Database Setup

Run the setup script in Snowflake:
```sql
-- Execute sql/setup/create_database.sql in Snowflake
```

### Running dbt

```bash
cd dbt
dbt run
dbt test
```

## Architecture

```
[Source Data] --> [RAW Schema] --> [STAGING Schema] --> [ANALYTICS Schema]
                    (Landing)       (Cleaned)           (Business Ready)
```

## Technologies

- **Snowflake** - Data warehouse
- **dbt** - Data transformation
- **Python** - ETL and automation
- **SQL** - Data modeling

## License

MIT License
