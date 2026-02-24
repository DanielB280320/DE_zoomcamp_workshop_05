# DE zoomcamp workshop 5 | Data platforms with bruin.

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

1. Install Bruin CLI: curl -LsSf https://getbruin.com/install/cli | sh
2. Initialize the zoomcamp template: bruin init zoomcamp my-pipeline
3. Configure your .bruin.yml with a DuckDB connection
4. Follow the tutorial in the main module README

After completing the setup, you should have a working NYC taxi data pipeline.

## Project Architecture: 

```
05_data_platforms/
└── bruin/
    ├── .bruin.yml              # Global config: connections, environments (GCP, BigQuery)
    ├── .gitignore
    ├── README.md
    ├── logs/                   # Auto-generated run logs (not tracked in Git)
    └── zoomcamp/
        └── pipeline/
            ├── pipeline.yml    # Pipeline definition: name, schedule, default connections, custom variables. 
            ├── README.md
            └── assets/
                ├── ingestion_hw5/              # Layer 1 - Raw data ingestion
                │   ├── trips.py                # Fetches raw trip data from source
                │   ├── payment_lookup.csv       # Static lookup table
                │   ├── payment_lookup.asset.yml
                │   ├── taxi_zone_lookup.csv     # Static lookup table
                │   ├── taxi_zone_lookup.asset.yml
                │   └── requirements.txt         # Python dependencies
                │
                ├── staging_hw5/                # Layer 2 - Clean & transform
                │   └── trips.sql               # Transforms raw → cleaned and enriched data
                │
                └── reports_hw5/                # Layer 3 - Aggregation & reporting
                    └── trips_report.sql         # Final business-ready data
```

[alt text](./pictures/Architecture.png)

[Source] → ingestion_hw5/ → staging_hw5/ → reports_hw5/ → BigQuery (GCP)


<b> Question 1. Bruin Pipeline Structure </b>

In a Bruin project, what are the required files/directories?

    Answers: .bruin.yml and pipeline/ with pipeline.yml and assets/

<b> Question 2. Materialization Strategies </b>

You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

    Answer: time_interval - incremental based on a time column

<b> Question 3. Pipeline Variables </b>

You have the following variable defined in pipeline.yml:

    variables:
        taxi_types:
            type: array
            items:
                type: string
            default: ["yellow", "green"]

How do you override this when running the pipeline to only process yellow taxis?

    Answer: bruin run --var 'taxi_types=["yellow"]'

<b> Question 4. Running with Dependencies </b>

You've modified the ingestion/trips.py asset and want to run it plus all downstream assets. Which command should you use?

    Answer: bruin run ingestion/trips.py --downstream

<b> Question 5. Quality Checks </b>

You want to ensure the pickup_datetime column in your trips table never has NULL values. Which quality check should you add to your asset definition?

    Answer: name: not_null

<b> Question 6. Lineage and Dependencies </b>

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

    Answer: bruin lineage

<b> Question 7. First-Time Run </b>

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

    Answer: --full-refresh