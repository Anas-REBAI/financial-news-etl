import os
from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition, sensor, RunRequest
from etl.assets import news, pdf_generation, prices, returns

# Load all assets from the modules
all_assets = load_assets_from_modules([news, pdf_generation, prices, returns])

# Create a job that executes all assets
my_pipeline = define_asset_job(
    name="my_first_pipeline",
    selection="*", 
)

# Schedule to run the pipeline every day at midnight
daily_schedule = ScheduleDefinition(
    job=my_pipeline,
    cron_schedule="*/2 * * * *",
    # cron_schedule="0 0 * * *",
)

# Trigger to monitor a folder
@sensor(job=my_pipeline)
def my_file_sensor(context):
    for file in os.listdir("etl"):
        if file.endswith(".py"):
            yield RunRequest(run_key=file)

# Definitions for Dagster
defs = Definitions(
    assets=all_assets,
    schedules=[daily_schedule],
    sensors=[my_file_sensor],
)