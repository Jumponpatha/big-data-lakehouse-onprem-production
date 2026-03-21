import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir="/opt/airflow/great_expectations")

# Define the Data Source name
data_source_name = "bronze_zone_data_source"

# Add the Data Source to the Data Context
data_source = context.data_sources.add_pandas(name=data_source_name)