# Import required modules from GX library.
import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations.exceptions import DataContextError

# Create Data Context.
context = gx.get_context(mode="file", project_root_dir="./validation")

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
connection_string = "mariadb+pymysql://root:${password}@${host}:3306/loan_etl_dev?charset=utf8mb4"
datasource_name = "loan model"
print(context.data_sources.get(datasource_name))
data_source = context.data_sources.get(datasource_name)
if not data_source:
    print("data source is not found")
    context.data_sources.add_sql(
    datasource_name, connection_string=connection_string
)
else:
    print("data source is found")

print(context.data_sources.get(datasource_name))

asset_name = "debtor"
database_table_name = "debtor"
if not data_source.get_asset(asset_name):
    table_data_asset = data_source.add_table_asset(
        table_name=database_table_name, name=asset_name
    )

print(data_source.assets)

full_table_batch_definition = data_source.get_asset(asset_name).get_batch_definition("FULL_TABLE")
if not full_table_batch_definition:
    full_table_batch_definition = data_source.get_asset(asset_name).add_batch_definition_whole_table(
        name="FULL_TABLE"
    )

# Verify that the Batch Definition is valid
full_table_batch = full_table_batch_definition.get_batch()
print("batch is being printed head")
print(full_table_batch.head())

preset_expectation = gx.expectations.ExpectColumnValuesToBeUnique(
    column="loan_id"
)

validation_result = full_table_batch.validate(preset_expectation)
print(validation_result)


suite_name = "my_expectation_suite"
suite = gx.ExpectationSuite(name=suite_name)

suite = context.suites.get(suite_name)
if not suite:
    suite = context.suites.add(suite)
    suite.add_expectation(preset_expectation)
    suite.save()
else:
    print("suite is found")


definition_name = "my_validation_definition"

try:
    validation_definition = context.validation_definitions.get(definition_name)
except DataContextError:
    validation_definition = None

if not validation_definition:
    validation_definition = gx.ValidationDefinition(
        data=full_table_batch_definition, suite=suite, name=definition_name
    )
    validation_definition = context.validation_definitions.add(validation_definition)

validation_results = validation_definition.run()
print(validation_results)   