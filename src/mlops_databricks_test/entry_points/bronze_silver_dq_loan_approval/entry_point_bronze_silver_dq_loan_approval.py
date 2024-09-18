import argparse
import importlib
import os
import pkg_resources
from mlops.dq_processors.dq_loader import DQLoader
from mlops.factory.config_manager import ConfigurationManager
from mlops.factory.input_dataframe_manager import InputDataFrameManager
from mlops.factory.output_dataframe_manager import OutputDataFrameManager
from mlops.utils.common import set_env_variables
from mlops.utils.spark_manager import PysparkSessionManager
from mlops.utils.sql_utils import SqlUtils

# Mapping for argument types used in argparse
TYPE_MAPPING = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool
}


def generate_args_code(entry_points_config):
    """Generate command-line arguments based on the entry point configuration."""
    parser = argparse.ArgumentParser(description="Entry point arguments")

    for param in entry_points_config['input_parameters']:
        # Define each argument
        name = f"--{param['name']}"
        param_type = param['type']
        required = param['required']
        help_text = param['help']
        choices = param.get('choices')

        if param_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type: {param_type}")

        # Add argument to the parser
        parser.add_argument(name, type=TYPE_MAPPING[param_type], required=required, help=help_text, choices=choices)

    # Parse and return the arguments
    return parser.parse_args()


def get_spark_session(config, platform):
    """Initialize and return a Spark session."""
    return PysparkSessionManager.start_session(config=config, platform=platform)


def get_data_sources_dfs(spark, entry_point_config):
    """Create and return dataframes for the input data sources defined in the entry point configuration."""
    return InputDataFrameManager(spark, entry_point_config).create_dataframes().get_dataframes()


def load_and_execute_function(entry_point_config, input_df_list, config):
    """Load a function dynamically from a module and execute it."""
    transformations_config = entry_point_config.get('transformations')
    function_path = transformations_config.get("entry_point_function_path")
    package_name = config.get('package_name')
    module_name, function_name = function_path.rsplit(".", 1)

    # Import the module and get the function
    module = importlib.import_module(f"{package_name}.entry_points.{module_name}")
    function = getattr(module, function_name)

    # Execute the function with the input dataframes and configuration
    return function(input_df_list, config)


def write_data_to_sinks(spark, output_df_dict, entry_point_config):
    """Write the transformed dataframes to the output sinks as defined in the entry point configuration."""
    OutputDataFrameManager(spark, output_df_dict, entry_point_config).write_data_to_sinks()


def execute_pre_sql_transformations(spark, sql_transformation_configs):
    """Execute SQL transformations that need to be performed before the main function."""
    transformation_df_dict = {}
    for sql_transformation_config in sql_transformation_configs:
        if sql_transformation_config["execution_type"] == "pre_function":
            transformation_df_dict[sql_transformation_config['transformation_name']] = SqlUtils.execute_sql_query(spark,
                                                                                                                  sql_transformation_config)
    return transformation_df_dict


def execute_post_sql_transformations(spark, sql_transformation_configs):
    """Execute SQL transformations that need to be performed after the main function."""
    transformation_df_dict = {}
    for sql_transformation_config in sql_transformation_configs:
        if sql_transformation_config["execution_type"] == "post_function":
            transformation_df_dict[sql_transformation_config['transformation_name']] = SqlUtils.execute_sql_query(spark,
                                                                                                                  sql_transformation_config)
    return transformation_df_dict


def handle_entry_point(entry_point_config, config, input_parameters):
    """Handle the logic to execute the appropriate function based on the entry point type."""
    entry_point_type = entry_point_config.get('type')

    if entry_point_type == "table-manager":
        # Execute a function that manages tables
        load_and_execute_function(entry_point_config, None, config)
    elif entry_point_type == "source-sink":
        # Process data from source to sink with optional transformations
        transformed_df_dict = {}
        dq_dfs_dict = {}
        pre_sql_transformation_dfs = {}
        post_sql_transformation_dfs = {}
        spark_config = entry_point_config.get('spark_configs', {})
        platform = config.get('platform')

        # Start Spark session
        spark = get_spark_session(spark_config, platform)

        # Get input dataframes from sources
        input_df_dict = get_data_sources_dfs(spark, entry_point_config)

        # Perform pre-function SQL transformations
        transformations = entry_point_config.get('transformations')
        sql_transform_configs = transformations.get('sql_transformation_configs')
        if sql_transform_configs:
            pre_sql_transformation_dfs = execute_pre_sql_transformations(spark, sql_transform_configs)

        # Combine pre-SQL transformations with input dataframes
        pre_sql_transformation_dfs = {**pre_sql_transformation_dfs, **input_df_dict}

        # Execute main function (if any)
        if transformations:
            transformed_df_dict = load_and_execute_function(entry_point_config, pre_sql_transformation_dfs, config)

        # Combine main function output with pre-SQL transformations
        transformed_df_dict = {**transformed_df_dict, **pre_sql_transformation_dfs}

        # Perform post-function SQL transformations
        if sql_transform_configs:
            post_sql_transformation_dfs = execute_post_sql_transformations(spark, sql_transform_configs)

        # Combine post-SQL transformations with the previous results
        post_sql_transformation_dfs = {**post_sql_transformation_dfs, **transformed_df_dict}

        # Perform Data Quality checks if specified
        dq_specs = entry_point_config.get('dq_specs')
        if dq_specs:
            dq_dfs_dict = execute_df_specs(spark, post_sql_transformation_dfs, entry_point_config)

        # Combine all transformations and DQ checks
        dq_dfs_dict = {**transformed_df_dict, **input_df_dict, **dq_dfs_dict}

        # Write the final data to the sinks
        write_data_to_sinks(spark, dq_dfs_dict, entry_point_config)
    else:
        raise ValueError(f"Unsupported entry point type: {entry_point_type}")


def execute_df_specs(spark, df_dict, config):
    """Execute data quality checks on the transformed dataframes."""
    dq_loader = DQLoader(config)
    return dq_loader.process_dq(spark, df_dict)


def main():
    """Main entry point of the script."""
    entry_point = 'bronze_silver_dq_loan_approval'  # Define the entry point for the processing

    # Load configuration from the specified path
    config_path = pkg_resources.resource_filename('mlops_databricks_test', 'configs/dev.yaml')
    config_manager = ConfigurationManager(config_path)
    config = config_manager.get_config_as_json()

    # Get the configuration specific to the entry point
    entry_point_config = config_manager.get_entry_point_config(entry_point)
    input_parameters = {}

    # Set environment variables if specified in the configuration
    if 'env_variables' in entry_point_config:
        set_env_variables(entry_point_config['env_variables'])

    # Generate and parse input arguments if they are defined
    if 'input_parameters' in entry_point_config:
        input_parameters = generate_args_code(entry_point_config)

    # Handle the entry point logic based on its type
    handle_entry_point(entry_point_config, config, input_parameters)


if __name__ == '__main__':
    # Execute the main function when the script is run
    main()
