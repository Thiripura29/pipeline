
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

TYPE_MAPPING = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool
}


def generate_args_code(entry_points_config):
    parser = argparse.ArgumentParser(description="Entry point arguments")

    for param in entry_points_config['parameters']:
        name = f"--{param['name']}"
        param_type = param['type']
        required = param['required']
        help_text = param['help']
        choices = param.get('choices')

        if param_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type: {param_type}")

        parser.add_argument(name, type=TYPE_MAPPING[param_type], required=required, help=help_text, choices=choices)

    return parser.parse_args()


def get_spark_session(config, platform):
    return PysparkSessionManager.start_session(config=config, platform=platform)


def get_data_sources_dfs(spark, entry_point_config):
    return InputDataFrameManager(spark, entry_point_config).create_dataframes().get_dataframes()


def load_and_execute_function(entry_point_config, input_df_list, config):
    function_path = entry_point_config.get("transformation_function_path")
    package_name = config.get('package_name')
    module_name, function_name = function_path.rsplit(".", 1)
    module = importlib.import_module(f"{package_name}.entry_points.{module_name}")
    function = getattr(module, function_name)
    return function(input_df_list, config)


def write_data_to_sinks(spark, output_df_dict, entry_point_config):
    OutputDataFrameManager(spark, output_df_dict, entry_point_config).write_data_to_sinks()


def handle_entry_point(entry_point_config, config):
    entry_point_type = entry_point_config.get('type')
    if entry_point_type == "table-manager":
        load_and_execute_function(entry_point_config, None, config)
    elif entry_point_type == "source-sink":
        transformed_df_dict = {}
        dq_dfs_dict = {}
        spark_config = entry_point_config.get('spark_configs', {})
        platform = config.get('platform')
        spark = get_spark_session(spark_config, platform)
        input_df_dict = get_data_sources_dfs(spark, entry_point_config)
        transform_function = entry_point_config.get('transformation_function_path')
        if transform_function:
            transformed_df_dict = load_and_execute_function(entry_point_config, input_df_dict, config)

        transformed_df_dict = {**transformed_df_dict, **input_df_dict}

        dq_specs = entry_point_config.get('dq_specs')
        if dq_specs:
            dq_dfs_dict = execute_df_specs(spark, transformed_df_dict, entry_point_config)

        dq_dfs_dict = {**transformed_df_dict, **input_df_dict, **dq_dfs_dict}

        write_data_to_sinks(spark, dq_dfs_dict, entry_point_config)
    else:
        raise ValueError(f"Unsupported entry point type: {entry_point_type}")


def execute_df_specs(spark, df_dict, config):
    dq_loader = DQLoader(config)
    return dq_loader.process_dq(spark, df_dict)


def main():
    entry_point = 'silver_loan_approval'
    
    config_path = pkg_resources.resource_filename('mlops_databricks_test', 'configs/dev.yaml')
    config_manager = ConfigurationManager(config_path)
    config = config_manager.get_config_as_json()
    entry_point_config = config_manager.get_entry_point_config(entry_point)

    if 'env_variables' in entry_point_config:
        set_env_variables(entry_point_config['env_variables'])

    handle_entry_point(entry_point_config, config)


if __name__ == '__main__':
    main()

