import os
import yaml
import logging
from modules.utils import setup_logging
from modules.validator import (
    validate_netcdf_format,
    validate_time_coverage,
    validate_data_range,
    validate_processing_consistency
)

def get_variable_category(variable, categories_map):
    """Finds the category for a given variable from the config map"""
    for category, variables in categories_map.items():
        if variable in variables:
            return category
    return "uncategorized"

def main():
    """Standalone script to validate the existing GFDL dataset"""
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)

    # --- setup ---
    base_path = config['base_data_path']
    log_path = os.path.join(base_path, "validation.log")
    setup_logging(log_path)
    logging.info("--- Dataset Validation Started ---")

    # --- load configuration for validation ---
    geo_scope = config['geographical_scope']
    validation_rules = config.get('validation_rules', {})
    raw_dir_name = config['raw_data_dir']

    # --- iterate through expected files ---
    for dataset in config['datasets']:
        logging.info(f"\n===== Validating Dataset: {dataset['name']} =====")
        
        for variable in dataset.get('variables_to_download', []):
            for mip_table in dataset['mip_tables_to_try']:
                for grid in dataset['grids_to_try']:
                    for member in dataset['ensemble_members']:
                        # iterate through the list of time period objects
                        for period_info in dataset['time_periods']:
                            
                            # extract the explicit values from the config
                            period_string = period_info['period_string']
                            expected_start = period_info['start_year']
                            expected_end = period_info['end_year']

                            # reconstruct the filename using the period_string
                            file_name = dataset['url_template'].format(
                                experiment=dataset['experiment'],
                                ensemble_member=member,
                                mip_table=mip_table,
                                variable=variable,
                                grid=grid,
                                time_period=period_string # using string for the filename
                            ).split('/')[-1]

                            category = get_variable_category(variable, config['variable_categories'])
                            raw_file_path = os.path.join(base_path, raw_dir_name, file_name)
                            processed_file_path = os.path.join(base_path, dataset['model'], dataset['experiment'], category, file_name)

                            if not os.path.exists(raw_file_path):
                                continue
                            
                            logging.info(f"--- Checking: {file_name} ---")

                            # 1. Validate raw file
                            logging.info("-> Validating Raw File...")
                            if validate_netcdf_format(raw_file_path):
                                # pass the explicit years to the validation function
                                validate_time_coverage(raw_file_path, expected_start, expected_end)
                                
                                var_range = validation_rules.get('variable_ranges', {}).get(variable, validation_rules.get('variable_ranges', {}).get('default'))
                                if var_range:
                                    validate_data_range(raw_file_path, variable, var_range['min'], var_range['max'])

                            # 2. Validate processed file
                            if not os.path.exists(processed_file_path):
                                logging.warning(f"  [WARN] Processed file is missing for existing raw file: {file_name}")
                                continue
                            
                            logging.info("-> Validating Processed File...")
                            if validate_netcdf_format(processed_file_path):
                                validate_processing_consistency(raw_file_path, processed_file_path, geo_scope, variable)

    logging.info("\n--- Dataset Validation Finished ---")

if __name__ == "__main__":
    main()