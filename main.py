import os
import yaml
from modules.utils import setup_logging, ensure_dir_exists, check_storage
from modules.downloader import download_file
from modules.processor import process_netcdf_file

def get_variable_category(variable, categories_map):
    """Finds the category for a given variable from the config map"""
    for category, variables in categories_map.items():
        if variable in variables:
            return category
    return "uncategorized"

def main():
    """Main pipeline orchestrator"""
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)

    base_path = config['base_data_path']
    log_path = os.path.join(base_path, config['log_file'])
    ensure_dir_exists(base_path)
    logger = setup_logging(log_path)
    logger.info("--- GFDL Data Pipeline Started ---")

    check_storage(base_path, required_gb=50)
    geo_scope = config['geographical_scope']
    variable_map = config['variable_categories']
    
    raw_download_dir = os.path.join(base_path, config['raw_data_dir'])
    ensure_dir_exists(raw_download_dir)

    for dataset in config['datasets']:
        logger.info(f"Starting dataset: {dataset['name']}")
        
        for variable in dataset['variables_to_download']:
            category = get_variable_category(variable, variable_map)
            final_processed_dir = os.path.join(
                base_path, 
                dataset['model'], 
                dataset['experiment'], 
                category
            )
            ensure_dir_exists(final_processed_dir)

            for mip_table in dataset['mip_tables_to_try']:
                # final nested loop to try all grids
                for grid in dataset['grids_to_try']:
                    logger.debug(f"Searching for '{variable}' (MIP: {mip_table}, Grid: {grid})")
                    
                    for member in dataset['ensemble_members']:
                        for period in dataset['time_periods']:
                            # construct the URL with all placeholders
                            url = dataset['url_template'].format(
                                experiment=dataset['experiment'],
                                ensemble_member=member,
                                mip_table=mip_table,
                                variable=variable,
                                grid=grid, 
                                time_period=period
                            )

                            raw_file_path = download_file(url, raw_download_dir)
                            
                            if raw_file_path:
                                file_name = os.path.basename(raw_file_path)
                                processed_file_path = os.path.join(final_processed_dir, file_name)
                                process_netcdf_file(raw_file_path, processed_file_path, geo_scope)

    logger.info("--- GFDL Data Pipeline Finished ---")

if __name__ == "__main__":
    main()