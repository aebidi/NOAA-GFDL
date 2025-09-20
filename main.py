import os
import yaml
import logging
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
    """Main pipeline orchestrator."""
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)

    # --- Setup ---
    base_path = config['base_data_path']
    log_path = os.path.join(base_path, config['log_file'])
    setup_logging(log_path)
    logging.info("--- GFDL Data Pipeline Started (Africa, Latin America) ---")

    # --- Load Configuration ---
    processing_regions = config.get('processing_regions', [])
    if not processing_regions:
        logging.error("No 'processing_regions' defined in config.yaml. Aborting.")
        return
        
    variable_map = config['variable_categories']
    raw_download_dir = os.path.join(base_path, config['raw_data_dir'])
    ensure_dir_exists(raw_download_dir)

    # --- Main Loop ---
    for dataset in config['datasets']:
        logging.info(f"\n===== Starting Dataset: {dataset['name']} =====")
        
        for variable in dataset.get('variables_to_download', []):
            for mip_table in dataset['mip_tables_to_try']:
                for grid in dataset['grids_to_try']:
                    for member in dataset['ensemble_members']:
                        # iterate through the list of time period objects
                        for period_info in dataset['time_periods']:
                            
                            # extracting the period_string for filename construction
                            period_string = period_info['period_string']
                            
                            url = dataset['url_template'].format(
                                experiment=dataset['experiment'],
                                ensemble_member=member,
                                mip_table=mip_table,
                                variable=variable,
                                grid=grid,
                                time_period=period_string
                            )
                            file_name = url.split('/')[-1]

                            # --- DOWNLOAD STAGE (only once per file) ---
                            raw_file_path = download_file(url, raw_download_dir)

                            # --- MULTI-REGION PROCESSING STAGE ---
                            if raw_file_path:
                                # loop through each defined region
                                for region in processing_regions:
                                    region_name = region['name']
                                    geo_scope = region['bounding_box']
                                    
                                    logging.info(f"-> Processing '{file_name}' for region: {region_name}")

                                    # constructing the region-specific final path
                                    category = get_variable_category(variable, variable_map)
                                    final_processed_dir = os.path.join(
                                        base_path, 
                                        region_name, # subdirectory for the region
                                        dataset['model'], 
                                        dataset['experiment'], 
                                        category
                                    )
                                    ensure_dir_exists(final_processed_dir)
                                    
                                    processed_file_path = os.path.join(final_processed_dir, file_name)
                                    
                                    # processing the single raw file for the current region
                                    process_netcdf_file(raw_file_path, processed_file_path, geo_scope)

    logging.info("\n--- GFDL Data Pipeline Finished ---")


'''
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
'''
if __name__ == "__main__":
    main()