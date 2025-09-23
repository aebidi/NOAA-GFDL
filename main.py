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
        
        # get the global discovery lists
        ensemble_members = dataset.get('ensemble_members', [])
        grids_to_try = dataset.get('grids_to_try', [])
        versions_to_try = dataset.get('versions_to_try', [])

        # loop through each defined configuration group (e.g., monthly, daily)
        for group in dataset.get('configuration_groups', []):
            logging.info(f"--> Searching in Group: {group['name']}")
            
            mip_tables = group.get('mip_tables', [])
            time_periods = group.get('time_periods', [])
            variables = group.get('variables', [])

            # --- full discovery nested loops ---
            for variable in variables:
                for mip_table in mip_tables:
                    for member in ensemble_members:
                        for grid in grids_to_try:
                            for version in versions_to_try:
                                for period in time_periods:
                                    
                                    url = dataset['url_template'].format(
                                        experiment=dataset['experiment'],
                                        ensemble_member=member,
                                        mip_table=mip_table,
                                        variable=variable,
                                        grid=grid,
                                        version=version,
                                        time_period=period
                                    )
                                    file_name = url.split('/')[-1]

                                    # --- DOWNLOAD STAGE ---
                                    raw_file_path = download_file(url, raw_download_dir)

                                    # --- MULTI-REGION PROCESSING STAGE ---
                                    if raw_file_path:
                                        for region in processing_regions:
                                            region_name = region['name']
                                            geo_scope = region['bounding_box']
                                            
                                            category = get_variable_category(variable, variable_map)
                                            final_processed_dir = os.path.join(
                                                base_path, region_name, dataset['model'], 
                                                dataset['experiment'], category
                                            )
                                            ensure_dir_exists(final_processed_dir)
                                            
                                            processed_file_path = os.path.join(final_processed_dir, file_name)
                                            
                                            process_netcdf_file(raw_file_path, processed_file_path, geo_scope)

    logging.info("\n--- GFDL Data Pipeline Finished ---")


if __name__ == "__main__":
    main()