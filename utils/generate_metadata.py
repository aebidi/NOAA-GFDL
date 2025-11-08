import os
import xarray as xr
import logging

# --- configuration ---
OUTPUT_FILENAME = "/mnt/datalake/abdullah/gfdl_mirror/metadata/METADATA.md"
BASE_PROCESSED_PATH = "/mnt/datalake/abdullah/gfdl_mirror"
REGION_TO_INSPECT = "Southern_Africa"

def main():
    """
    Generates a METADATA.md file by walking through the processed data directories,
    finding all unique variables in each directory, and inspecting one example
    file for each unique variable.
    """
    markdown_content = []
    markdown_content.append("# GFDL Mirrored Dataset: Metadata and Variable Dictionary\n\n")
    markdown_content.append("This document provides a detailed summary of each unique variable present in the processed dataset. The metadata is extracted directly from an example NetCDF file for each variable.\n\n")

    logging.info("--- Metadata Generation Started (v3 - Variable-Aware Scan) ---")

    start_path = os.path.join(BASE_PROCESSED_PATH, REGION_TO_INSPECT)
    
    if not os.path.isdir(start_path):
        logging.error(f"Inspection directory not found: {start_path}")
        return

    # using os.walk to traverse the directory tree
    for root, dirs, files in os.walk(start_path):
        # filter for only .nc files
        nc_files = [f for f in files if f.endswith('.nc')]
        
        if not nc_files:
            continue

        # 1. find all unique variable IDs in the current directory based on filenames
        unique_variables_in_dir = sorted(list({f.split('_')[0] for f in nc_files}))
        
        # create a main header for the directory path
        relative_path = os.path.relpath(root, start_path)
        markdown_content.append(f"### Path: `/{relative_path}`\n\n")
        
        # 2. loop through each unique variable found in this directory
        for variable_id in unique_variables_in_dir:
            
            # finding the first file that matches this variable to use as an example
            example_file = next((f for f in nc_files if f.startswith(variable_id + '_')), None)
            if not example_file:
                continue

            file_path_to_check = os.path.join(root, example_file)
            logging.info(f"Inspecting '{variable_id}' using file: {example_file}")

            try:
                with xr.open_dataset(file_path_to_check) as ds:
                    # --- extract and format Metadata for this specific variable ---
                    markdown_content.append(f"#### Variable: `{variable_id}`\n\n")
                    
                    if variable_id not in ds.data_vars:
                         markdown_content.append(f"- **Error:** Variable ID '{variable_id}' not found in the data variables of this file. Skipping.\n\n---\n")
                         logging.warning(f"Variable '{variable_id}' not found in file {example_file}. Filename might not match content.")
                         continue

                    var_meta = ds[variable_id].attrs
                    markdown_content.append(f"- **Long Name:** {var_meta.get('long_name', 'N/A')}\n")
                    markdown_content.append(f"- **Units:** {var_meta.get('units', 'N/A')}\n")
                    markdown_content.append(f"- **Standard Name:** `{var_meta.get('standard_name', 'N/A')}`\n")
                    
                    markdown_content.append("\n**File Provenance (from Global Attributes):**\n")
                    for key, value in ds.attrs.items():
                        if key in ['source_id', 'history', 'tracking_id', 'variant_label', 'table_id', 'experiment_id', 'frequency']:
                            markdown_content.append(f"- **{key}:** {value}\n")
                    
                    markdown_content.append("\n---\n")

            except Exception as e:
                logging.error(f"Could not read metadata from {example_file}. Reason: {e}")

    # --- write the final Markdown file ---
    output_path = os.path.join(os.getcwd(), OUTPUT_FILENAME)
    with open(output_path, 'w') as f:
        f.writelines(markdown_content)

    logging.info(f"--- Metadata Generation Finished. File saved to: {output_path} ---")
    print(f"\nSUCCESS: Metadata documentation has been generated and saved to '{OUTPUT_FILENAME}'")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()