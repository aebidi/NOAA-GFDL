import os
import xarray as xr
import logging

def process_netcdf_file(raw_file_path, processed_file_path, geo_scope):
    """
    Opens a raw NetCDF file, subsets it to the specified geographical scope and saves the result to a new file.
    """
    if os.path.exists(processed_file_path):
        logging.info(f"Processed file already exists, skipping: {processed_file_path}")
        return

    logging.info(f"Processing: {raw_file_path}")
    try:
        with xr.open_dataset(raw_file_path) as ds:
            # using .sel() to select data within the latitude and longitude bounds
            # note: we assume the coordinate names are 'lat' and 'lon', might need
            # adjustment if the source files use different names (e.g., 'latitude')
            subset = ds.sel(
                lat=slice(geo_scope['min_lat'], geo_scope['max_lat']),
                lon=slice(geo_scope['min_lon'], geo_scope['max_lon'])
            )
            
            # saving the subsetted data to a new NetCDF file
            subset.to_netcdf(processed_file_path)
            logging.info(f"Successfully processed and saved to: {processed_file_path}")

    except FileNotFoundError:
        logging.error(f"Raw file not found for processing: {raw_file_path}")
    except KeyError as e:
        logging.error(f"Dimension error in {raw_file_path}. Are 'lat' and 'lon' the correct coordinate names? Error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {raw_file_path}. Error: {e}")