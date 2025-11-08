import os
import xarray as xr
import logging
import numpy as np

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



'''
# NEW LOGIC FOR DOWNLOADING (PRIORITY 3) REGIONAL NARCCAP DATASET

def find_coord_names(dataset):
    """
    Auto-detects the names of the latitude and longitude coordinates and their dimensions.
    Handles both 1D (rectilinear) and 2D (curvilinear) coordinate systems.
    """
    lat_name, lon_name = None, None
    # a common pattern is that 'lat' and 'lon' are 2D coordinates themselves
    if 'lat' in dataset.coords and 'lon' in dataset.coords:
        if len(dataset['lat'].dims) == 2 and len(dataset['lon'].dims) == 2:
            # this is likely a curvilinear grid
            return 'lat', 'lon', True # return a flag indicating it's curvilinear

    # fallback to the previous method for rectilinear grids
    for coord_name in dataset.coords:
        if 'units' in dataset[coord_name].attrs:
            units = dataset[coord_name].attrs['units']
            if units == 'degrees_north':
                lat_name = coord_name
            elif units == 'degrees_east':
                lon_name = coord_name
    return lat_name, lon_name, False # return flag indicating not curvilinear

def process_netcdf_file(raw_file_path, processed_file_path, geo_scope):
    """
    Opens a raw NetCDF file, subsets it to the specified geographical scope,
    and saves the result. Handles both rectilinear and curvilinear grids.
    """
    if os.path.exists(processed_file_path):
        logging.info(f"  [INFO] Processed file already exists, skipping: {os.path.basename(processed_file_path)}")
        return

    logging.info(f"Processing: {os.path.basename(raw_file_path)}")
    try:
        with xr.open_dataset(raw_file_path) as ds:
            lat_name, lon_name, is_curvilinear = find_coord_names(ds)

            if not lat_name or not lon_name:
                raise ValueError("Could not auto-detect lat/lon coordinate names.")

            if is_curvilinear:
                # --- NEW LOGIC FOR CURVILINEAR GRIDS ---
                logging.info(f"  - Detected curvilinear grid with coordinates: lat='{lat_name}', lon='{lon_name}'")
                
                # finding the indices of the grid cells within the bounding box.
                # this creates a boolean mask of the same shape as the lat/lon arrays.
                mask = (
                    (ds[lat_name] >= geo_scope['min_lat']) & (ds[lat_name] <= geo_scope['max_lat']) &
                    (ds[lon_name] >= geo_scope['min_lon']) & (ds[lon_name] <= geo_scope['max_lon'])
                )
                
                # checking if any data points fall within the bounding box
                if not mask.any():
                    logging.warning(f"  [WARN] No data points for this file fall within the '{geo_scope['name']}' bounding box. Skipping.")
                    return

                # find the min/max indices along each dimension (e.g., yc, xc) where the mask is True.
                # this defines the smallest possible rectangle on the native grid that contains all our points.
                grid_dims = ds[lat_name].dims
                y_dim, x_dim = grid_dims[0], grid_dims[1]
                
                y_indices = np.any(mask, axis=1)
                x_indices = np.any(mask, axis=0)
                
                # using these boolean arrays of indices to slice the dataset
                subset = ds.isel({y_dim: y_indices, x_dim: x_indices})
                
            else:
                # --- ORIGINAL LOGIC FOR RECTILINEAR GRIDS ---
                logging.info(f"  - Detected rectilinear grid with coordinates: lat='{lat_name}', lon='{lon_name}'")
                subset_criteria = {
                    lat_name: slice(geo_scope['min_lat'], geo_scope['max_lat']),
                    lon_name: slice(geo_scope['min_lon'], geo_scope['max_lon'])
                }
                subset = ds.sel(**subset_criteria)
            
            subset.to_netcdf(processed_file_path)
            logging.info(f"  [SUCCESS] Successfully processed and saved to: {os.path.basename(processed_file_path)}")

    except Exception as e:
        logging.error(f"  [FAIL] Could not process {os.path.basename(raw_file_path)}. Reason: {e}")

'''