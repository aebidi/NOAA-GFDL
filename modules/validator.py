import os
import xarray as xr
import numpy as np
import logging

def validate_netcdf_format(file_path):
    """Checks if a file is a valid and readable NetCDF file"""
    try:
        with xr.open_dataset(file_path, engine="netcdf4") as ds:
            pass
        logging.info(f"  [PASS] Format OK: {os.path.basename(file_path)}")
        return True
    except Exception as e:
        logging.error(f"  [FAIL] Format CORRUPT: {os.path.basename(file_path)}. Reason: {e}")
        return False

def validate_time_coverage(file_path, expected_start, expected_end):
    """Checks if the file's time dimension covers the expected year range"""
    try:
        with xr.open_dataset(file_path) as ds:
            start_year = ds.time.dt.year.min().item()
            end_year = ds.time.dt.year.max().item()
            # checking if the file contains data within the expected range
            if start_year <= expected_start and end_year >= expected_end:
                logging.info(f"  [PASS] Time Coverage OK: Found {start_year}-{end_year}.")
                return True
            else:
                logging.error(f"  [FAIL] Time Coverage INCOMPLETE: Found {start_year}-{end_year}, expected {expected_start}-{expected_end}.")
                return False
    except Exception as e:
        logging.error(f"  [FAIL] Time Coverage UNKNOWN: Could not read time dimension. Reason: {e}")
        return False

def validate_data_range(file_path, variable_id, valid_min, valid_max):
    """Checks if the data values for a variable are within a plausible range"""
    try:
        # explicitly convert min/max values to float to prevent type errors
        valid_min = float(valid_min)
        valid_max = float(valid_max)

        with xr.open_dataset(file_path) as ds:
            sample_data = ds[variable_id].isel(time=slice(0, 10)).values
            min_val = np.nanmin(sample_data)
            max_val = np.nanmax(sample_data)
            if min_val >= valid_min and max_val <= valid_max:
                logging.info(f"  [PASS] Data Range OK: Found min={min_val:.2f}, max={max_val:.2f}.")
                return True
            else:
                logging.error(f"  [FAIL] Data Range UNREALISTIC: Found min={min_val:.2f}, max={max_val:.2f}. Expected range [{valid_min}, {valid_max}].")
                return False
    except Exception as e:
        logging.error(f"  [FAIL] Data Range UNKNOWN: Could not perform check. Reason: {e}")
        return False

def validate_processing_consistency(raw_file_path, processed_file_path, geo_scope, variable_id):
    """Checks that the processed file is a true subset of the raw file"""
    try:
        with xr.open_dataset(raw_file_path) as raw_ds, xr.open_dataset(processed_file_path) as processed_ds:
            raw_subset = raw_ds.sel(
                lat=slice(geo_scope['min_lat'], geo_scope['max_lat']),
                lon=slice(geo_scope['min_lon'], geo_scope['max_lon'])
            )
            raw_mean = raw_subset[variable_id].mean().item()
            processed_mean = processed_ds[variable_id].mean().item()

            if np.isclose(raw_mean, processed_mean):
                logging.info(f"  [PASS] Processing Consistency OK: Means match ({raw_mean:.4f}).")
                return True
            else:
                logging.error(f"  [FAIL] Processing Consistency MISMATCH: Raw mean={raw_mean:.4f}, Processed mean={processed_mean:.4f}.")
                return False
    except Exception as e:
        logging.error(f"  [FAIL] Processing Consistency UNKNOWN: Could not perform check. Reason: {e}")
        return False