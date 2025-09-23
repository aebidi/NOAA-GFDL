import xarray as xr
import os
import numpy as np

# --- Configuration: choose which processed file to inspect ---

# 1. Pick a region. Options: "Southern_Africa", "East_Africa", "West_Africa", "Latin_America", "Southeast_Asia"
REGION_NAME = "Southeast_Asia"

# 2. Define the variable and its category to build the path.
VARIABLE_ID = "mrso"
VARIABLE_CATEGORY = "soil_moisture" # e.g., "temperature", "radiation", "soil_moisture"

# 3. Define the specific file properties.
MIP_TABLE = "day" # e.g., "Lmon", "Amon", "day"
ENSEMBLE_MEMBER = "r1i1p1f1" # e.g., "r1i1p1f1", "r2i1p1f1", "r3i1p1f1"
TIME_PERIOD = "18500101-18691231"
GRID = "gr1" # e.g., "gr1", "gn", "gr", "gr1z"
VERSION = "v20180701" # e.g., "v20190726", "v20180701"

# --- this script builds the path and filename automatically ---
FILE_NAME = f"{VARIABLE_ID}_{MIP_TABLE}_GFDL-ESM4_historical_{ENSEMBLE_MEMBER}_{GRID}_{TIME_PERIOD}.nc"
PROCESSED_FILE_PATH = os.path.join(
    "/mnt/datalake/abdullah/gfdl_mirror",
    REGION_NAME,
    "esm4",
    "historical",
    VARIABLE_CATEGORY,
    FILE_NAME
)


def inspect_processed_file(file_path, variable_id):
    """
    Opens a processed NetCDF file and prints a detailed textual summary of its contents.
    """
    print("="*70)
    print(f"INSPECTING PROCESSED FILE: {os.path.basename(file_path)}")
    print(f"LOCATION: {os.path.dirname(file_path)}")
    print("="*70)

    if not os.path.exists(file_path):
        print(f"\n[ERROR] FILE NOT FOUND.\n  - Please check the configuration at the top of the script.")
        print(f"  - Searched for: {file_path}")
        return

    try:
        with xr.open_dataset(file_path) as ds:
            # --- 1. Global Attributes ---
            print("\n[1] GLOBAL ATTRIBUTES:")
            for key, value in ds.attrs.items():
                print(f"  - {key}: {value}")

            # --- 2. Dimensions ---
            print("\n[2] DIMENSIONS:")
            for dim, size in ds.dims.items():
                print(f"  - {dim}: {size}")

            # --- 3. Coordinates ---
            print("\n[3] COORDINATES:")
            for name, coord in ds.coords.items():
                print(f"  - {name} ({coord.dims}):")
                print(f"    - Values: {coord.values.min()} to {coord.values.max()}")
                if 'units' in coord.attrs:
                    print(f"    - Units: {coord.attrs['units']}")

            # --- 4. Data Variables ---
            print("\n[4] DATA VARIABLES:")
            for name, var in ds.data_vars.items():
                print(f"  - {name} {var.dims}:")
                for attr, value in var.attrs.items():
                    print(f"    - {attr}: {value}")

            # --- 5. Data Sample from a Central Point ---
            print("\n[5] DATA SAMPLE:")
            if variable_id in ds.data_vars:
                target_lat = (ds.lat.min() + ds.lat.max()).item() / 2
                target_lon = (ds.lon.min() + ds.lon.max()).item() / 2
                
                data_at_point = ds[variable_id].sel(lat=target_lat, lon=target_lon, method='nearest')
                
                print(f"  - Sampling variable '{variable_id}' near the center of the region.")
                print(f"  - Model grid point closest to target: Lat={data_at_point.lat.item():.2f}, Lon={data_at_point.lon.item():.2f}")
                
                sample_slice = data_at_point.isel(time=0)
                
                # using the xarray method to format the date string
                # .dt accessor works on datetime coordinates
                # .strftime() formats the date
                # .item() extracts the single value cleanly
                date_string = sample_slice.time.dt.strftime('%Y-%m-%d').item()

                print(f"  - Value(s) at first time step ({date_string}):")
                print(f"    {sample_slice.values}")
            else:
                print(f"  - Variable '{variable_id}' not found in this file's data variables.")

    except Exception as e:
        print(f"\n[ERROR] An error occurred while reading the file: {e}")

if __name__ == "__main__":
    inspect_processed_file(PROCESSED_FILE_PATH, VARIABLE_ID)