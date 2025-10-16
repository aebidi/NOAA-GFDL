# NOTE: move to the /gfdl_pipeline directory to run

import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import os
import yaml

# --- Configuration: choose which file pair to visualise ---

# 1. pick a region to see the processed version
REGION_NAME = "Southeast_Asia"

# 2. defining the variable and its category
VARIABLE_ID = "mrso"
VARIABLE_CATEGORY = "soil_moisture" # e.g., "temperature", "radiation", "soil_moisture"

# 3. defining the specific file properties
MIP_TABLE = "day" # e.g., "Lmon", "Amon", "day"
ENSEMBLE_MEMBER = "r1i1p1f1" # e.g., "r1i1p1f1", "r2i1p1f1", "r3i1p1f1"
TIME_PERIOD = "18500101-18691231"
GRID = "gr1" # e.g., "gr1", "gn", "gr", "gr1z"
VERSION = "v20180701" # e.g., "v20190726", "v20180701"

# --- this script builds paths and filenames automatically ---
FILE_NAME = f"{VARIABLE_ID}_{MIP_TABLE}_GFDL-ESM4_historical_{ENSEMBLE_MEMBER}_{GRID}_{TIME_PERIOD}.nc"

# path to the processed file (region-specific)
PROCESSED_FILE_PATH = os.path.join(
    "/mnt/datalake/abdullah/gfdl_mirror",
    REGION_NAME,
    "esm4",
    "historical",
    VARIABLE_CATEGORY,
    FILE_NAME
)

# path to the raw file (global)
RAW_FILE_PATH = os.path.join(
    "/mnt/datalake/abdullah/gfdl_mirror",
    "raw",
    FILE_NAME
)


def plot_netcdf_map(file_path, title, variable, is_regional=False, geo_scope=None):
    """
    Opens a NetCDF file and plots its data on a map.
    If 'is_regional' is True, it zooms to the specified geo_scope.
    """
    print("="*60)
    print(f"Generating map for: {title}")
    print(f"File: {os.path.basename(file_path)}")
    print("="*60)

    if not os.path.exists(file_path):
        print(f"\n[ERROR] FILE NOT FOUND.\n  - Searched for: {file_path}\n")
        return

    try:
        with xr.open_dataset(file_path) as ds:
            data_var = ds[variable]
            
            if 'depth' in data_var.dims:
                data_slice = data_var.isel(time=0, depth=0)
            else:
                data_slice = data_var.isel(time=0)
            
            time_str = data_slice.time.dt.strftime('%Y-%m-%d').item()

            plt.figure(figsize=(12, 8))
            ax = plt.axes(projection=ccrs.PlateCarree())

            data_slice.plot(
                ax=ax,
                transform=ccrs.PlateCarree(),
                cbar_kwargs={'shrink': 0.7, 'label': f'{variable} ({data_slice.attrs.get("units", "")})'}
            )

            ax.coastlines()
            ax.gridlines(draw_labels=True, linewidth=1, color='gray', alpha=0.5, linestyle='--')
            
            # --- KEY LOGIC ---
            # if its a regional plot, zoom in, otherwise show the whole globe
            if is_regional and geo_scope:
                lon_min, lon_max = geo_scope['min_lon'] - 5, geo_scope['max_lon'] + 5
                lat_min, lat_max = geo_scope['min_lat'] - 5, geo_scope['max_lat'] + 5
                ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())
            # --- END OF KEY LOGIC ---
            
            plt.title(f"{title}\nVariable: {variable} | Time: {time_str}")
            
            output_filename = f"map_{title.replace(' ', '_').lower()}.png"
            plt.savefig(output_filename, dpi=150, bbox_inches='tight')
            print(f"\nSUCCESS: Map saved to '{output_filename}'")

    except Exception as e:
        print(f"\n[ERROR] An error occurred while plotting. Reason: {e}\n")


if __name__ == "__main__":
    # load the main config file to get the bounding box for the selected region
    try:
        with open("config.yaml", 'r') as f:
            config = yaml.safe_load(f)
        
        target_region_scope = None
        for region in config.get('processing_regions', []):
            if region['name'] == REGION_NAME:
                target_region_scope = region['bounding_box']
                break
        
        if not target_region_scope:
            print(f"[ERROR] Could not find bounding box for region '{REGION_NAME}' in config.yaml")
        else:
            # --- Plot both files ---
            # 1. Plot the Raw Global File
            plot_netcdf_map(RAW_FILE_PATH, "Raw_Global_Data", VARIABLE_ID, is_regional=False)
            
            # 2. Plot the Processed Regional File
            plot_netcdf_map(PROCESSED_FILE_PATH, f"Processed_Regional_Data_{REGION_NAME}", VARIABLE_ID, is_regional=True, geo_scope=target_region_scope)

    except FileNotFoundError:
        print("[ERROR] Could not find config.yaml. Make sure you are running the script from the 'gfdl_pipeline' directory.")
    except Exception as e:
        print(f"[ERROR] An error occurred while reading the config file: {e}")