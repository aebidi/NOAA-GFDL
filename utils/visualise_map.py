import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs  # The library for map projections
import os

# --- Configuration ---
# We'll use the same 'tsl' file from our previous inspection.
VARIABLE_TO_PLOT = 'tsl'
RAW_FILE_PATH = "/mnt/datalake/abdullah/gfdl_mirror/raw/tsl_Lmon_GFDL-ESM4_historical_r1i1p1f1_gr1_195001-201412.nc"
PROCESSED_FILE_PATH = "/mnt/datalake/abdullah/gfdl_mirror/esm4/historical/temperature/tsl_Lmon_GFDL-ESM4_historical_r1i1p1f1_gr1_195001-201412.nc"

def plot_netcdf_map(file_path, title, variable):
    """
    Opens a NetCDF file and plots the data for a single time step on a world map.
    """
    print(f"--- Generating plot for: {title} ---")
    
    if not os.path.exists(file_path):
        print(f"ERROR: File not found at {file_path}")
        return

    try:
        with xr.open_dataset(file_path) as ds:
            # Select the data for the very first time step and the first soil depth level
            data_slice = ds[variable].isel(time=0, depth=0)

            # --- Create the Plot ---
            plt.figure(figsize=(12, 6))
            # Use a map projection for the plot axes
            ax = plt.axes(projection=ccrs.PlateCarree())

            # Plot the data. The `transform` argument tells cartopy the data's coordinate system.
            data_slice.plot(ax=ax, transform=ccrs.PlateCarree(), cbar_kwargs={'shrink': 0.6})

            # Add map features for context
            ax.coastlines()
            ax.gridlines(draw_labels=True, linewidth=1, color='gray', alpha=0.5, linestyle='--')
            
            plt.title(title)

            # --- Save the plot to a file ---
            # if you cannot view plots directly from your SSH session, comment out `plt.show()`
            # and uncomment the two lines below. You can then download the PNG file to view it.
            output_filename = f"{title.replace(' ', '_').lower()}.png"
            plt.savefig(output_filename, dpi=150, bbox_inches='tight')
            print(f"Plot saved to: {output_filename}")

    except Exception as e:
        print(f"An error occurred while plotting {file_path}: {e}")

if __name__ == "__main__":
    plot_netcdf_map(RAW_FILE_PATH, "Raw Global Data", VARIABLE_TO_PLOT)
    plot_netcdf_map(PROCESSED_FILE_PATH, "Processed Regional Data", VARIABLE_TO_PLOT)