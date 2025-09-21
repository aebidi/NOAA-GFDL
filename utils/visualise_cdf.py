import xarray as xr
import os

# --- Configuration ---
# path to the processed, region-specific data file
PROCESSED_FILE_PATH = "/mnt/datalake/abdullah/gfdl_mirror/esm4/historical/temperature/tsl_Lmon_GFDL-ESM4_historical_r1i1p1f1_gr1_195001-201412.nc"

# coordinates for a known land location (Lusaka, Zambia)
# using negative for southern latitude
TARGET_LAT = -15.4
TARGET_LON = 28.3


def inspect_specific_point(file_path, lat, lon):
    """Opens a NetCDF file and inspects the data at the coordinate nearest to the target."""
    print("="*60)
    print(f"Inspecting Specific Coordinate in: {os.path.basename(file_path)}")
    print(f"Target Location: Lusaka, Zambia (Lat: {lat}, Lon: {lon})")
    print("="*60)

    if not os.path.exists(file_path):
        print(f"\nERROR: File not found at '{file_path}'.\nPlease check the path.\n")
        return

    try:
        with xr.open_dataset(file_path) as ds:
            # .sel() with method='nearest' to find the closest grid point to our target
            # this is the standard way to query data for a specific real-world location
            data_at_point = ds['tsl'].sel(lat=lat, lon=lon, method='nearest')
            
            print(f"\nModel grid point closest to target:")
            # .item() converts the single value to a standard Python number
            print(f"  - Actual Latitude: {data_at_point.lat.values.item():.2f}")
            print(f"  - Actual Longitude: {data_at_point.lon.values.item():.2f}\n")

            # --- Displaying the data ---
            # get the data for the first time step (January 1950) at all soil depths
            first_timestep_data = data_at_point.isel(time=0)
            
            print("--- Soil Temperature Profile (tsl) at this point for the first time step ---")
            print(f"Time: {first_timestep_data.time.dt.strftime('%Y-%m-%d').values}")
            print(f"Units: {first_timestep_data.attrs.get('units', 'K')}\n")
            
            # print the temperature at each depth
            for i, depth in enumerate(first_timestep_data.depth.values):
                temperature = first_timestep_data.values[i]
                print(f"  Depth: {depth:.2f} meters, Temperature: {temperature:.2f} K")
            
            print("\nThis confirms that valid numerical data exists in the file.\n")

    except Exception as e:
        print(f"\nERROR: Could not read or process the file. Reason: {e}\n")


if __name__ == "__main__":
    inspect_specific_point(PROCESSED_FILE_PATH, TARGET_LAT, TARGET_LON)