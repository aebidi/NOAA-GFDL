import xarray as xr

# path to the file that was successfully downloaded before the error
file_to_inspect = "/mnt/datalake/abdullah/gfdl_mirror/raw_regional/mrro_HRM3_gfdl_1968010103.nc"

print(f"--- Inspecting File: {file_to_inspect} ---")
try:
    with xr.open_dataset(file_to_inspect) as ds:
        print("\n--- Dimensions ---")
        print(ds.dims)
        print("\n--- Coordinates ---")
        print(ds.coords)
        print("\nLook for the coordinate names that represent latitude and longitude in the list above.")
except Exception as e:
    print(f"Error reading file: {e}")