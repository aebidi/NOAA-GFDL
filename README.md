**GFDL Data Mirroring Pipeline: Documentation**

**Version:** 5.0\
**Date:** 2025-11-07\
**Author:** Abdullah Mubasshar

<h2>1. Executive Summary</h2>

This document provides a complete overview of the GFDL Data Mirroring Pipeline, a project to secure and process irreplaceable climate datasets from NOAA\'s GFDL. All planned project phases are now complete.

The pipeline has successfully mirrored all available Priority 1, 2, and 3 data for the target variables from the GFDL-ESM4 (historical and future scenarios) and GFDL-SPEAR (seasonal forecast historical) models. A key success was the \"Full Discovery\" process, which uncovered and downloaded a rich dataset including daily-frequency data and multiple ensemble members, significantly enhancing the value of the mirrored dataset.

The raw global data has been processed for five key geographical regions: Southern Africa, East Africa, West Africa, Latin America, and Southeast Asia, resulting in a \~99% reduction in data size per region. The entire pipeline is automated via a CRON job (currently disabled) for long-term maintenance.

This document serves as the definitive technical guide for the ML team to begin using the data and for future maintenance of the pipeline.

<h2>2. The Data Pipeline</h2>

The pipeline is a modular Python application designed for scalability and maintainability. It is controlled entirely by a central configuration file.

**2.1 Architecture & Structure**

The code is located at /mnt/datalake/abdullah/gfdl_pipeline/. The structure remains modular, where the config.yaml file specifies global search parameters (ensembles, grids, versions) which are then applied to groups of variables organized by time frequency (e.g., Monthly, Daily).
```
gfdl_pipeline/
├── main.py              # orchestrator: Runs the entire pipeline.
├── config.yaml          # control File: Defines ALL parameters and targets.
├── modules/
│   ├── downloader.py     # handles file downloads.
│   ├── processor.py      # handles geographical subsetting.
│   ├── validator.py      # contains data integrity check functions.
└── utils.py              # helper functions.
```

**2.2 Multi-Region Workflow**

The pipeline\'s workflow is now region-aware:

1.  **Load Configuration:** main.py reads the config.yaml file, including the new list of processing_regions.

2.  **Discover and Download:** It loops through the defined datasets (e.g., ESM4_historical). For each variable, it discovers and downloads the raw global file into the central staging directory (/mnt/datalake/abdullah/gfdl_mirror/raw/). This step is skipped if the file already exists.

3.  **Iterate and Process:** Upon securing a raw file, the pipeline **loops through every region defined in processing_regions**. For each region, it:

    -   Calls the processor.py module.

    -   Subsets the raw file using the region\'s specific bounding box.

    -   Saves the processed, region-specific file to a new, structured output directory.

**2.3 How to Manage Regions**

The pipeline is controlled by the config.yaml file. To add, remove, or modify a geographical scope, simply edit the processing_regions list. No code changes are required.

**Example config.yaml structure for defining regions:**
```
processing_regions:
  - name: "Southern_Africa"
    bounding_box: { min_lon: 12.0, max_lon: 41.0, min_lat: -26.0, max_lat: -4.0 }
  - name: "East_Africa"
    bounding_box: { min_lon: 22.0, max_lon: 52.0, min_lat: -12.0, max_lat: 22.0 }
  # Add new regions here...
```

<h2>3. Data Specifications: For the ML Team</h2>

**3.1 Raw Data Specifications (NOT to be used by ML Team)**

The specifications for the raw data remain unchanged. It is global, large, and serves as the temporary source material for processing.

-   **Location:** Archived to [Azure Blob Storage](https://syvlarchive.blob.core.windows.net/noaa-container?sp=racwdlmeop&st=2025-10-28T21:11:04Z&se=2026-10-29T05:26:04Z&spr=https&sv=2024-11-04&sr=c&sig=HRl8pEZvikXcqDn7Z6%2FLbpOCV4ZwEWX2l0k8qJqrpOs%3D).

-   **Usage:** This directory is for pipeline use only. **The ML team should NOT use this data directly.**

**3.2 Processed Data Specifications**

This is the most critical update for the ML team. The processed data is now organised by region at the top level.

-   **Location:** /mnt/datalake/abdullah/gfdl_mirror/

-   **New Directory Structure:**
```
/gfdl_mirror/
├── Southern_Africa/
│   └── esm4/
│       └── historical/
│           ├── radiation/
│           ├── soil_moisture/
│           ├── energy_fluxes/
│           └── temperature/
│       └── scenarios/
│   └── spear/
│
├── East_Africa/
│   └── esm4/
│       └── ...
├── West_Africa/
│   └── esm4/
│       └── ...
└── Latin_America/
    └── esm4/
        └── ...
```

-   **File Naming and Format:** Unchanged. Files are NetCDF4 (.nc) and retain their original, descriptive names.

-   **Data Content:** Each file is a geographically subsetted version of the raw global data, containing only the data points within the bounding box of its parent region. All notes regarding nan values and the use of xarray still apply.

**3.3 File Naming Convention**

Filenames are descriptive and contain all necessary metadata.\
Example:\
`ts_Amon_GFDL-ESM4_historical_r3i1p1f1_gr1_v20180701_195001-201412.nc`

Within each regional folder, the structure is:\
\<model\>/\<type\>/\<variable_category\>/\<filename.nc\>

**Examples:**

-   \.../Latin_America/esm4/historical/temperature/

-   \.../Latin_America/esm4/scenarios/ssp245/soil_moisture/

-   \.../Latin_America/spear/forecast_historical/precipitation/

| **Component**             | **Meaning**                |
| ------------------------- | -------------------------- |
| **Variable**              | `ts` (Surface Temperature) |
| **MIP Table / Frequency** | `Amon` (Monthly)           |
| **Model**                 | `GFDL-ESM4`                |
| **Experiment**            | `historical`               |
| **Ensemble Member**       | `r3i1p1f1`                 |
| **Grid**                  | `gr1`                      |
| **Version**               | `v20180701`                |
| **Time Period**           | `195001 – 201412`          |


**3.4 Key Data Characteristics**

-   **Format:** NetCDF4 (.nc)

-   **Transformation:** Each file contains a geographically subsetted "slice" of the global data for its parent region.

-   **Coordinates:** Standard Latitude/Longitude (WGS 84).

-   **Units:** Standard scientific units (e.g., Temperature in K, Radiation in W m⁻²).\
    Units are stored in metadata and readable via xarray.

-   **nan Values:** Grid cells over oceans or large water bodies contain nan values for land-based variables. Your code must handle these appropriately.

**3.5 How to Load the Data (Python Example)**

```python
import xarray as xr

# Example: Load a processed file for Latin America
file_path = "/mnt/datalake/abdullah/gfdl_mirror/Latin_America/esm4/historical/temperature/ts_Amon_GFDL-ESM4_historical_r3i1p1f1_gr1_v20180701_195001-201412.nc"

# Open the dataset
ds = xr.open_dataset(file_path)

# Print a summary of the file’s content
print(ds)

# Example: Get data for a specific location (e.g., São Paulo)
sao_paulo_temp = ds['ts'].sel(lat=-23.5, lon=-46.6, method='nearest')

# Plot the time series
sao_paulo_temp.plot()
```

**3.6 Critical Data Notes**

-   **NaN Values:** Grid cells over oceans will contain NaN values for land-based variables. Code must handle or ignore them.

-   **Units:** Stored in metadata (e.g., temperature in Kelvin, precipitation in kg m-2 s-1).

<h2>4. Example Visualisations</h2>

**4.1 Global Raw Data (var "mrso", region "global", time period "1850")**

![](utils/map_processed_regional_data_southeast_asia.png){width="5.427408136482939in" height="2.8303576115485565in"}

**4.2 Processed Data (var "mrso", region "Southeast Asia", time period "1850")**

![](utils/map_raw_global_data.png){width="5.410714129483814in" height="3.837134733158355in"}

<h2>5. How to Run the Pipeline & Tools</h2>

Enter project directory and activate the virtual environment:\
`source /mnt/datalake/abdullah/GFDL/venv/bin/activate`

All commands must be run from\
`/mnt/datalake/abdullah/GFDL/gfdl_pipeline/`

**Run Full Pipeline (Main Pipeline):**
```bash
python main.py
```
**Run a Specific Dataset:**
```bash
python main.py \--name \<dataset_name\>ESM4_SPEAR_historical

e.g., python main.py \--name ESM4_SPEAR_historical
```
**Run Data Integrity Checks:**
```bash
python integrity_checker.py
```
Results are saved to:\
/mnt/datalake/abdullah/gfdl_mirror/validation.log

**Generate Metadata:**
```bash
python generate_metadata.py
```
**Visualise Data:**

-   **Text Summary:** `python visualise_cdf.py`

-   **Map Visualization:** `python visualise_map.py`

**Automation (CRON Job)**

-   **Schedule:** Every Sunday at 2:00 AM

-   **Script:** run_pipeline.sh (handles venv and locking)

-   **Logs:** /mnt/datalake/abdullah/gfdl_mirror/cron.log

**Note:**

-   A weekly CRON job is already configured but disabled.

-   To re-enable: open crontab (crontab -e) and uncomment the line containing `run_pipeline.sh`.

<h2>6. Data Management & Archiving: Cold Storage Workflow</h2>

To manage the large volume of data generated by this pipeline, we employ a two-tiered storage strategy.

-   **Raw Data:** The large, intermediate raw data (~2.3TBs) is archived to a cost-effective cold storage solution in Azure Blob Storage.

-   **Processed Data:** The high-value, analysis-ready processed data (~1TB) remains locally for fast ML access across regions.

This section details the procedures for archiving new raw data and retrieving it from the archive when needed.

**6.1 Archiving Raw Data to Cold Storage (Azure Blob)**

This procedure should be followed after a major data acquisition phase (like completing Phase 1 or 2) to free up local disk space.
The primary tool for this is Microsoft’s `azcopy` command-line utility.

*Prerequisites*

- `azcopy` must be installed on the server:
```bash
sudo mv azcopy_linux_amd64_*/azcopy /usr/local/bin/
```

- You must obtain a valid SAS URL for the target Azure Blob container with Write/Delete permissions from the cloud administrator.

**Procedure**

1. Construct the `azcopy` Command

The command to recursively copy the entire raw directory is:
```bash
azcopy copy '/mnt/datalake/abdullah/gfdl_mirror/raw/' 'YOUR_FULL_SAS_URL_HERE' --recursive
```

Replace `YOUR_FULL_SAS_URL_HERE` with the valid SAS URL, which in our case is `https://syvlarchive.blob.core.windows.net/noaa-container?sp=racwdlmeop&st=2025-10-28T21:11:04Z&se=2026-10-29T05:26:04Z&spr=https&sv=2024-11-04&sr=c&sig=HRl8pEZvikXcqDn7Z6%2FLbpOCV4ZwEWX2l0k8qJqrpOs%3D`

The single quotes are important.

2. Execute in a Stable Session

A full transfer of terabytes can take many hours or days.
It is highly recommended to run this command inside a screen or tmux session to ensure the process continues even if you disconnect.
```bash
# 1. Start a new named session
screen -S gfdl_archive_transfer

# 2. Run the azcopy command inside the screen session
azcopy copy '/mnt/datalake/abdullah/gfdl_mirror/raw/' 'https://syvlarchive.blob.core.windows.net/noaa-container?sp=racwdlmeop&st=2025-10-28T21:11:04Z&se=2026-10-29T05:26:04Z&spr=https&sv=2024-11-04&sr=c&sig=HRl8pEZvikXcqDn7Z6%2FLbpOCV4ZwEWX2l0k8qJqrpOs%3D' --recursive

# 3. Detach from the session by pressing Ctrl+A, then D
# Re-attach later with:
screen -r gfdl_archive_transfer
```

3. Verify and Clean Up

After `azcopy` completes:

- Use the Azure Portal to visually inspect the blob container and confirm that files and directory structure have been uploaded successfully.

- Once verified, you can safely delete the local raw files to free up space:
```bash
# WARNING: This is a permanent deletion. Verify the cloud copy first.
rm -rf /mnt/datalake/abdullah/gfdl_mirror/raw/*
```

**5.2 Accessing/Retrieving Raw Data from Cold Storage**

There may be cases where you need to retrieve a raw file — for example, to re-process it for a new region or to debug an issue.
`azcopy` is also used for retrieval.

**Prerequisites**

- You must have a valid SAS URL for the Azure Blob container with Read permissions.

**Procedure 1: Downloading a Single File (for Spot-Checking or Debugging)**

This is the most common use case.

1. Identify the File Path:
Determine the full relative path of the file you need within the archive (e.g.,
`raw/esm4/historical/temperature/ts_Amon_...nc).`

2. Construct the `azcopy` Command:
Use the `--include-path` flag.
```bash
azcopy copy '<SAS_URL>' '<local_destination_directory>' --include-path '<path/to/your/file.nc>'
```

*Example:*

To download a single temperature file to your current directory:
```bash
# Create a temporary directory for the download
mkdir ~/temp_retrieval
cd ~/temp_retrieval

# Run the command
azcopy copy 'YOUR_SAS_URL_HERE' . --include-path 'raw/ts_Amon_GFDL-ESM4_historical_r3i1p1f1_gr1_v20180701_195001-201412.nc'
```

**Procedure 2: Downloading All Files for a Specific Variable**

This is useful if you need to re-process an entire variable.

*Construct the Command:*
Use the `--include-pattern` and `--recursive flags`.

*Example:*
To download all `tsl` (Soil Temperature) files from the historical dataset:
```bash
# Create a dedicated directory
mkdir ~/tsl_retrieval

# Run the command
azcopy copy 'YOUR_SAS_URL_HERE' '~/tsl_retrieval' --include-pattern 'tsl_*.nc' --recursive
```

This command will scan the entire container and download any file starting with `tsl_` into the `~/tsl_retrieval` directory.

**Important Note:**
Retrieving large volumes of data from cloud storage can incur costs and take a significant amount of time.
Always retrieve only the specific files you need.

<h2>7. Phase 4: Documentation & Metadata Summary</h2>

-   **METADATA.md** contains full variable-level dictionary.

-   **Scientific References:**

    -   GFDL-ESM4: <https://doi.org/10.1029/2019MS002015>

    -   GFDL-SPEAR: <https://doi.org/10.1029/2019MS001895>


-   **Contact:**  

    -   <GFDL.Climate.Model.Info@noaa.gov> (data content)

    -   <oar.gfdl.webmaster-data1@noaa.gov> (web development, system administration, etc.)


### **Appendix A: Final Data Availability Status**

This table provides the definitive list of all data successfully mirrored.

| **Category**  | **Variable ID** | **Description**              | **Source Model** | **Available Frequencies**   | **Ensembles**                |
| ------------- | --------------- | ---------------------------- | ---------------- | --------------------------- | ---------------------------- |
| Energy Fluxes | hfls            | Latent Heat Flux             | ESM4             | Monthly (Amon)              | r1i1p1f1                     |
| Energy Fluxes | hfss            | Sensible Heat Flux           | ESM4             | Monthly (Amon)              | r1i1p1f1                     |
| Soil Moisture | mrro            | Root Zone Soil Moisture      | ESM4             | Monthly (Lmon), Daily (day) | r1i1p1f1                     |
| Soil Moisture | mrso            | Total Soil Moisture          | ESM4             | Monthly (Lmon), Daily (Day) | r1i1p1f1, r2i1p1f1, r3i1p1f1 |
| Soil Moisture | mrsol           | Soil Moisture by Layer       | ESM4             | Monthly (Emon)              | r1i1p1f1                     |
| Radiation     | rsds            | Downward Shortwave Radiation | ESM4             | Monthly (Amon), Daily (day) | r1i1p1f1, r3i1p1f1           |
| Radiation     | rss             | Net Shortwave Radiation      | ESM4             | Monthly (Emon)              | r1i1p1f1                     |
| Radiation     | rsus            | Upward Shortwave Radiation   | ESM4             | Monthly (Amon)              | r1i1p1f1                     |
| Temperature   | ts              | Surface Temperature          | ESM4             | Monthly (Amon)              | r1i1p1f1, r2i1p1f1, r3i1p1f1 |
| Temperature   | tsl             | Soil Temperature by Layer    | ESM4             | Monthly (Lmon)              | r1i1p1f1                     |
| Temperature   | tas             | Near-Surface Air Temp        | SPEAR            | Monthly (Amon), Daily (Day) | r1–r30 available             |
| Temperature   | tasmax          | Daily Maximum Near-Surface Air Temp | SPEAR            | Daily (Day)                 | r1–r30 available |
| Temperature   | tasmin          | Daily Minimum Near-Surface Air Temp | SPEAR            | Daily (Day)                 | r1–r30 available |
| Precipitation | pr              | Precipitation                       | SPEAR            | Monthly (Amon), Daily (Day) | r1–r30 available |
| Humidity      | hus             | Specific Humidity                   | SPEAR            | Monthly (Amon)              | r1–r30 available |
| Wind          | sfcWind         | Near-Surface Wind Speed             | SPEAR            | Monthly (Amon)              | r1–r30 available |


### **Appendix B: Attempted but Unavailable Variables**
The robust pipeline attempted to find and download the following variables using multiple common MIP tables, but they were not available from the data source for the historical experiment.

*Implication for ML Team:*
These variables are not present in the mirrored dataset and should not be included in analysis plans for the historical period.
| **Category**  | **Variable ID** | **Description**                     | **Status**                                  |
| ------------- | --------------- | ----------------------------------- | ------------------------------------------- |
| Radiation     | par             | Photosynthetically Active Radiation | **Unavailable – File not found on server.** |
| Energy Fluxes | hfgs            | Ground Heat Flux                    | **Unavailable – File not found on server.** |


### **Appendix C: Project Decisions Log**
Priority 3 (NARCCAP): Cancelled.
Reason: Dataset only covers North America and does not overlap with project regions (Africa, Latin America, SE Asia). Proceeding would have consumed storage and pipeline time without benefit.


### **Appendix D: Geographical Scopes for Processed Data**

| Region Name       | `min_lon` | `max_lon` | `min_lat` | `max_lat` |
| ----------------- | --------- | --------- | --------- | --------- |
| **Southern_Africa** | 12.0      | 41.0      | -26.0     | -4.0      |
| **East_Africa**     | 22.0      | 52.0      | -12.0     | 22.0      |
| **West_Africa**     | -20.0     | 20.0      | -4.0      | 25.0      |
| **Latin_America**   | -118.0    | -34.0     | -56.0     | 33.0      |
| **Southeast_Asia**  | 90.0      | 142.0     | -12.0     | 25.0      |

**Southern Africa**
- Zambia
- Malawi
- Mozambique
- Zimbabwe
- Angola
- DRC
- Tanzania
- Namibia

**East Africa**
- Kenya
- Uganda
- Rwanda
- Burundi
- Ethiopia
- Eritrea
- Djibouti
- Somalia
- South Sudan
- Sudan (sometimes classified as East Africa)

**West Africa**
- Nigeria
- Ghana
- Senegal
- Mali
- Burkina Faso
- Niger
- Guinea
- Sierra Leone
- Liberia
- Ivory Coast (Côte d'Ivoire)
- Benin
- Togo
- Gambia
- Guinea-Bissau
- Cape Verde
- Mauritania

**Latin America**
- Brazil
- Argentina
- Colombia
- Chile
- Mexico
- Costa Rica
- Peru
- Paraguay
- Bolivia
- Venezuela

**Southeast Asia**
- Philippines
- Indonesia
- Thailand
- Vietnam
- Malaysia
- Singapore
- Cambodia
