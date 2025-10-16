## **GFDL Data Mirroring Pipeline: Final Documentation & Handover**

**Version:** 4.0
**Date:** 5th October 2025
**Status:** Phase 1 (Historical Data, All Regions) - COMPLETE & AUTOMATED

### 1. Executive Summary

This document provides a complete overview of the GFDL Data Mirroring Pipeline, a project initiated to secure irreplaceable climate datasets from NOAA's GFDL. **Phase 1 of this project is now complete.**

The pipeline has successfully mirrored all available Priority 1 historical (1850-2014) data from the GFDL-ESM4 model. A key success was the "Full Discovery" process, which uncovered and downloaded a rich dataset including **daily-frequency data** and **multiple ensemble members** for critical variables, significantly enhancing the value of the mirrored dataset for detailed analysis.

The raw global data has been processed for five key geographical regions, resulting in a **~99% reduction in data size** per region. The final output is a collection of analysis-ready, region-specific datasets. The entire pipeline is now fully automated via a weekly CRON job.

This document serves as the primary technical guide for the ML team to begin using the data and for future maintenance of the pipeline.

---

### 2. For the ML Team: Using the Processed Data

This is the guide to accessing and using the analysis-ready dataset.

#### **2.1. Data Location & Structure**

The processed data is located at `/mnt/datalake/abdullah/gfdl_mirror/`. It is organised by region at the top level:

```
/gfdl_mirror/
├── East_Africa/
├── Latin_America/
├── Southeast_Asia/
├── Southern_Africa/
├── West_Africa/
├── raw/                 <-- IGNORE THIS DIRECTORY (for pipeline use only)
├── validation.log
└── cron.log
```

Within each regional folder, the structure is: `esm4/historical/<variable_category>/<filename.nc>`

#### **2.2. File Naming Convention**

Filenames are descriptive and contain all necessary metadata. Parse them to find the data you need.

**Example:** `ts_Amon_GFDL-ESM4_historical_r3i1p1f1_gr1_v20180701_195001-201412.nc`

*   **Variable:** `ts` (Surface Temperature)
*   **MIP Table / Frequency:** `Amon` (Monthly)
*   **Model:** `GFDL-ESM4`
*   **Experiment:** `historical`
*   **Ensemble Member:** `r3i1p1f1`
*   **Grid:** `gr1`
*   **Version:** `v20180701`
*   **Time Period:** `195001-201412`

#### **2.3. Key Data Characteristics**

*   **Format:** NetCDF4 (`.nc`).
*   **Key Transformation:** The data has been **geographically subsetted**. Each file contains a "slice" of the global data corresponding to its parent region's bounding box.
*   **Coordinates:** Standard Latitude/Longitude (WGS 84).
*   **Units:** All variables are in standard scientific units (e.g., Temperature in `K`, Radiation in `W m-2`). Units are stored in each file's metadata and can be read by `xarray`.
*   **`nan` Values:** **CRITICAL:** Grid cells over oceans or large bodies of water will contain `nan` ("Not a Number") values for land-based variables (e.g., `tsl`, `mrso`). Data loading and analysis code **must** handle or ignore these `nan` values.

#### **2.4. How to Load the Data (Python Example)**

The recommended library is `xarray`.

```python
import xarray as xr

# Example: Load a processed file for Latin America
file_path = "/mnt/datalake/abdullah/gfdl_mirror/Latin_America/esm4/historical/temperature/ts_Amon_GFDL-ESM4_historical_r3i1p1f1_gr1_v20180701_195001-201412.nc"

# Open the dataset
ds = xr.open_dataset(file_path)

# Print a summary of the file's contents
print(ds)

# --- Example: Get data for a specific location (e.g., near São Paulo, Brazil) ---
# xarray automatically finds the nearest grid point to the coordinates you provide.
sao_paulo_temp = ds['ts'].sel(lat=-23.5, lon=-46.6, method='nearest')

# Plot the time series for this location
sao_paulo_temp.plot()
```

---

### 3. The Data Pipeline Architecture

The pipeline code is located at `/mnt/datalake/abdullah/GFDL/gfdl_pipeline/`.

*   **`config.yaml` (The "Brain"):** This file controls everything. It defines the regions, the data to search for (variables, ensembles, grids, versions), and the rules for validation. The pipeline is driven by a "Full Discovery" logic, grouped by time frequency (Monthly, Daily) for efficiency.
*   **`main.py` (The "Orchestrator"):** This is the main script that reads the `config.yaml` and executes the workflow. It uses a "Download Once, Process Many" strategy for high efficiency.
*   **`modules/downloader.py` (The "Worker"):** A robust module that handles downloading files. It is idempotent (skips existing files) and handles network errors gracefully.
*   **`modules/processor.py` (The "Value-Add"):** This module performs the most critical task: geographical subsetting. It uses `xarray` to carve out regional data from the global raw files.
*   **`modules/validator.py` (The "QA"):** A library of functions that check for file corruption, time coverage, data range sanity, and processing consistency.
*   **`modules/utils.py` (The "Toolbox"):** Shared helper functions for logging and directory management.

---

### 4. How to Run the Pipeline & Tools

All commands should be run from `/mnt/datalake/abdullah/GFDL/gfdl_pipeline/` after activating the virtual environment (`source /mnt/datalake/abdullah/GFDL/venv/bin/activate`).

*   **To Update Data (Main Pipeline):**
    ```bash
    python main.py
    ```
    This will run the full discovery and processing workflow, automatically skipping any data that is already up-to-date.

*   **To Run Data Integrity Checks:**
    ```bash
    python integrity_checker.py
    ```
    This audits the entire existing dataset against the rules in `config.yaml`. Results are saved to `/mnt/datalake/abdullah/gfdl_mirror/validation.log`.

*   **To Visualise Data:**
    *   **Text Summary:** Configure and run `python visualise_cdf.py` to print a detailed summary of any processed file.
    *   **Map Visualisation:** Configure and run `python visualise_map.py` to generate PNG maps comparing a raw global file to its processed regional version.

*   **Automation (CRON Job):**
    *   The pipeline is fully automated by a CRON job defined in the `abdullah` user's crontab.
    *   **Schedule:** It runs every **Sunday at 2:00 AM**.
    *   **Script:** It executes the wrapper script `run_pipeline.sh`, which handles the virtual environment, directory changes, and a lock file to prevent overlapping runs.
    *   **Log File:** All output from the automated runs is appended to `/mnt/datalake/abdullah/gfdl_mirror/cron.log`.

---

### **Appendix A: Final Data Availability Status (Phase 1)**

This table provides the definitive list of all data successfully mirrored.

| Category          | Variable ID | Description                        | Available Time Frequencies | Available Ensemble Members                |
| ----------------- | ----------- | ---------------------------------- | -------------------------- | ----------------------------------------- |
| Energy Fluxes     | `hfls`      | Latent Heat Flux                   | Monthly (`Amon`)           | `r1i1p1f1`                                |
| Energy Fluxes     | `hfss`      | Sensible Heat Flux                 | Monthly (`Amon`)           | `r1i1p1f1`                                |
| Soil Moisture     | `mrro`      | Root Zone Soil Moisture            | Monthly (`Lmon`), Daily (`day`) | `r1i1p1f1`                                |
| Soil Moisture     | `mrso`      | Total Soil Moisture                | Monthly (`Lmon`)           | `r1i1p1f1`                                |
|                   |             |                                    | Daily (`day`)              | **`r1i1p1f1`, `r2i1p1f1`, `r3i1p1f1`**      |
| Soil Moisture     | `mrsol`     | Soil Moisture by Layer             | Monthly (`Emon`)           | `r1i1p1f1`                                |
| Radiation         | `rsds`      | Downward Shortwave Radiation       | Monthly (`Amon`)           | **`r1i1p1f1`, `r3i1p1f1`**                  |
|                   |             |                                    | Daily (`day`)              | `r1i1p1f1`                                |
| Radiation         | `rss`       | Net Shortwave Radiation            | Monthly (`Emon`)           | `r1i1p1f1`                                |
| Radiation         | `rsus`      | Upward Shortwave Radiation         | Monthly (`Amon`)           | `r1i1p1f1`                                |
| Temperature       | `ts`        | Surface Temperature                | Monthly (`Amon`)           | **`r1i1p1f1`, `r2i1p1f1`, `r3i1p1f1`**      |
| Temperature       | `tsl`       | Soil Temperature by Layer          | Monthly (`Lmon`)           | `r1i1p1f1`                                |

*Note: The variables `par` and `hfgs` were attempted but confirmed to be unavailable from the source for the historical period.*

### **Appendix B: Geographical Scopes for Processed Data**

| Region Name       | `min_lon` | `max_lon` | `min_lat` | `max_lat` |
| ----------------- | --------- | --------- | --------- | --------- |
| **Southern_Africa** | 12.0      | 41.0      | -26.0     | -4.0      |
| **East_Africa**     | 22.0      | 52.0      | -12.0     | 22.0      |
| **West_Africa**     | -20.0     | 20.0      | -4.0      | 25.0      |
| **Latin_America**   | -118.0    | -34.0     | -56.0     | 33.0      |
| **Southeast_Asia**  | 90.0      | 142.0     | -12.0     | 25.0      |
