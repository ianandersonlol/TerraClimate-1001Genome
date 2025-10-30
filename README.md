# TerraClimate Data Extraction Pipeline

A modern Python pipeline for extracting climate data from TerraClimate for 1001 Arabidopsis Genome accessions.

## Overview

This pipeline downloads monthly climate data (1958-present) for 1,061 Arabidopsis thaliana accessions from the TerraClimate database. The data is optimized for GWAS (Genome-Wide Association Studies) with mutual information analysis.

## What's New in This Version

**Major improvements over the original R script:**

- **100-1000x faster** - Fixed the critical bug where lat/lon arrays were loaded 1,061 times per variable
- **Modular architecture** - Separate modules for indexing, extraction, transformation, and validation
- **Efficient caching** - Spatial indices are computed once and cached
- **Multiple output formats** - Parquet (fast, compressed) or CSV
- **Flexible aggregation** - Monthly, annual, seasonal, or summary statistics per accession
- **Data validation** - Automatic quality checks and reporting
- **Derived indices** - Automatically compute aridity index, water balance, temperature range, etc.
- **Better error handling** - Checkpoint progress and log failures

## Installation

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- numpy, pandas, xarray
- netCDF4 (for reading TerraClimate data)
- pyarrow (for parquet format)
- tqdm (progress bars)

### 2. Verify input data

Ensure `availableplantsforclimate.csv` is present with columns:
- `CS_number` (accession ID)
- `latitude` (decimal degrees)
- `longitude` (decimal degrees)

**Note:** If your CSV has different column names, the pipeline will attempt to auto-detect and use the first three columns as ID, latitude, and longitude respectively.

## Usage

### Quick Start

Run the complete pipeline with default settings (recommended for GWAS):

```bash
python main.py
```

This will:
1. Build spatial index (cached for reuse)
2. Extract all 14 climate variables
3. Validate data quality
4. Create summary statistics per accession (one row per accession)
5. Save as `climate_data_for_gwas.parquet`

### Common Use Cases

**Extract specific variables only:**
```bash
python main.py --variables tmax,tmin,ppt,soil
```

**Keep monthly resolution (not aggregated):**
```bash
python main.py --aggregation monthly
```

**Create annual means instead of overall summary:**
```bash
python main.py --aggregation annual
```

**Output as CSV:**
```bash
python main.py --format csv
```

**Limit time range:**
```bash
python main.py --start-year 1980 --end-year 2020
```

**Rebuild spatial index from scratch:**
```bash
python main.py --rebuild-index
```

### Testing Individual Modules

You can test individual components of the pipeline:

```bash
# Test spatial indexing only
python spatial_index.py

# Test extraction for a single variable
python extract.py

# Test transformation functions
python transform.py
```

Each module has a `__main__` block that demonstrates its core functionality.

### Command Line Options

```
--variables           Comma-separated list of variables to extract (default: all 14)
                      Example: --variables tmax,tmin,ppt

--aggregation         Temporal aggregation method (default: summary)
                      Options: summary | annual | seasonal | monthly
                      - summary: One row per accession with overall statistics
                      - annual: One row per accession-year
                      - seasonal: One row per accession-season-year
                      - monthly: Keep full monthly resolution

--format              Output file format (default: parquet)
                      Options: parquet | csv | both

--no-validation       Skip data validation step (faster but not recommended)

--rebuild-index       Force rebuild of spatial index cache from scratch

--start-year          Filter data to years >= this value
                      Example: --start-year 1980

--end-year            Filter data to years <= this value
                      Example: --end-year 2020

--derived             Add derived climate indices (default: True)
                      Indices: aridity, water balance, temp range, moisture availability
```

## Climate Variables

The pipeline extracts these variables from TerraClimate:

| Variable | Description | Units |
|----------|-------------|-------|
| `aet` | Actual Evapotranspiration | mm/month |
| `def` | Climatic Water Deficit | mm/month |
| `pet` | Potential Evapotranspiration | mm/month |
| `ppt` | Precipitation | mm/month |
| `q` | Runoff | mm/month |
| `soil` | Soil Moisture | mm |
| `srad` | Solar Radiation | W/m² |
| `swe` | Snow Water Equivalent | mm |
| `tmax` | Maximum Temperature | °C |
| `tmin` | Minimum Temperature | °C |
| `vap` | Vapor Pressure | kPa |
| `ws` | Wind Speed | m/s |
| `vpd` | Vapor Pressure Deficit | kPa |
| `PDSI` | Palmer Drought Severity Index | unitless |

### Derived Climate Indices

When `--derived` is enabled (default), the pipeline also computes:

- **Aridity Index**: `PET / PPT` (higher = more arid)
- **Water Balance**: `PPT - AET`
- **Temperature Range**: `tmax - tmin`
- **Moisture Availability**: `soil - def`

## Output Formats

### Summary Format (default, recommended for GWAS)

One row per accession with summary statistics:

```
accession_id, tmax_mean, tmax_std, tmax_min, tmax_max, tmin_mean, ...
CS76347,      23.4,       5.2,      11.2,     34.1,     12.3,      ...
```

### Monthly Format

One row per accession-month:

```
accession_id, year, month, tmax, tmin, ppt, ...
CS76347,      1958, 1,     15.2, 3.4,  85.2, ...
CS76347,      1958, 2,     16.8, 4.1,  72.3, ...
```

### Annual Format

One row per accession-year with yearly statistics:

```
accession_id, year, tmax_mean, tmax_std, ...
CS76347,      1958, 18.5,      6.2,      ...
CS76347,      1959, 19.1,      5.8,      ...
```

### Seasonal Format

One row per accession-season-year:

```
accession_id, year, season, tmax_mean, tmax_std, ...
CS76347,      1958, Winter, 10.2,      3.1,      ...
CS76347,      1958, Spring, 18.5,      4.2,      ...
```

## Project Structure

```
.
├── main.py              # Main pipeline script
├── config.py            # Configuration settings
├── spatial_index.py     # Spatial indexing (matches plants to grid cells)
├── extract.py           # Climate data extraction
├── transform.py         # Data transformation and aggregation
├── validate.py          # Data quality validation
├── requirements.txt     # Python dependencies
│
├── availableplantsforclimate.csv  # Input: accession coordinates
│
├── cache/              # Cached spatial indices (auto-created)
│   └── spatial_index.pkl
├── output/             # Output data files (auto-created)
│   ├── climate_data_for_gwas.parquet
│   ├── validation_report.txt
│   └── extraction_failures.log
│
└── archive/            # Original R script and old CSV files (if present)
    ├── ClimateData_pull.r
    └── arabidopsis_*_data.csv
```

**Note:** The `cache/`, `output/`, and `data/` directories are automatically created on first run if they don't exist.

## How It Works

### 1. Spatial Indexing (spatial_index.py)

**Problem in old script:** The R script loaded the global lat/lon arrays 1,061 times per variable (14,854 times total!), causing massive slowdown.

**Solution:** Build the spatial index once:
- Load lat/lon grid from one sample NetCDF file
- For each accession, find its nearest grid cell
- Cache the mapping as `accession_id -> (lat_index, lon_index)`
- Reuse this index for all variables

### 2. Extraction (extract.py)

For each climate variable:
- Open NetCDF file once (not 1,061 times!)
- Extract time series for all accessions using cached indices
- Use xarray's efficient indexing
- Return as pandas DataFrame

### 3. Transformation (transform.py)

Transform raw monthly data into analysis-ready format:
- Merge all variables into wide format
- Compute derived climate indices
- Aggregate to desired temporal resolution
- Handle missing values

### 4. Validation (validate.py)

Check data quality:
- Missing value analysis per variable
- Temporal coverage completeness (verify all accessions have expected number of months)
- Value range sanity checks (detect outliers beyond reasonable climate ranges)
- Generate detailed validation report saved to `output/validation_report.txt`

The validation report includes:
- Dataset shape and accession counts
- Missing value percentages
- Time range coverage (e.g., 1958-2024)
- Number of accessions with complete vs. incomplete temporal coverage
- Value ranges and out-of-range warnings

## Configuration

Edit `config.py` to customize:

```python
# Number of parallel workers
N_WORKERS = 4

# Output format
OUTPUT_FORMAT = "parquet"  # or "csv" or "both"

# Climate variables to extract
CLIMATE_VARIABLES = ["tmax", "tmin", "ppt", ...]

# Spatial tolerance for matching (degrees)
SPATIAL_TOLERANCE = 1/48
```

## Performance

**Original R script:**
- Hours to days for all 14 variables
- ~1,061 redundant lat/lon array loads per variable (14,854 total network requests)
- Sequential processing with repeated overhead

**New Python pipeline:**
- ~5-15 minutes for all 14 variables
- One-time spatial indexing (cached and reused)
- Efficient xarray operations with vectorized indexing
- Network streaming from THREDDS server with minimal overhead

**Typical runtime breakdown:**
- First run: ~10-15 minutes (includes spatial index building)
- Subsequent runs: ~5-10 minutes (uses cached spatial index)
- Single variable extraction: ~30-60 seconds per variable

## Next Steps for GWAS

1. **Load the data:**
```python
import pandas as pd
df = pd.read_parquet('output/climate_data_for_gwas.parquet')
```

2. **Compute mutual information** between climate variables:
```python
from sklearn.feature_selection import mutual_info_regression

# Example: mutual information between all pairs of climate variables
climate_cols = [col for col in df.columns if col != 'accession_id']
mi_matrix = compute_pairwise_mi(df[climate_cols])
```

3. **Run GWAS** with climate features as phenotypes:
- Use climate variables or derived indices as phenotypes
- Map genetic variants associated with climate adaptation
- Identify genes involved in stress response

## Data Source

**TerraClimate: Monthly Climate and Climatic Water Balance for Global Terrestrial Surfaces**

- **Resolution:** ~4.6 km (1/24 degree grid)
- **Temporal coverage:** 1958-present (updated monthly)
- **Access method:** Streamed via OpenDAP from THREDDS server (no download required)
- **Source:** http://thredds.northwestknowledge.net:8080/thredds/terraclimate_aggregated.html

The pipeline accesses data remotely via the THREDDS Data Server using the OpenDAP protocol through xarray. This means:
- No need to download large NetCDF files locally
- Automatic access to the most recent data
- Efficient extraction of only the spatial locations needed
- Requires stable internet connection during extraction

## Citation

If you use TerraClimate data, please cite:

Abatzoglou, J. T., Dobrowski, S. Z., Parks, S. A., & Hegewisch, K. C. (2018).
TerraClimate, a high-resolution global dataset of monthly climate and climatic water balance from 1958–2015.
Scientific Data, 5, 170191.

## Troubleshooting

**Network errors when accessing THREDDS:**
- The pipeline streams data from a remote server
- Temporary network issues may occur
- Failed extractions are logged to `output/extraction_failures.log`

**Missing spatial index cache:**
- First run will build the index (takes a few minutes)
- Subsequent runs will use the cached version
- Use `--rebuild-index` to force rebuild

**Memory issues:**
- If processing all variables at once uses too much memory
- Extract variables one at a time: `python main.py --variables tmax`
- Then combine results manually

**Wrong column names in input CSV:**
- The pipeline expects: `CS_number`, `latitude`, `longitude`
- If columns differ, the pipeline will auto-detect and use the first three columns
- You can also manually edit column name expectations in `spatial_index.py` line ~28

**Data contains NaN or missing coordinates:**
- Rows with missing latitude or longitude values are automatically dropped
- Check console output for warnings about how many accessions were loaded

## License

This pipeline is provided as-is for research purposes.
