"""
Configuration for TerraClimate data extraction pipeline.
"""
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = BASE_DIR / "cache"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Input data
ACCESSIONS_FILE = BASE_DIR / "availableplantsforclimate.csv"

# TerraClimate variables
# See: http://thredds.northwestknowledge.net:8080/thredds/terraclimate_aggregated.html
CLIMATE_VARIABLES = [
    "aet",   # Actual Evapotranspiration
    "def",   # Climatic Water Deficit
    "pet",   # Potential Evapotranspiration
    "ppt",   # Precipitation
    "q",     # Runoff
    "soil",  # Soil Moisture
    "srad",  # Solar Radiation
    "swe",   # Snow Water Equivalent
    "tmax",  # Maximum Temperature
    "tmin",  # Minimum Temperature
    "vap",   # Vapor Pressure
    "ws",    # Wind Speed
    "vpd",   # Vapor Pressure Deficit
    "PDSI"   # Palmer Drought Severity Index
]

# TerraClimate THREDDS server settings
THREDDS_BASE_URL = "http://thredds.northwestknowledge.net:8080/thredds/dodsC"
TERRACLIMATE_URL_TEMPLATE = f"{THREDDS_BASE_URL}/agg_terraclimate_{{var}}_1958_CurrentYear_GLOBE.nc"

# Spatial matching tolerance (degrees)
# Original script used 1/48 degree (~2.3 km)
SPATIAL_TOLERANCE = 1/48

# Processing settings
N_WORKERS = 4  # Number of parallel workers for extraction
CHUNK_SIZE = 50  # Number of accessions to process per chunk

# Output format settings
OUTPUT_FORMAT = "parquet"  # Options: "parquet", "csv", "both"
# For GWAS with mutual information, a wide format will be more efficient:
# One row per accession-timepoint, all climate variables as columns
USE_WIDE_FORMAT = True

# Time range (None = use all available data)
START_YEAR = None  # e.g., 1958
END_YEAR = None    # e.g., 2024

# Validation settings
CHECK_MISSING_VALUES = True
LOG_EXTRACTION_FAILURES = True

# Cache settings
CACHE_SPATIAL_INDEX = True  # Cache the lat/lon index mapping
CACHE_NETCDF_HANDLE = False  # Whether to cache opened NetCDF file handles
