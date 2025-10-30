"""
Climate data extraction module.

Efficiently extracts time series data for each accession from TerraClimate NetCDF files.
Uses pre-computed spatial indices and parallel processing for speed.
"""
import numpy as np
import pandas as pd
import xarray as xr
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

import config
from spatial_index import build_spatial_index, get_time_coordinates, load_accessions


def extract_timeseries_for_accession(
    nc_dataset: xr.Dataset,
    accession_id: str,
    lat_idx: int,
    lon_idx: int,
    variable: str,
    time_index: pd.DatetimeIndex
) -> pd.DataFrame:
    """
    Extract time series for a single accession.

    Args:
        nc_dataset: Opened xarray Dataset
        accession_id: Accession identifier
        lat_idx: Latitude index in grid
        lon_idx: Longitude index in grid
        variable: Climate variable name
        time_index: DatetimeIndex for time dimension

    Returns:
        DataFrame with columns: accession_id, year, month, variable_value
    """
    # Extract the time series using xarray's efficient indexing
    # This is much faster than the old R approach
    data = nc_dataset[variable].isel(lat=lat_idx, lon=lon_idx).values

    # Create dataframe
    df = pd.DataFrame({
        'accession_id': accession_id,
        'year': time_index.year,
        'month': time_index.month,
        variable: data
    })

    return df


def extract_variable_sequential(
    variable: str,
    spatial_index: Dict[str, Tuple[int, int]],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    show_progress: bool = True
) -> pd.DataFrame:
    """
    Extract data for a single climate variable across all accessions.

    This version opens the NetCDF file once and processes all accessions sequentially.
    Much faster than the original R script which loaded lat/lon arrays repeatedly!

    Args:
        variable: Climate variable name
        spatial_index: Pre-computed spatial index mapping
        start_year: Optional start year to filter data
        end_year: Optional end year to filter data
        show_progress: Whether to show progress bar

    Returns:
        DataFrame with all accessions' data for this variable
    """
    url = config.TERRACLIMATE_URL_TEMPLATE.format(var=variable)

    print(f"\nExtracting {variable} data...")
    print(f"Opening NetCDF: {url}")

    # Open the NetCDF file ONCE (not 1,061 times!)
    with xr.open_dataset(url) as ds:
        # Get time coordinates
        time_index = get_time_coordinates(ds)

        # Filter by year if specified
        if start_year is not None or end_year is not None:
            time_mask = pd.Series(True, index=time_index)
            if start_year is not None:
                time_mask &= (time_index.year >= start_year)
            if end_year is not None:
                time_mask &= (time_index.year <= end_year)

            ds = ds.sel(time=time_mask)
            time_index = time_index[time_mask]

        print(f"Time range: {time_index[0]} to {time_index[-1]}")
        print(f"Total time points: {len(time_index)}")

        # Extract data for all accessions
        all_data = []

        iterator = spatial_index.items()
        if show_progress:
            iterator = tqdm(iterator, total=len(spatial_index),
                          desc=f"Extracting {variable}")

        for accession_id, (lat_idx, lon_idx) in iterator:
            df = extract_timeseries_for_accession(
                ds, accession_id, lat_idx, lon_idx, variable, time_index
            )
            all_data.append(df)

    # Combine all accessions
    result_df = pd.concat(all_data, ignore_index=True)

    print(f"Extracted {len(result_df)} records for {variable}")

    return result_df


def extract_variable_parallel_accessions(
    variable: str,
    spatial_index: Dict[str, Tuple[int, int]],
    n_workers: int = config.N_WORKERS,
    chunk_size: int = config.CHUNK_SIZE
) -> pd.DataFrame:
    """
    Extract data for a single climate variable with parallel processing.

    Note: This approach has overhead from repeatedly opening NetCDF connections.
    For most use cases, the sequential version will be faster since we're
    reading from a remote THREDDS server. Parallel is better if working with
    local NetCDF files.

    Args:
        variable: Climate variable name
        spatial_index: Pre-computed spatial index mapping
        n_workers: Number of parallel workers
        chunk_size: Number of accessions per chunk

    Returns:
        DataFrame with all accessions' data for this variable
    """
    # For now, just use sequential - it's simpler and likely faster for remote data
    # If you download NetCDF files locally, parallel processing would be beneficial
    return extract_variable_sequential(variable, spatial_index)


def extract_all_variables(
    variables: Optional[List[str]] = None,
    use_cached_index: bool = True,
    sequential: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Extract data for all climate variables.

    Args:
        variables: List of variable names (None = use all from config)
        use_cached_index: Whether to use cached spatial index
        sequential: Whether to use sequential extraction (recommended)

    Returns:
        Dictionary mapping variable name to DataFrame
    """
    if variables is None:
        variables = config.CLIMATE_VARIABLES

    # Build/load spatial index ONCE for all variables
    print("Loading spatial index...")
    spatial_index = build_spatial_index(use_cache=use_cached_index)

    # Extract each variable
    results = {}

    for var in variables:
        try:
            if sequential:
                df = extract_variable_sequential(
                    var, spatial_index,
                    start_year=config.START_YEAR,
                    end_year=config.END_YEAR
                )
            else:
                df = extract_variable_parallel_accessions(var, spatial_index)

            results[var] = df
            print(f"Successfully extracted {var}")

        except Exception as e:
            print(f"Error extracting {var}: {e}")
            if config.LOG_EXTRACTION_FAILURES:
                with open(config.OUTPUT_DIR / "extraction_failures.log", "a") as f:
                    f.write(f"{var}: {e}\n")

    return results


def extract_vectorized_all_locations(
    variable: str,
    spatial_index: Dict[str, Tuple[int, int]]
) -> pd.DataFrame:
    """
    ADVANCED: Extract all locations in a single vectorized operation.

    This is the theoretically fastest approach, but requires more memory
    and may not work well with large grids or remote data sources.

    Args:
        variable: Climate variable name
        spatial_index: Pre-computed spatial index mapping

    Returns:
        DataFrame with all accessions' data
    """
    url = config.TERRACLIMATE_URL_TEMPLATE.format(var=variable)

    print(f"\nVectorized extraction for {variable}...")

    with xr.open_dataset(url) as ds:
        time_index = get_time_coordinates(ds)

        # Prepare arrays of all lat/lon indices
        accession_ids = list(spatial_index.keys())
        lat_indices = [spatial_index[acc][0] for acc in accession_ids]
        lon_indices = [spatial_index[acc][1] for acc in accession_ids]

        # Extract all at once using fancy indexing
        # This reads a chunk of the NetCDF file covering all locations
        data = ds[variable].isel(
            lat=xr.DataArray(lat_indices, dims='accession'),
            lon=xr.DataArray(lon_indices, dims='accession')
        ).values  # Shape: (n_accessions, n_time_points)

        # Reshape to long format
        n_accessions = len(accession_ids)
        n_times = len(time_index)

        # Create arrays for the dataframe
        accession_array = np.repeat(accession_ids, n_times)
        time_array = np.tile(time_index, n_accessions)
        data_array = data.flatten()

        # Build dataframe
        df = pd.DataFrame({
            'accession_id': accession_array,
            'year': time_array.year,
            'month': time_array.month,
            variable: data_array
        })

    print(f"Extracted {len(df)} records for {variable}")

    return df


if __name__ == "__main__":
    # Test extraction for a single variable
    print("Testing climate data extraction...")

    # Build spatial index
    spatial_index = build_spatial_index(use_cache=True)

    # Test with just one variable
    test_var = "tmax"
    print(f"\nTesting extraction for {test_var}...")

    df = extract_variable_sequential(test_var, spatial_index)

    print(f"\nResults shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head(10))
    print(f"\nData summary:")
    print(df.describe())
