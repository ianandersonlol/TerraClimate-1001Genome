"""
Spatial indexing module for matching accession coordinates to TerraClimate grid cells.

This module pre-computes the grid indices for all accessions, avoiding the
expensive repeated lat/lon array loading that slowed down the original R script.
"""
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from typing import Dict, Tuple, Optional

import config


def load_accessions() -> pd.DataFrame:
    """
    Load the accession data with coordinates.

    Returns:
        DataFrame with columns: CS_number (or similar ID), latitude, longitude
    """
    df = pd.read_csv(config.ACCESSIONS_FILE)

    # Assuming column structure based on the repository exploration
    # Adjust column names if needed
    expected_cols = ['CS_number', 'latitude', 'longitude']

    if not all(col in df.columns for col in expected_cols):
        # Try to infer column names
        print(f"Warning: Expected columns {expected_cols}")
        print(f"Found columns: {list(df.columns)}")

        # Assume first column is ID, second is lat, third is lon
        if len(df.columns) >= 3:
            df.columns = expected_cols[:len(df.columns)]
            print(f"Renamed columns to: {list(df.columns)}")

    # Clean data: remove any rows with missing coordinates
    df = df.dropna(subset=['latitude', 'longitude'])

    print(f"Loaded {len(df)} accessions")
    return df


def get_grid_coordinates(nc_file: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extract latitude and longitude arrays from a NetCDF dataset.

    Args:
        nc_file: Opened xarray Dataset

    Returns:
        Tuple of (lat_array, lon_array)
    """
    lat = nc_file['lat'].values
    lon = nc_file['lon'].values
    return lat, lon


def find_nearest_grid_cell(lat: float, lon: float,
                           grid_lats: np.ndarray,
                           grid_lons: np.ndarray,
                           tolerance: float = config.SPATIAL_TOLERANCE) -> Optional[Tuple[int, int]]:
    """
    Find the nearest grid cell for a given lat/lon coordinate.

    Args:
        lat: Latitude of point
        lon: Longitude of point
        grid_lats: Array of grid latitude values
        grid_lons: Array of grid longitude values
        tolerance: Maximum distance (in degrees) to consider a match

    Returns:
        Tuple of (lat_index, lon_index) or None if no match within tolerance
    """
    # Find indices where lat/lon are within tolerance
    lat_matches = np.abs(grid_lats - lat) < tolerance
    lon_matches = np.abs(grid_lons - lon) < tolerance

    lat_indices = np.where(lat_matches)[0]
    lon_indices = np.where(lon_matches)[0]

    if len(lat_indices) == 0 or len(lon_indices) == 0:
        return None

    # If multiple matches, find the closest one
    if len(lat_indices) > 1 or len(lon_indices) > 1:
        lat_idx = lat_indices[np.argmin(np.abs(grid_lats[lat_indices] - lat))]
        lon_idx = lon_indices[np.argmin(np.abs(grid_lons[lon_indices] - lon))]
    else:
        lat_idx = lat_indices[0]
        lon_idx = lon_indices[0]

    return (lat_idx, lon_idx)


def build_spatial_index(use_cache: bool = True) -> Dict[str, Tuple[int, int]]:
    """
    Build a spatial index mapping each accession to its grid cell indices.

    This is the KEY optimization - we do this once instead of 1,061 times per variable!

    Args:
        use_cache: Whether to load/save cached index

    Returns:
        Dictionary mapping accession_id -> (lat_index, lon_index)
    """
    cache_file = config.CACHE_DIR / "spatial_index.pkl"

    # Try to load from cache
    if use_cache and cache_file.exists():
        print(f"Loading spatial index from cache: {cache_file}")
        with open(cache_file, 'rb') as f:
            spatial_index = pickle.load(f)
        print(f"Loaded spatial index for {len(spatial_index)} accessions")
        return spatial_index

    print("Building spatial index...")

    # Load accession coordinates
    accessions_df = load_accessions()

    # Open a sample NetCDF file to get the grid coordinates
    # Use any variable - they all have the same grid
    sample_var = config.CLIMATE_VARIABLES[0]
    sample_url = config.TERRACLIMATE_URL_TEMPLATE.format(var=sample_var)

    print(f"Opening sample NetCDF file: {sample_url}")
    with xr.open_dataset(sample_url) as ds:
        grid_lats, grid_lons = get_grid_coordinates(ds)

    print(f"Grid dimensions: {len(grid_lats)} lats x {len(grid_lons)} lons")

    # Build the index
    spatial_index = {}
    failed_accessions = []

    for _, row in accessions_df.iterrows():
        accession_id = row['CS_number']
        lat = row['latitude']
        lon = row['longitude']

        indices = find_nearest_grid_cell(lat, lon, grid_lats, grid_lons)

        if indices is not None:
            spatial_index[accession_id] = indices
        else:
            failed_accessions.append(accession_id)
            print(f"Warning: No grid cell found for {accession_id} at ({lat}, {lon})")

    print(f"Successfully indexed {len(spatial_index)} accessions")

    if failed_accessions:
        print(f"Failed to index {len(failed_accessions)} accessions:")
        print(failed_accessions)

    # Save to cache
    if use_cache:
        print(f"Saving spatial index to cache: {cache_file}")
        with open(cache_file, 'wb') as f:
            pickle.dump(spatial_index, f)

    return spatial_index


def get_time_coordinates(nc_file: xr.Dataset) -> pd.DatetimeIndex:
    """
    Extract time coordinates from NetCDF file and convert to datetime.

    Args:
        nc_file: Opened xarray Dataset

    Returns:
        DatetimeIndex of time points
    """
    time = pd.to_datetime(nc_file['time'].values)
    return time


if __name__ == "__main__":
    # Test the spatial indexing
    print("Testing spatial index building...")
    spatial_index = build_spatial_index(use_cache=False)
    print(f"\nSuccessfully built spatial index for {len(spatial_index)} accessions")
    print(f"\nSample entries:")
    for i, (acc_id, indices) in enumerate(list(spatial_index.items())[:5]):
        print(f"  {acc_id}: lat_idx={indices[0]}, lon_idx={indices[1]}")
