"""
Data transformation module.

Handles reshaping and formatting climate data for downstream analysis.
For GWAS with mutual information, we want a wide format with all climate
variables as features.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path

import config


def merge_variables_wide_format(
    variable_data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Merge multiple climate variables into wide format.

    For GWAS analysis, this creates a matrix where:
    - Each row is an accession-timepoint
    - Each column is a climate variable
    - Easy to compute mutual information between variables

    Args:
        variable_data: Dictionary mapping variable name to DataFrame

    Returns:
        Wide-format DataFrame with all variables
    """
    print("\nMerging variables into wide format...")

    # Start with the first variable
    first_var = list(variable_data.keys())[0]
    result_df = variable_data[first_var].copy()

    # Merge each additional variable
    for var in list(variable_data.keys())[1:]:
        df = variable_data[var]

        # Merge on accession_id, year, month
        result_df = result_df.merge(
            df[['accession_id', 'year', 'month', var]],
            on=['accession_id', 'year', 'month'],
            how='outer'
        )

        print(f"  Merged {var}, shape: {result_df.shape}")

    print(f"\nFinal merged shape: {result_df.shape}")

    return result_df


def create_temporal_features(
    df: pd.DataFrame,
    add_season: bool = True,
    add_quarter: bool = True
) -> pd.DataFrame:
    """
    Add temporal features that might be useful for analysis.

    Args:
        df: Input DataFrame with year and month columns
        add_season: Whether to add meteorological season
        add_quarter: Whether to add quarter of year

    Returns:
        DataFrame with additional temporal columns
    """
    df = df.copy()

    if add_season:
        # Meteorological seasons
        season_map = {
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        }
        df['season'] = df['month'].map(season_map)

    if add_quarter:
        df['quarter'] = pd.to_datetime(
            df[['year', 'month']].assign(day=1)
        ).dt.quarter

    return df


def compute_temporal_aggregates(
    df: pd.DataFrame,
    variables: List[str],
    aggregation_period: str = 'year'
) -> pd.DataFrame:
    """
    Compute temporal aggregates (e.g., annual means, seasonal means).

    For GWAS, you might want to use aggregated climate features rather than
    monthly values. This reduces dimensionality and captures long-term patterns.

    Args:
        df: Input DataFrame in wide format
        variables: List of climate variable columns to aggregate
        aggregation_period: 'year', 'season', or 'quarter'

    Returns:
        Aggregated DataFrame
    """
    print(f"\nComputing {aggregation_period} aggregates...")

    group_cols = ['accession_id']

    if aggregation_period == 'year':
        group_cols.append('year')
    elif aggregation_period == 'season':
        df = create_temporal_features(df, add_season=True, add_quarter=False)
        group_cols.extend(['year', 'season'])
    elif aggregation_period == 'quarter':
        df = create_temporal_features(df, add_season=False, add_quarter=True)
        group_cols.extend(['year', 'quarter'])
    else:
        raise ValueError(f"Unknown aggregation period: {aggregation_period}")

    # Aggregate
    agg_dict = {var: ['mean', 'std', 'min', 'max'] for var in variables}

    result_df = df.groupby(group_cols).agg(agg_dict).reset_index()

    # Flatten column names
    result_df.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                        for col in result_df.columns.values]

    print(f"Aggregated shape: {result_df.shape}")

    return result_df


def compute_climate_summaries(
    df: pd.DataFrame,
    variables: List[str]
) -> pd.DataFrame:
    """
    Compute overall climate summaries per accession.

    This creates one row per accession with summary statistics across the
    entire time period. Useful for GWAS when you want to associate genetic
    variants with overall climate conditions.

    Args:
        df: Input DataFrame in wide format
        variables: List of climate variable columns

    Returns:
        DataFrame with one row per accession
    """
    print("\nComputing per-accession climate summaries...")

    summary_stats = ['mean', 'std', 'min', 'max', 'median']
    agg_dict = {var: summary_stats for var in variables}

    result_df = df.groupby('accession_id').agg(agg_dict).reset_index()

    # Flatten column names
    result_df.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                        for col in result_df.columns.values]

    print(f"Summary shape: {result_df.shape} (one row per accession)")

    return result_df


def add_derived_climate_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived climate indices that might be biologically meaningful.

    Examples:
    - Aridity index: PET/PPT
    - Water balance: PPT - AET
    - Temperature range: tmax - tmin (if both available)

    Args:
        df: Input DataFrame with climate variables

    Returns:
        DataFrame with additional derived indices
    """
    df = df.copy()

    # Aridity index (higher = more arid)
    if 'pet' in df.columns and 'ppt' in df.columns:
        # Avoid division by zero
        df['aridity_index'] = df['pet'] / (df['ppt'] + 0.1)

    # Water balance
    if 'ppt' in df.columns and 'aet' in df.columns:
        df['water_balance'] = df['ppt'] - df['aet']

    # Temperature range
    if 'tmax' in df.columns and 'tmin' in df.columns:
        df['temp_range'] = df['tmax'] - df['tmin']

    # Moisture availability
    if 'soil' in df.columns and 'def' in df.columns:
        df['moisture_availability'] = df['soil'] - df['def']

    return df


def prepare_for_gwas(
    variable_data: Dict[str, pd.DataFrame],
    aggregation: str = 'summary',
    add_derived: bool = True
) -> pd.DataFrame:
    """
    Prepare climate data in the optimal format for GWAS analysis.

    Args:
        variable_data: Dictionary mapping variable names to DataFrames
        aggregation: Type of aggregation
                    'summary' = one row per accession (recommended for GWAS)
                    'annual' = one row per accession-year
                    'seasonal' = one row per accession-season
                    'monthly' = keep monthly resolution
        add_derived: Whether to add derived climate indices

    Returns:
        DataFrame ready for GWAS analysis
    """
    # Merge into wide format
    df = merge_variables_wide_format(variable_data)

    # Get list of climate variables
    climate_vars = [col for col in df.columns
                   if col not in ['accession_id', 'year', 'month', 'season', 'quarter']]

    # Add derived indices
    if add_derived:
        print("\nAdding derived climate indices...")
        df = add_derived_climate_indices(df)
        # Update climate vars to include derived indices
        climate_vars = [col for col in df.columns
                       if col not in ['accession_id', 'year', 'month', 'season', 'quarter']]

    # Apply aggregation
    if aggregation == 'summary':
        df = compute_climate_summaries(df, climate_vars)
    elif aggregation == 'annual':
        df = compute_temporal_aggregates(df, climate_vars, 'year')
    elif aggregation == 'seasonal':
        df = compute_temporal_aggregates(df, climate_vars, 'season')
    elif aggregation == 'monthly':
        # Keep as-is
        pass
    else:
        raise ValueError(f"Unknown aggregation type: {aggregation}")

    return df


def save_dataframe(
    df: pd.DataFrame,
    name: str,
    output_dir: Path = config.OUTPUT_DIR,
    format: str = config.OUTPUT_FORMAT
) -> None:
    """
    Save DataFrame in the specified format(s).

    Args:
        df: DataFrame to save
        name: Base filename (without extension)
        output_dir: Output directory
        format: 'parquet', 'csv', or 'both'
    """
    output_dir.mkdir(exist_ok=True)

    if format in ['parquet', 'both']:
        parquet_path = output_dir / f"{name}.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"Saved: {parquet_path}")

    if format in ['csv', 'both']:
        csv_path = output_dir / f"{name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}")


if __name__ == "__main__":
    # This would be used after extraction
    print("Transform module - use with extract.py output")
    print("\nExample usage:")
    print("  from extract import extract_all_variables")
    print("  from transform import prepare_for_gwas, save_dataframe")
    print("")
    print("  # Extract data")
    print("  var_data = extract_all_variables()")
    print("")
    print("  # Transform for GWAS")
    print("  gwas_df = prepare_for_gwas(var_data, aggregation='summary')")
    print("")
    print("  # Save")
    print("  save_dataframe(gwas_df, 'climate_data_for_gwas')")
