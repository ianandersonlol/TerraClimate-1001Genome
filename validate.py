"""
Data validation module.

Checks data quality and generates reports on extraction completeness.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from pathlib import Path

import config


def check_missing_values(df: pd.DataFrame, variable_name: str = None) -> Dict:
    """
    Check for missing values in the dataset.

    Args:
        df: DataFrame to check
        variable_name: Optional name for reporting

    Returns:
        Dictionary with missing value statistics
    """
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isna().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100

    missing_by_column = df.isna().sum()
    missing_by_column = missing_by_column[missing_by_column > 0]

    report = {
        'variable': variable_name,
        'total_cells': total_cells,
        'missing_cells': missing_cells,
        'missing_percentage': missing_pct,
        'columns_with_missing': missing_by_column.to_dict()
    }

    return report


def check_temporal_coverage(
    df: pd.DataFrame,
    expected_start_year: int = 1958
) -> Dict:
    """
    Verify that all accessions have complete temporal coverage.

    Args:
        df: DataFrame with year and month columns
        expected_start_year: Expected start year of data

    Returns:
        Dictionary with temporal coverage statistics
    """
    # Count unique year-month combinations per accession
    coverage = df.groupby('accession_id').apply(
        lambda x: len(x[['year', 'month']].drop_duplicates())
    )

    # Get the expected number of months
    if 'year' in df.columns:
        min_year = df['year'].min()
        max_year = df['year'].max()
        expected_months = (max_year - min_year + 1) * 12
    else:
        expected_months = None

    # Find accessions with incomplete coverage
    if expected_months:
        incomplete = coverage[coverage < expected_months]
    else:
        # Use the most common count as expected
        expected_months = coverage.mode()[0] if len(coverage) > 0 else None
        incomplete = coverage[coverage < expected_months] if expected_months else pd.Series()

    report = {
        'min_year': int(df['year'].min()) if 'year' in df.columns else None,
        'max_year': int(df['year'].max()) if 'year' in df.columns else None,
        'expected_months': int(expected_months) if expected_months else None,
        'accessions_with_complete_coverage': int((coverage == expected_months).sum()) if expected_months else None,
        'accessions_with_incomplete_coverage': int(len(incomplete)),
        'incomplete_accessions': incomplete.to_dict()
    }

    return report


def check_value_ranges(df: pd.DataFrame, variable_name: str) -> Dict:
    """
    Check if values are within reasonable ranges for climate variables.

    Args:
        df: DataFrame with climate data
        variable_name: Name of the climate variable

    Returns:
        Dictionary with value range statistics
    """
    # Define reasonable ranges for climate variables
    # These are rough global extremes - adjust if needed
    ranges = {
        'tmax': (-50, 60),      # Temperature in Celsius
        'tmin': (-80, 50),
        'ppt': (0, 2000),       # Precipitation in mm/month
        'aet': (0, 500),        # Evapotranspiration in mm/month
        'pet': (0, 500),
        'def': (0, 500),        # Water deficit in mm/month
        'soil': (0, 1000),      # Soil moisture in mm
        'q': (0, 1000),         # Runoff in mm/month
        'swe': (0, 10000),      # Snow water equivalent in mm
        'srad': (0, 500),       # Solar radiation in W/m2
        'ws': (0, 50),          # Wind speed in m/s
        'vap': (0, 100),        # Vapor pressure in kPa
        'vpd': (0, 10),         # Vapor pressure deficit in kPa
        'PDSI': (-10, 10)       # Palmer Drought Severity Index
    }

    if variable_name not in ranges:
        return {'variable': variable_name, 'warning': 'No range defined for this variable'}

    min_val, max_val = ranges[variable_name]

    if variable_name in df.columns:
        data = df[variable_name]

        out_of_range = (data < min_val) | (data > max_val)
        n_out_of_range = out_of_range.sum()

        report = {
            'variable': variable_name,
            'expected_range': (min_val, max_val),
            'actual_range': (float(data.min()), float(data.max())),
            'values_out_of_range': int(n_out_of_range),
            'percentage_out_of_range': float((n_out_of_range / len(data)) * 100)
        }
    else:
        report = {
            'variable': variable_name,
            'warning': f'Variable {variable_name} not found in DataFrame'
        }

    return report


def validate_extraction(
    variable_data: Dict[str, pd.DataFrame],
    save_report: bool = True
) -> Dict:
    """
    Comprehensive validation of extracted climate data.

    Args:
        variable_data: Dictionary mapping variable names to DataFrames
        save_report: Whether to save validation report to file

    Returns:
        Dictionary containing all validation results
    """
    print("\n" + "="*60)
    print("VALIDATION REPORT")
    print("="*60)

    full_report = {
        'n_variables': len(variable_data),
        'variables': list(variable_data.keys()),
        'per_variable_reports': {}
    }

    for var_name, df in variable_data.items():
        print(f"\n{var_name}:")
        print("-" * 40)

        var_report = {}

        # Basic shape info
        print(f"  Shape: {df.shape}")
        var_report['shape'] = df.shape

        # Number of accessions
        n_accessions = df['accession_id'].nunique() if 'accession_id' in df.columns else 0
        print(f"  Accessions: {n_accessions}")
        var_report['n_accessions'] = n_accessions

        # Missing values
        if config.CHECK_MISSING_VALUES:
            missing_report = check_missing_values(df, var_name)
            print(f"  Missing values: {missing_report['missing_cells']} "
                  f"({missing_report['missing_percentage']:.2f}%)")
            var_report['missing_values'] = missing_report

        # Temporal coverage
        if 'year' in df.columns and 'month' in df.columns:
            temporal_report = check_temporal_coverage(df)
            print(f"  Time range: {temporal_report['min_year']}-{temporal_report['max_year']}")
            print(f"  Complete coverage: {temporal_report['accessions_with_complete_coverage']} accessions")
            if temporal_report['accessions_with_incomplete_coverage'] > 0:
                print(f"  WARNING: {temporal_report['accessions_with_incomplete_coverage']} "
                      "accessions with incomplete coverage")
            var_report['temporal_coverage'] = temporal_report

        # Value ranges
        range_report = check_value_ranges(df, var_name)
        if 'actual_range' in range_report:
            print(f"  Value range: {range_report['actual_range']}")
            if range_report['values_out_of_range'] > 0:
                print(f"  WARNING: {range_report['values_out_of_range']} values "
                      f"({range_report['percentage_out_of_range']:.2f}%) out of expected range")
        var_report['value_ranges'] = range_report

        full_report['per_variable_reports'][var_name] = var_report

    print("\n" + "="*60)

    # Save report if requested
    if save_report:
        report_path = config.OUTPUT_DIR / "validation_report.txt"
        with open(report_path, 'w') as f:
            f.write(format_validation_report(full_report))
        print(f"\nValidation report saved to: {report_path}")

    return full_report


def format_validation_report(report: Dict) -> str:
    """
    Format validation report as readable text.

    Args:
        report: Validation report dictionary

    Returns:
        Formatted string report
    """
    lines = []
    lines.append("="*60)
    lines.append("CLIMATE DATA VALIDATION REPORT")
    lines.append("="*60)
    lines.append(f"\nNumber of variables: {report['n_variables']}")
    lines.append(f"Variables: {', '.join(report['variables'])}\n")

    for var_name, var_report in report['per_variable_reports'].items():
        lines.append(f"\n{var_name}:")
        lines.append("-" * 40)

        if 'shape' in var_report:
            lines.append(f"  Shape: {var_report['shape']}")

        if 'n_accessions' in var_report:
            lines.append(f"  Accessions: {var_report['n_accessions']}")

        if 'missing_values' in var_report:
            mv = var_report['missing_values']
            lines.append(f"  Missing: {mv['missing_cells']} cells ({mv['missing_percentage']:.2f}%)")

        if 'temporal_coverage' in var_report:
            tc = var_report['temporal_coverage']
            lines.append(f"  Time range: {tc['min_year']}-{tc['max_year']}")
            lines.append(f"  Complete coverage: {tc['accessions_with_complete_coverage']} accessions")

        if 'value_ranges' in var_report:
            vr = var_report['value_ranges']
            if 'actual_range' in vr:
                lines.append(f"  Value range: {vr['actual_range']}")
                if vr['values_out_of_range'] > 0:
                    lines.append(f"  Out of range: {vr['values_out_of_range']} values")

    lines.append("\n" + "="*60)

    return "\n".join(lines)


if __name__ == "__main__":
    print("Validation module - use after extraction")
    print("\nExample usage:")
    print("  from extract import extract_all_variables")
    print("  from validate import validate_extraction")
    print("")
    print("  var_data = extract_all_variables()")
    print("  report = validate_extraction(var_data)")
