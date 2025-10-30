#!/usr/bin/env python3
"""
Main pipeline script for TerraClimate data extraction and processing.

This script orchestrates the complete workflow:
1. Build spatial index (one-time, cached)
2. Extract climate data for all variables
3. Validate data quality
4. Transform to desired format (for GWAS)
5. Save results

Usage:
    python main.py                          # Run full pipeline
    python main.py --variables tmax,tmin    # Extract only specific variables
    python main.py --aggregation summary    # Create summary statistics per accession
    python main.py --format csv             # Output as CSV instead of parquet
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

import config
from spatial_index import build_spatial_index
from extract import extract_all_variables
from validate import validate_extraction
from transform import prepare_for_gwas, save_dataframe


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract and process TerraClimate data for Arabidopsis accessions'
    )

    parser.add_argument(
        '--variables',
        type=str,
        default=None,
        help='Comma-separated list of variables to extract (default: all)'
    )

    parser.add_argument(
        '--aggregation',
        type=str,
        choices=['summary', 'annual', 'seasonal', 'monthly'],
        default='summary',
        help='Type of temporal aggregation (default: summary - one row per accession)'
    )

    parser.add_argument(
        '--format',
        type=str,
        choices=['parquet', 'csv', 'both'],
        default=config.OUTPUT_FORMAT,
        help=f'Output format (default: {config.OUTPUT_FORMAT})'
    )

    parser.add_argument(
        '--no-validation',
        action='store_true',
        help='Skip validation step'
    )

    parser.add_argument(
        '--rebuild-index',
        action='store_true',
        help='Rebuild spatial index from scratch (ignore cache)'
    )

    parser.add_argument(
        '--derived',
        action='store_true',
        default=True,
        help='Add derived climate indices (aridity, water balance, etc.)'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=None,
        help='Start year for data extraction (default: all available)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=None,
        help='End year for data extraction (default: all available)'
    )

    return parser.parse_args()


def print_banner():
    """Print a nice banner."""
    print("\n" + "="*70)
    print(" "*15 + "TerraClimate Data Extraction Pipeline")
    print(" "*20 + "for 1001 Arabidopsis Genomes")
    print("="*70 + "\n")


def print_step(step_num: int, step_name: str):
    """Print a step header."""
    print(f"\n{'='*70}")
    print(f"STEP {step_num}: {step_name}")
    print("="*70)


def main():
    """Main pipeline execution."""
    args = parse_args()

    print_banner()

    # Override config with command line arguments
    if args.start_year:
        config.START_YEAR = args.start_year
    if args.end_year:
        config.END_YEAR = args.end_year

    # Parse variables if specified
    variables = None
    if args.variables:
        variables = [v.strip() for v in args.variables.split(',')]
        print(f"Extracting variables: {', '.join(variables)}\n")
    else:
        print(f"Extracting all variables: {', '.join(config.CLIMATE_VARIABLES)}\n")

    start_time = datetime.now()

    try:
        # STEP 1: Build/Load Spatial Index
        print_step(1, "Building Spatial Index")
        spatial_index = build_spatial_index(use_cache=not args.rebuild_index)
        print(f"Spatial index ready for {len(spatial_index)} accessions")

        # STEP 2: Extract Climate Data
        print_step(2, "Extracting Climate Data")
        variable_data = extract_all_variables(
            variables=variables,
            use_cached_index=True
        )

        if not variable_data:
            print("\nERROR: No data extracted!")
            sys.exit(1)

        print(f"\nSuccessfully extracted {len(variable_data)} variables")

        # STEP 3: Validate Data
        if not args.no_validation:
            print_step(3, "Validating Data Quality")
            validation_report = validate_extraction(variable_data, save_report=True)
        else:
            print("\nSkipping validation (--no-validation flag)")

        # STEP 4: Transform Data
        print_step(4, "Transforming Data for Analysis")
        print(f"Aggregation type: {args.aggregation}")
        print(f"Adding derived indices: {args.derived}")

        final_df = prepare_for_gwas(
            variable_data,
            aggregation=args.aggregation,
            add_derived=args.derived
        )

        print(f"\nFinal dataset shape: {final_df.shape}")
        print(f"Columns: {len(final_df.columns)}")
        print(f"Rows: {len(final_df)}")

        # STEP 5: Save Results
        print_step(5, "Saving Results")

        # Determine output filename based on aggregation type
        if args.aggregation == 'summary':
            output_name = 'climate_data_for_gwas'
        else:
            output_name = f'climate_data_{args.aggregation}'

        save_dataframe(final_df, output_name, format=args.format)

        # Also save individual variable files if requested
        if args.aggregation == 'monthly':
            print("\nSaving individual variable files...")
            for var_name, df in variable_data.items():
                save_dataframe(df, f'{var_name}_monthly', format=args.format)

        # DONE
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "="*70)
        print("PIPELINE COMPLETE!")
        print("="*70)
        print(f"\nTotal runtime: {duration}")
        print(f"Output directory: {config.OUTPUT_DIR}")
        print(f"\nMain output file: {output_name}.{args.format}")
        print("\nNext steps:")
        print("  1. Load the data in your analysis environment")
        print("  2. Compute mutual information between climate variables")
        print("  3. Run GWAS with climate features as phenotypes")
        print("\nExample (Python):")
        print(f"  import pandas as pd")
        if args.format in ['parquet', 'both']:
            print(f"  df = pd.read_parquet('{config.OUTPUT_DIR / output_name}.parquet')")
        else:
            print(f"  df = pd.read_csv('{config.OUTPUT_DIR / output_name}.csv')")
        print()

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nERROR: Pipeline failed with exception:")
        print(f"{type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
