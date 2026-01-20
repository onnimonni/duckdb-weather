#!/usr/bin/env python3
"""
GRIB to Parquet Converter for Weather Data

This script downloads GFS data from NOAA and converts it to Parquet format
with H3 spatial indexing for use with DuckDB.

Requirements:
    pip install herbie-data xarray cfgrib h3 pandas pyarrow

Usage:
    python grib_to_parquet.py --date 2024-01-15 --cycle 12 --output weather.parquet
"""

import argparse
from datetime import datetime
from pathlib import Path

def fetch_gfs_data(date: str, cycle: int, forecast_hour: int = 0):
    """
    Fetch GFS data using Herbie library.

    Args:
        date: Date in YYYY-MM-DD format
        cycle: Model cycle hour (0, 6, 12, or 18)
        forecast_hour: Forecast hour (0-384)

    Returns:
        xarray.Dataset with weather variables
    """
    try:
        from herbie import Herbie
    except ImportError:
        print("Install herbie-data: pip install herbie-data")
        raise

    H = Herbie(
        date,
        model="gfs",
        product="pgrb2.0p25",
        fxx=forecast_hour,
    )

    # Download surface variables
    ds = H.xarray(
        ":TMP:2 m above ground:|"
        ":DPT:2 m above ground:|"
        ":RH:2 m above ground:|"
        ":UGRD:10 m above ground:|"
        ":VGRD:10 m above ground:|"
        ":GUST:surface:|"
        ":PRES:surface:|"
        ":TCDC:entire atmosphere:|"
        ":PRATE:surface:|"
        ":VIS:surface:"
    )

    return ds


def normalize_longitude(ds):
    """Convert longitude from 0-360 to -180-180 range."""
    import xarray as xr

    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
    ds = ds.sortby('longitude')
    return ds


def add_h3_index(df, resolution: int = 5):
    """Add H3 spatial index to dataframe."""
    try:
        import h3
    except ImportError:
        print("Install h3: pip install h3")
        raise

    df['h3_index'] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(df['latitude'], df['longitude'])
    ]

    return df


def grib_to_parquet(
    date: str,
    cycle: int,
    forecast_hour: int,
    output_path: str,
    h3_resolution: int = 5,
):
    """
    Convert GRIB data to Parquet with H3 indexing.

    Args:
        date: Date in YYYY-MM-DD format
        cycle: Model cycle hour
        forecast_hour: Forecast hour
        output_path: Output Parquet file path
        h3_resolution: H3 resolution (5 = ~8.5km, 6 = ~3.2km)
    """
    import pandas as pd

    print(f"Fetching GFS data for {date} cycle {cycle:02d}Z f{forecast_hour:03d}...")
    ds = fetch_gfs_data(date, cycle, forecast_hour)

    print("Normalizing coordinates...")
    ds = normalize_longitude(ds)

    print("Converting to DataFrame...")
    df = ds.to_dataframe().reset_index()

    # Rename columns to standard names
    column_mapping = {
        't2m': 'temperature_k',
        'd2m': 'dewpoint_k',
        'r2': 'relative_humidity',
        'u10': 'wind_u_ms',
        'v10': 'wind_v_ms',
        'gust': 'wind_gust_ms',
        'sp': 'surface_pressure_pa',
        'tcc': 'cloud_cover',
        'prate': 'precip_rate_kg_m2_s',
        'vis': 'visibility_m',
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    print(f"Adding H3 index at resolution {h3_resolution}...")
    df = add_h3_index(df, h3_resolution)

    # Add metadata columns
    df['forecast_time'] = pd.Timestamp(date) + pd.Timedelta(hours=cycle + forecast_hour)
    df['run_time'] = pd.Timestamp(date) + pd.Timedelta(hours=cycle)
    df['forecast_hour'] = forecast_hour

    print(f"Writing to {output_path}...")
    df.to_parquet(output_path, index=False, compression='zstd')

    print(f"Done! Created {output_path}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert GRIB weather data to Parquet with H3 indexing"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--cycle",
        type=int,
        default=0,
        choices=[0, 6, 12, 18],
        help="Model cycle hour (default: 0)",
    )
    parser.add_argument(
        "--forecast-hour",
        type=int,
        default=0,
        help="Forecast hour 0-384 (default: 0)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="weather.parquet",
        help="Output Parquet file path",
    )
    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=5,
        choices=range(0, 16),
        help="H3 resolution (default: 5, ~8.5km)",
    )

    args = parser.parse_args()

    grib_to_parquet(
        date=args.date,
        cycle=args.cycle,
        forecast_hour=args.forecast_hour,
        output_path=args.output,
        h3_resolution=args.h3_resolution,
    )


if __name__ == "__main__":
    main()
