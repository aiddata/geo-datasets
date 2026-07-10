"""
US Census Bureau TIGER/Line Shapefiles

https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

County
https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip
"""
import argparse
from ast import arg
from datetime import datetime
from pathlib import Path
import requests


import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point, Polygon, MultiPolygon

# parse args
arg_parser = argparse.ArgumentParser(description="Download TIGER/Line shapefiles")
arg_parser.add_argument("--year", help="Year of the TIGER/Line shapefiles to download", default="2025")
arg_parser.add_argument("--dataset", help="Dataset to download (e.g., COUNTY, STATE)", default="COUNTY")
arg_parser.add_argument("--raw-dir", help="Directory for downloaded shapefiles")
arg_parser.add_argument("--output-dir", help="Directory for processed shapefiles")
arg_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files", default=False)
arg_parser.add_argument("--overwrite-download", action="store_true", help="Overwrite existing files", default=False)
arg_parser.add_argument("--overwrite-process", action="store_true", help="Overwrite existing files", default=True)

args = arg_parser.parse_args()

# download TIGER/Line shapefiles
def download_tiger_shapefiles(download_path, overwrite=False):

    if not overwrite and download_path.exists():
        print(f"File {download_path} already exists. Skipping...")
        return

    print(f"Downloading {url} to {download_path}...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses

    with open(download_path, 'wb') as f:
        f.write(response.content)

    print(f"Downloaded {download_path}")

# open, check features, and save to GeoPackage
def process_shapefile(zip_path, output_path, overwrite=False):

    if not overwrite and output_path.exists():
        print(f"GeoPackage {output_path} already exists. Skipping...")
        return

    gdf = gpd.read_file(zip_path)
    print(f"Loaded {len(gdf)} features from {zip_path}")

    # Check for valid geometries
    assert gdf.is_valid.all(), "Some geometries are invalid. Consider filtering them out."

    # fill null values in each column based on data type it should be without nulls
    for column in gdf.columns:
        if column == "geometry":
            continue
        elif pd.api.types.is_numeric_dtype(gdf[column]):
            gdf[column] = gdf[column].fillna(0)
        else:
            gdf[column] = gdf[column].fillna("")


    # Save to GeoPackage
    gdf.to_file(output_path, driver="GPKG")
    print(f"Saved to {output_path}")


# create ingest json
def create_ingest_json(output_path, year, dataset):

    defaults={
        "name": f"TIGER_{year}_{dataset}",
        "short_name": f"TIGER_{year}_{dataset}",
        "file_mask": "None",
        "active": 1,
        "public": 1,
        "path": str(Path("/data/TIGER") / output_path.name),
        "file_extension": ".gpkg",
        "title": f"US CENSUS TIGER/Line {year} {dataset}",
        "description": f"US CENSUS TIGER/Line shapefiles for {dataset} in {year}",
        "details": "",
        "tags": ["TIGER", "Census", dataset, f"{year}", "USA"],
        "citation": f"U.S. Census Bureau. ({year}). {year} TIGER/Line Shapefiles: {dataset} (machine readable data files). U.S. Department of Commerce. census.gov (Accessed {datetime.now().strftime('%Y-%m-%d')}).",
        "source_name": "United States Census Bureau",
        "source_url": "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
        "other": {},
        "ingest_src": "geoquery_automated",
        "is_global": False,
        "spatial_extent": None,
        "group_name": "TIGER",
        "group_title": "US TIGER/Line Shapefiles",
        "group_class": "parent",
        "group_level": "2",
    }

    ingest_json_path = Path(output_path).parent / f"ingest_{year}_{dataset.lower()}.json"
    with open(ingest_json_path, 'w') as f:
        import json
        json.dump(defaults, f, indent=4)

    print(f"Ingest JSON created at {ingest_json_path}")

if __name__ == "__main__":
    if args.raw_dir is None or args.output_dir is None:
        raise ValueError("Please specify both raw and output directories using --raw-dir and --output-dir")

    if args.overwrite:
        args.overwrite_download = True
        args.overwrite_process = True

    url = f"https://www2.census.gov/geo/tiger/TIGER{args.year}/{args.dataset}/tl_{args.year}_us_{args.dataset.lower()}.zip"
    download_path = Path(args.raw_dir) / "TIGER" / f"tl_{args.year}_us_{args.dataset.lower()}.zip"
    download_path.parent.mkdir(parents=True, exist_ok=True)

    download_tiger_shapefiles(download_path, args.overwrite_download)

    output_dir = Path(args.output_dir) / "TIGER"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / f"{download_path.stem}.gpkg"

    process_shapefile(download_path, output_path, args.overwrite_process)

    create_ingest_json(output_path, args.year, args.dataset)
