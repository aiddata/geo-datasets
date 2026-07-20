"""
Prepare GCDF v3.0.1 as a dataset for ingestion into GeoQuery.

Download the data from the GCDF GitHub repository, rename columns, and save as a GeoPackage. Then, update the filter_ingest.json file to include the columns for filtering in GeoQuery and their corresponding filter options.
"""
import json
from pathlib import Path

import geopandas as gpd

url = "https://github.com/aiddata/gcdf-geospatial-data/releases/download/v3.0.1/all_combined_global.gpkg.zip"

raw_gdf = gpd.read_file(url, layer="all_combined_global")

rename_dict = {
    "Amount.(Constant.USD.2021)": "Commitment Value",
    "Sector.Name": "Sector Name",
    "Status": "Project Status",
    "Commitment.Year": "Commitment Year",
    "Completion.Year": "Completion Year",
    "geometry": "geometry"
}

gdf = raw_gdf.rename(columns=rename_dict)

gdf = gdf[rename_dict.values()].copy()

gdf.to_file(Path("data/gcdf_v301_dynamic/gcdf_v301_dynamic.gpkg"), layer="gcdf_v301_dynamic", driver="GPKG")

# open filter_ingest.json and update the options>filters section to include the columns and values in the gdf
with open(Path("data/gcdf_v301_dynamic/filter_ingest.json"), "r") as f:
    filter_ingest = json.load(f)


filter_ingest["other"]["filters"] = {
    "Commitment Year": {
        "type": "range",
        "min": int(gdf["Commitment Year"].min()),
        "max": int(gdf["Commitment Year"].max())
    },
    "Completion Year": {
        "type": "range",
        "min": int(gdf["Completion Year"].min()),
        "max": int(gdf["Completion Year"].max())
    },
    "Project Status": {
        "type": "categorical",
        "categories": gdf["Project Status"].unique().tolist()
    },
    "Sector Name": {
        "type": "categorical",
        "categories": gdf["Sector Name"].unique().tolist()
    }
}
with open(Path("data/gcdf_v301_dynamic/filter_ingest.json"), "w") as f:
    json.dump(filter_ingest, f, indent=4)
