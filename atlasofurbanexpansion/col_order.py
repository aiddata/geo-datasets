
import os
import geopandas as gpd


base_dir = "/sciclone/aiddata10/REU/geo/data/boundaries/atlasofurbanexpansion/2016"

levels = [
    "studyArea",
    "urban_edge_t3",
    "urban_edge_t2",
    "urban_edge_t1"
]

def update_col_order(path):
    gdf = gpd.read_file(path)
    fixed_cols = ["gqid", "asdf_id", "City Name", "Region", "Country", "id"]
    var_cols = [i for i in gdf.columns if i not in fixed_cols]
    col_order = fixed_cols + var_cols
    gdf_new = gdf[col_order]
    gdf_new.to_file(path, driver='GeoJSON')

for level in levels:
    print "Updating level: {}".format(level)
    level_path = os.path.join(base_dir, level, "{}.geojson".format(level))
    update_col_order(level_path)
    simplified_level_path = os.path.join(base_dir, level, "simplified.geojson".format(level))
    update_col_order(simplified_level_path)
