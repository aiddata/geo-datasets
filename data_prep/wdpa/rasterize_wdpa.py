
from shapely.geometry import box
import numpy as np
from affine import Affine
from rasterio import features
import rasterio
import fiona
import pandas as pd
import geopandas as gpd


def split_geom(geom, split_map=False):

    feat_parts = []

    if geom.area == 0:
        return feat_parts
    else:
        gb = tuple(geom.bounds)
        box_area = box(*gb).area


    if box_area < 200:
        print "Box area: {0}".format(box_area)
        feat_parts += [geom]

    else:
        # find largest dimension of box
        # split in half along that dimension
        x_size = gb[2] - gb[0]
        y_size = gb[3] - gb[1]

        if split_map:
            box_a_bounds = (-180, -90, 0, 90)
            box_b_bounds = (0, -90, 180, 90)
        elif x_size > y_size:
            x_split = gb[2]-(gb[2]-gb[0])/2
            box_a_bounds = (gb[0], gb[1], x_split, gb[3])
            box_b_bounds = (x_split, gb[1], gb[2], gb[3])
        else:
            y_split = gb[3]-(gb[3]-gb[1])/2
            box_a_bounds = (gb[0], gb[1], gb[2], y_split)
            box_b_bounds = (gb[0], y_split, gb[2], gb[3])


        box_a = box(*box_a_bounds)
        geom_a = geom.intersection(box_a)
        split_a = split_geom(geom_a)
        feat_parts += split_a

        box_b = box(*box_b_bounds)
        geom_b = geom.intersection(box_b)
        split_b = split_geom(geom_b)
        feat_parts += split_b

    return feat_parts


def get_output_raster(ref_shp, psize):

    try:
        psize = float(psize)
    except:
        raise Exception("Invalid pixel size (could not be converted to float)")

    c = fiona.open(ref_shp)
    #print c.bounds
    (minx, miny, maxx, maxy) = c.bounds
    out_shape = (int(round((maxy - miny) / psize))+1,
                 int(round((maxx - minx) / psize))+1)
    c.close()

    return out_shape, minx, maxy


def rasterize(inpath, pxl_size, fieldnms, outpath):

    """
    rasterize features
    arg:
        - input shapefile path
        - pixel size of output raster file
        - fieldnms: a dictionary with attribute table field name as keys and feature values in list as cooresponding value
        - outpath: output path

    :return
        a series of raster files in output path, binary outputs
    """

    try:
        pxl_size = float(pxl_size)
    except:
        raise Exception("pixel size must be a number")


    try:
        gdf = gpd.read_file(inpath)
    except:
        df = pd.DataFrame()
        with fiona.open(inpath) as shp:
            for feat in shp:
                new_props = feat['properties'].copy()
                new_props['geometry'] = feat['geometry']
                df = df.append(new_props, ignore_index=True)
            gdf = gpd.GeoDataFrame(df)

    (nrows, ncols), minx, maxy = get_output_raster(inpath, pxl_size)

    for fieldnm in fieldnms.keys():
        for value in fieldnms[fieldnm]:
            select_boundary = gdf[gdf[fieldnm] == value]

            outfn = outpath + ".{0}_{1}.tif".format(fieldnm, value)

            transform = Affine(pixels, 0, minx,
                               0, -pixels, maxy)

            with rasterio.open(outfn, 'w', driver='GTiff', dtype=rasterio.uint8,
                               count=1, width=ncols, height=nrows, affine=transform) as out:
                shapes = [(feat, 1) for feat in select_boundary.geometry]
                burned = features.rasterize(shapes, transform=transform, out_shape=(nrows, ncols))

                out.write_band(1, burned.astype(np.uint8))



fieldnms = {
    "NAME_1":["Virginia", "Massachusetts"]
}

"""
    "DESIG_TYPE":["National", "Regional", "International", "Not Applicable"],
    "IUCN_CAT": ["Ia", "Ib", "II", "III", "IV", "V", "VI", "Not Applicable", "Not Assigned", "Not Reported"]

"""
inshp = "/Users/miranda/Documents/AidData/sciclone/datasets/wdpa/example/USA_adm_shp/USA_adm2.shp"
pixels = 0.001
output = "/Users/miranda/Documents/AidData/sciclone/datasets/wdpa/example/wdpa"


rasterize(inshp, pxl_size=pixels, fieldnms=fieldnms, outpath=output)
