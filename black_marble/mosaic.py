#!/usr/bin/env python

# https://viirsland.gsfc.nasa.gov/PDF/BlackMarbleUserGuide_v1.2_20210421.pdf


#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_long_name=Temporal Radiance Composite Using All Observations During Snow-free Period
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_coordinates=latitude longitude
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_long_name=Number of Observations of Temporal Radiance Composite Using All Observations During Snow-free Period
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_offset=0
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_scale_factor=1
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_units=number of observations
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num_valid_range=0 65534
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Num__FillValue=65535
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_offset=0
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_coordinates=latitude longitude
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_Description=Quality:
# 	  0 = Good quality
# 	  1 = Poor quality
# 	  2 = Gap filled
# 	  255 = Fill value

#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_long_name=Quality Flag of Temporal Radiance Composite Using All Observations During Snow-free Period
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_offset=0
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_scale_factor=1
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_units=quality flag, no units
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality_valid_range=0 254
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Quality__FillValue=255
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_scale_factor=0.1
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_coordinates=latitude longitude
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_long_name=Standard Deviation of Temporal Radiance Composite Using All Observations During Snow-free Period
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_offset=0
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_scale_factor=0.1
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_units=nWatts/(cm^2 sr)
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std_valid_range=0 65534
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_Std__FillValue=65535
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_units=nWatts/(cm^2 sr)
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free_valid_range=0 65534
#   HDFEOS_GRIDS_VIIRS_Grid_DNB_2d_Data_Fields_AllAngle_Composite_Snow_Free__FillValue=65535


from pathlib import Path

import rasterio
from rasterio import Affine, windows


def build_convert_list(year_list, mode, results_dir):
    convert_task_list = []

    for year in year_list:
        year_dir = results_dir / 'data' / 'h5_tiles' / mode / str(year)

        year_files = list(year_dir.iterdir())

        if mode == 'monthly':
            for month_dir in year_files:
                month = month_dir.name
                month_dir = year_dir / month
                month_files = list(month_dir.iterdir())
                convert_task_list.extend(month_files)
        else:
            convert_task_list.extend(year_files)

    convert_task_list = [[i] for i in convert_task_list]
    return convert_task_list


def convert_hdf_to_geotiff(data_path, lat_path, lon_path, output_path):
    '''Convert Black Marble HDF (h5) tiles to GeoTIFF (tif) tiles
    '''
    data = rasterio.open(data_path).read()
    lat = rasterio.open(lat_path).read()
    lon = rasterio.open(lon_path).read()

    lon_min = lon.min()
    lon_max = lon.max()
    lon_size = len(lon[0][0])
    lon_res = lon[0][0][1] - lon[0][0][0]

    lat_min = lat.min()
    lat_max = lat.max()
    lat_size = len(lat[0][0])
    lat_res = lat[0][0][1] - lat[0][0][0]

    profile = {
        'driver': 'COG',
        # "bigtiff": True,
        'compress': 'LZW',
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        'dtype': 'uint16',
        'nodata': 65535,
        'count': 1,
        'width': lon_size,
        'height': lat_size,
        'transform': Affine(lon_res, 0.0, lon_min,
                            0.0, lat_res, lat_max),
        'crs': {'init': 'epsg:4326'},
    }

    with rasterio.open(output_path, 'w+', **profile) as dataset:
        dataset.write(data)




def run_convert(h5_tile_path):
    try:
        hdf_base = f'HDF5:{h5_tile_path}://HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data_Fields'
        data_path = f'{hdf_base}/AllAngle_Composite_Snow_Free'
        lat_path = f'{hdf_base}/lat'
        lon_path = f'{hdf_base}/lon'
        tif_tile_path = Path(str(h5_tile_path).replace('h5_tiles', 'tif_tiles').replace('.h5', '.tif'))
        tif_tile_path.parent.mkdir(parents=True, exist_ok=True)
        convert_hdf_to_geotiff(data_path, lat_path, lon_path, tif_tile_path)
        return (0, None, tif_tile_path)
    except Exception as e:
        return (1, repr(e), h5_tile_path)



def build_mosaic_list(year_list, mode, results_dir):

    output_dir = results_dir / 'data' / 'tif_mosaic' / mode
    output_dir.mkdir(parents=True, exist_ok=True)

    mosaic_task_list = []

    for year in year_list:
        year_dir = results_dir / 'data' / 'tif_tiles' / mode / str(year)

        year_files = list(year_dir.iterdir())

        if mode == 'monthly':
            for month_dir in year_files:
                month = month_dir.name
                month_dir = year_dir / month
                month_files = list(month_dir.iterdir())
                output_path = output_dir / f'{year}_{month}.tif'
                mosaic_task_list.append([f'{year}_{month}', month_files, output_path])
        else:
            output_path = output_dir / f'{year}.tif'
            mosaic_task_list.append([f'{year}', year_files, output_path])

    return mosaic_task_list




def create_mosaic(tile_list, output_path):
    """Combine tiles from list into mosaic raster

    Memory efficient mosaic function
    Based on code from:
    - https://gis.stackexchange.com/questions/348925/merging-rasters-with-rasterio-in-blocks-to-avoid-memoryerror
    - https://github.com/mapbox/rasterio/blob/master/rasterio/merge.py
    - https://github.com/mapbox/rio-merge-rgba/blob/master/merge_rgba/__init__.py
    """

    res = 0.004166666666662877

    out_transform = Affine.translation(-180, 90)
    # Resolution/pixel size
    out_transform *= Affine.scale(res, -res)

    # Compute output array shape. We guarantee it will cover the output bounds completely
    output_width = int(360 / res)
    output_height = int(180 / res)

    # destination array shape
    shape = (output_height, output_width)

    dest_profile = {
        "driver": 'COG',
        # "bigtiff": True,
        "compress": "LZW",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "dtype": 'uint16',
        "nodata": 65535,
        "count": 1,
        "width": shape[1],
        "height": shape[0],
        "transform": out_transform,
        'crs': {'init': 'epsg:4326'},
    }

    # open output file in write/read mode and fill with destination mosaic array
    with rasterio.open(output_path, 'w+', **dest_profile) as mosaic_raster:
        for tile_path in tile_list:
            with rasterio.open(tile_path, 'r') as tile_src:
                for ji, src_window in tile_src.block_windows(1):
                    # convert relative input window location to relative output window location
                    # using real world coordinates (bounds)
                    src_bounds = windows.bounds(src_window, transform=tile_src.profile["transform"])
                    dst_window = windows.from_bounds(*src_bounds, transform=mosaic_raster.profile["transform"])
                    # round the values of dest_window as they can be float
                    dst_window = windows.Window(round(dst_window.col_off), round(dst_window.row_off), round(dst_window.width), round(dst_window.height))
                    # read data from source window
                    r = tile_src.read(1, window=src_window)
                    # if tiles/windows have overlap:
                    # before writing the window, replace source nodata with dest nodata as it can already have been written
                    # dest_pre = mosaic_raster.read(1, window=dst_window)
                    # mask = (np.isnan(r))
                    # r[mask] = dest_pre[mask]
                    # write data to output window
                    mosaic_raster.write(r, 1, window=dst_window)



def run_mosaic(temporal, file_list, output_path):
    try:
        create_mosaic(file_list, output_path)
        return (0, None, temporal, output_path)
    except Exception as e:
        return (1, repr(e), temporal, output_path)

