

from pathlib import Path
import tarfile
import requests

import rasterio
from rasterio import features
import numpy as np
import pandas as pd
import geopandas as gpd


# raw_dir = Path('/sciclone/aiddata10/REU/pre_geo/raw/udel_climate')
# data_dir = Path('/sciclone/aiddata10/REU/pre_geo/data/rasters/udel_climate')

raw_dir = Path('/home/userx/Desktop/pre_geo/raw/udel_climate')
data_dir = Path('/home/userx/Desktop/pre_geo/data/rasters/udel_climate')


methods = ['mean', 'min', 'max', 'sum', 'var', 'sd']

build_monthly = True
build_yearly = True

overwrite_download = False
overwrite_processing = False

# =====================================


tmp_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsT2017.html"
tmp_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/air_temp_2017.tar.gz"
pre_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsP2017.html"
pre_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/precip_2017.tar.gz"

download_urls = [tmp_readme_url, tmp_data_url, pre_readme_url, pre_data_url]

raw_dir.mkdir(parents=True, exist_ok=True)

# download data
for url in download_urls:
    fname = url.split('/')[-1]
    fpath = raw_dir / fname
    if not fpath.exists() or overwrite_download:
        print(f'downloading {url}...')
        r = requests.get(url, allow_redirects=True)
        with open(fpath, 'wb') as dst:
            dst.write(r.content)
        # wget.download(url, out=str(fpath))

# extract
extract_list = list(raw_dir.glob('*.tar.gz'))

for fpath in extract_list:
    dirname = str(fpath).split('.')[0]
    print(f'extracting {fpath}...')
    with tarfile.open(fpath) as tar:
        tar.extractall(path=raw_dir/dirname)




# process
data_dirname_list = [str(i).split('/')[-1].split('.')[0] for i in extract_list]

flist = [(i, list((raw_dir / i).glob('*'))) for i in data_dirname_list]

if len(flist) == 0 or len(flist[0][1]) == 0 or len(flist[1][1]) == 0:
    raise Exception(f'no files found ({raw_dir})')




months = [f'{i:02d}' for i in range(1, 13)]

for dataset, data_files in flist:
    for fpath in data_files:

        print(f'processing {fpath}...')
        year = fpath.name.split('.')[1]
        data = pd.read_csv(fpath, sep='\s+', header=None)
        data.columns = ['lon', 'lat'] + months + ["extra"]

        gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon, data.lat))
        gdf = gdf.set_crs(epsg=4326)

        meta = {
            'driver': 'COG',
            'compress': 'LZW',
            'dtype': 'float32',
            'height': 360,
            'width': 720,
            'count': 1,
            'crs': 'EPSG:4326',
            'transform': rasterio.Affine(0.5, 0.0, -180.0, 0.0, -0.5, 90.0),
            'nodata': -9999.0,
        }

        # monthly
        if build_monthly:

            out_dir = data_dir / dataset / 'monthly' / year
            out_dir.mkdir(parents=True, exist_ok=True)

            for m in months:

                out_path = data_dir / dataset / 'monthly' / year / f"{dataset}_{year}.tif"

                if out_path.exists() and not overwrite_processing:
                    print(f'\tmonthly {year}_{m} exists, skipping...')
                    continue

                else:

                    print(f'\tbuilding monthly {year}_{m}...')

                    month_gdf = gdf[['geometry', m]].copy()
                    month_gdf = month_gdf.rename(columns={m: 'value'})

                    shapes = list((geom, value) for geom, value in zip(month_gdf.geometry, month_gdf.value))
                    out = features.rasterize(list(shapes), out_shape=(meta['height'], meta['width']), fill=meta['nodata'], transform=meta['transform'], dtype=meta['dtype'])

                    with rasterio.open(out_path, 'w', **meta) as dst:
                        dst.write(np.array([out]))


        # yearly
        if build_yearly:

            for j in methods:

                out_path = data_dir / dataset / 'yearly' / j / f"{dataset}_{year}.tif"

                if out_path.exists() and not overwrite_processing:
                    print(f'\tyearly {year}_{j} exists, skipping...')
                    continue

                else:

                    print(f'\tbuilding yearly {year}_{j}...')
                    out_dir = data_dir / dataset/ 'yearly' / j
                    out_dir.mkdir(parents=True, exist_ok=True)

                    gdf[f"year_{j}"] = gdf[months].apply(j, axis=1)
                    year_gdf = gdf[['geometry', f"year_{j}"]].copy()
                    year_gdf = year_gdf.rename(columns={f"year_{j}": 'value'})

                    shapes = list((geom, value) for geom, value in zip(year_gdf.geometry, year_gdf.value))
                    out = features.rasterize(list(shapes), out_shape=(meta['height'], meta['width']), fill=meta['nodata'], transform=meta['transform'], dtype=meta['dtype'])

                    with rasterio.open(out_path, 'w', **meta) as dst:
                        dst.write(r)