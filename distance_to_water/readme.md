## Distance to Water

Data from [Earth Observation Group](https://payneinstitute.mines.edu/eog/)
Distance to water, measured in meters. Derived using World Vector Shorelines from [GSHHG](http://www.soest.hawaii.edu/pwessel/gshhg/) combined with rivers and lakes from World Data Bank 2 (via [Natural Earth](https://github.com/nvkelso/natural-earth-vector)).

To run:
1.  If on HPC (vortex-alpha nodes), prepare modules and base environment
```
source "/opt/anaconda3-2021.05/etc/profile.d/conda.csh"
module unload gcc/4.7.3 python/2.7.8 openmpi-1.10.0-gcc mpi4py-2.0.0-gcc acml/5.3.1 numpy/1.9.2 gdal-nograss/1.11.2 proj/4.7.0 geos/3.5.0
module load gcc/9.3.0 openmpi/3.1.4/gcc-9.3.0 anaconda3/2021.05
```

2. Create Conda environment (if does not exist yet):
```
conda create -n va_geo python=3.9 -c conda-forge
conda activate va_geo
conda install -c conda-forge rasterio pandas geopandas shapely fiona pyproj requests prefect prefect-dask dask-jobqueue
```

3. Load Conda environment:
```
conda activate va_geo
```

4. Update config options in `config.ini`

5. Run `build_dist_to_water.py`:
```
 python build_dist_to_water.py
