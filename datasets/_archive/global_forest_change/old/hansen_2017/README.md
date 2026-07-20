
Data source: https://earthenginepartners.appspot.com/science-2013-global-forest/download_v1.5.html


Data Download:
- download a list of .txt files that include a list of tiles (loss, first, etc)
- run download_all_global_hansen.sh to download all tiles for each layer
- data path: /sciclone/aiddata10/REU/pre_geo/hansen_2017

Image Mosaic
- Run bash mosaic_hansen.sh "layer name" to mosaic images




Data Layers
- Tree canopy cover for year 2000 (treecover2000):
Tree cover in the year 2000, defined as canopy closure for all vegetation taller than 5m in height. Encoded as a percentage per output grid cell, in the range 0–100.

- Global forest cover gain 2000–2012 (gain):
Forest gain during the period 2000–2012, defined as the inverse of loss, or a non-forest to forest change entirely within the study period. Encoded as either 1 (gain) or 0 (no gain).

- Year of gross forest cover loss event (lossyear):
Forest loss during the period 2000–2017, defined as a stand-replacement disturbance, or a change from a forest to non-forest state. Encoded as either 0 (no loss) or else a value in the range 1–17, representing loss detected primarily in the year 2001–2017, respectively.

- Data mask (datamask):
Three values representing areas of no data (0), mapped land surface (1), and permanent water bodies (2).

- Circa year 2000 Landsat 7 cloud-free image composite (first): 
Reference multispectral imagery from the first available year, typically 2000. If no cloud-free observations were available for year 2000, imagery was taken from the closest year with cloud-free data, within the range 1999–2012.

- Circa year 2017 Landsat cloud-free image composite (last): 
Reference multispectral imagery from the last available year, typically 2017. If no cloud-free observations were available for year 2017, imagery was taken from the closest year with cloud-free data, within the range 2010–2015.


