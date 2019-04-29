
https://earthenginepartners.appspot.com/science-2013-global-forest/download_v1.6.html


# overview of layers

## default layers

treecover2000
- Tree canopy cover for year 2000 (treecover2000)
- Tree cover in the year 2000, defined as canopy closure for all vegetation taller than 5m in height. Encoded as a percentage per output grid cell, in the range 0–100.

gain
- Global forest cover gain 2000–2018 (gain)
- Forest gain during the period 2000–2018, defined as the inverse of loss, or a non-forest to forest change entirely within the study period. Encoded as either 1 (gain) or 0 (no gain).

lossyear
- Year of gross forest cover loss event (lossyear)
- A disaggregation of total forest loss to annual time scales. Encoded as either 0 (no loss) or else a value in the range 1–17, representing loss detected primarily in the year 2001–2018, respectively.

datamask
- Data mask (datamask)
- Three values representing areas of no data (0), mapped land surface (1), and permanent water bodies (2).




# use of layers

## tree cover

    _% treecover_ = mean of all cells in boundary using treecover00 layer

    _total % loss_ = % of cells in boundary from loss layer where value == 1

    _yearly % loss_ = loss for each year (Y) is % of cells from lossyear layer where value == Y
