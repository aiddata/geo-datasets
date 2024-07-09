
https://earthenginepartners.appspot.com/science-2013-global-forest/download_v1.2.html

# overview of layers


## default layers

treecover2000  
- Tree canopy cover for year 2000 (treecover2000)
- Tree cover in the year 2000, defined as canopy closure for all vegetation taller than 5m in height. Encoded as a percentage per output grid cell, in the range 0–100.

loss  
- Global forest cover loss 2000–2014 (loss)
- Forest loss during the period 2000–2014, defined as a stand-replacement disturbance, or a change from a forest to non-forest state. Encoded as either 1 (loss) or 0 (no loss).

gain  
- Global forest cover gain 2000–2012 (gain)
- Forest gain during the period 2000–2012, defined as the inverse of loss, or a non-forest to forest change entirely within the study period. Encoded as either 1 (gain) or 0 (no gain).

lossyear  
- Year of gross forest cover loss event (lossyear)
- A disaggregation of total forest loss to annual time scales. Encoded as either 0 (no loss) or else a value in the range 1–13, representing loss detected primarily in the year 2001–2014, respectively.

datamask  
- Data mask (datamask)
- Three values representing areas of no data (0), mapped land surface (1), and permanent water bodies (2).



## custom layers

00forest25
- 00forest25 = 1 if treecover2000 >= 25 else 0
- boolean layer identifying forest based on whether there was at least 25% treecover in treecover2000

lossgain
- lossgain = loss + gain
- 0: no loss or gain
- 1: loss or gain
- 2: loss and gain

loss25
- loss25 = loss * 00forest25
- forest loss for forested areas from treecover2000 (see 00forest25) only

lossyr25
- lossyr25 = lossyear * 00forest25
- year of loss for areas with loss from loss25



# use of layers

## tree cover
    
    _% treecover_ = mean of all cells in boundary using treecover00 layer

    _total % loss_ = % of cells in boundary from loss layer where value == 1

    _yearly % loss_ = loss for each year (Y) is % of cells from lossyear layer where value == Y


## forest cover

    _% forested_ = % of cells in boundary from 00forest25 layer where value == 1

    _total % loss_ = % of cells in boundary from loss25 layer where value == 1

    _yearly % loss_ = loss for each year (Y) is % of cells from lossyr25 layer where value == Y
