# VIIRS Nighttime Lights TS

[The Visible Infrared Imaging Radiometer Suite (VIIRS)](https://ncc.nesdis.noaa.gov/VIIRS/) is an instrument on a National Oceanic and Atmospheric Administation (NOAA) satellite that collects atmospheric imagery.
The [Earth Observation Group (EOG)](https://payneinstitute.mines.edu/eog/) at Colorado School of Mines produces monthly and annual composites of nighttime lights, using data from VIIRS.

viirs = Visible Infrared Imaging Radiometer Suite

dnb = Day/Night Band

vcm = viirs cloud mask

sl = stray light

cfg = cloud free grids

vcmcfg - excludes any data contaminated by stray light
vcmslcfg - data impacted by stray light are corrected but not removed

## Instructions

1. Download compressed raw data (both vcmcfg and vcmslcfg) from source
	- Set start and end year in download.sh
	- Run download script:
		```
		bash download.sh
		```

2. Unzip desired files from raw data
	+ Set start and end year in extract.sh
	+ Run extract script:
		```
		bash extract.sh
		```

3. Filter/prepare raw monthly data tiles
	- Adjust the following settings in viirs_data_filter.py:
		- Set list of all years to process (can be int or str)
		- Set value for minimum cloud free day threshold
		+ Set mode (serial or parallel)
	+ Edit jobscript_data_filter based on resources needed
	+ Submit jobscript_data_filter job:
		```
		qsub jobscript_data_filter
		```

4. Mosaic filtered monthly tiles (result of viirs_data_filter.py)
	- Adjust the following settings in viirs_mosaic.py:
		+ Set years (must be list of integers)
		+ Set mode (serial or parallel)
	+ Edit jobscript_mosaic based on resources needed
	+ Submit jobscript_mosaic job
		```
		qsub jobscript_mosaic
		```

5. Create yearly aggregates from filter monthly data (result of viirs_data_filter.py) and then mosaics
	- Adjust the following settings in viirs_yearly.py:
		+ Set years (must be list of integers)
		+ Set aggregation method (default: max)
		+ Set run_agg and run_mosaic boolean variables
		+ Set mode (serial or parallel)
	+ Edit jobscript_yearly based on resources needed
	+ Run jobscript_yearly
		```
		qsub jobscript_yearly
		```

## References 

Source link:
https://eogdata.mines.edu/products/vnl/

Monthly Cite:
C. D. Elvidge, K. E. Baugh, M. Zhizhin, and F.-C. Hsu, “Why VIIRS data are superior to DMSP for mapping nighttime lights,” Asia-Pacific Advanced Network 35, vol. 35, p. 62, 2013.

Annual Cite:
Elvidge, C.D, Zhizhin, M., Ghosh T., Hsu FC, Taneja J. Annual time series of global VIIRS nighttime lights derived from monthly averages:2012 to 2019. Remote Sensing 2021, 13(5), p.922,
