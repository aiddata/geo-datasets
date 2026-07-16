# Annual VIIRS Nighttime Lights v2.2 - Average Value

Annual VIIRS nighttime lights product Version 2.2. Average value with background pixels masked. Please use in combination with VIIRS Nighttime Light Cloud Free Coverage product to confirm sufficient cloud free measurements are available within your boundary features.

[The Visible Infrared Imaging Radiometer Suite (VIIRS)](https://ncc.nesdis.noaa.gov/VIIRS/) is an instrument on a National Oceanic and Atmospheric Administation (NOAA) satellite that collects atmospheric imagery.
The [Earth Observation Group (EOG)](https://payneinstitute.mines.edu/eog/) at Colorado School of Mines produces monthly and annual composites of nighttime lights, using data from VIIRS.

Product abbreviations:
	- viirs = Visible Infrared Imaging Radiometer Suite
	- dnb = Day/Night Band
	- vcm = viirs cloud mask
	- sl = stray light
	- cfg = cloud free grids
	- vcmcfg = excludes any data contaminated by stray light
	- vcmslcfg = data impacted by stray light are corrected but not removed

## Quick start


1. Create an account for [https://eogdata.mines.edu/nighttime_light](https://eogdata.mines.edu/nighttime_light)
	- Add username and password to get_token.py (Do not share your password publicly on GitHub or elsewhere)

2. Review and edit the variables in `config.toml` as needed
    - `run_annual`
    - `annual_version`
    - `years` is a comma-separated list of years to process
    - `run_monthly`
    - `months`
    - `cf_minimum`
    - `annual_files`
    - `monthly_files`
    - `raw_dir` is a working/output directory
    - `output_dir` is a working/output directory
    - `overwrite_download`, if true, overwrites existing files rather than skipping
    - `overwrite_extract`, if true, overwrites existing files rather than skipping
    - `overwrite_processing`, if true, overwrites existing files rather than skipping
    - `max_retries`
    - `username`
    - `password`
    - `client_secret`

## Source

[Earth Observation Group - VIIRS Nighttime Lights](https://eogdata.mines.edu/products/vnl/)

## References

Any VNL
C. D. Elvidge, K. E. Baugh, M. Zhizhin, and F.-C. Hsu, “Why VIIRS data are superior to DMSP for mapping nighttime lights,” Asia-Pacific Advanced Network 35, vol. 35, p. 62, 2013.

Annual VNL V2
C. D. Elvidge, M. Zhizhin, T. Ghosh, F-C. Hsu, "Annual time series of global VIIRS nighttime lights derived from monthly averages: 2012 to 2019", Remote Sensing (In press)
