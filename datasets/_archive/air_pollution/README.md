# Ambient Air Pollution (GBD 2013)

Ozone and PM2.5 (fus_calibrated) point estimates from the Global Burden of
Disease 2013 exposure assessment (Brauer et al., 2016), rasterized to 0.1
degree resolution for each year in `1990, 1995, 2000, 2005, 2010-2013`.

## Manual download step

The source file is served by `pubs.acs.org` behind a Cloudflare bot challenge
that a plain HTTP request cannot pass (confirmed 403 even with a browser
User-Agent), so it can't be automated. No public mirror was found either, so
this has to be done by hand before running the flow:

1. In a browser, download the "SI 005" zip from
   https://pubs.acs.org/doi/suppl/10.1021/acs.est.5b03709/suppl_file/es5b03709_si_005.zip
   (or via the article page: https://pubs.acs.org/doi/10.1021/acs.est.5b03709)
2. Unzip it and place `GBD2013final.csv` at `<raw_dir>/GBD2013final.csv`
   (i.e. the `raw_dir` value set in `config.toml`)

The flow will fail with a clear error if the CSV isn't there.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` — directory containing the manually-placed `GBD2013final.csv`
- `output_dir` — where the output rasters are written, one subdirectory per
  pollutant (`o3/`, `fus_calibrated/`), each containing one file per year
  (e.g. `o3/o3_1990.tif`)
- `overwrite_process`, if true, overwrites existing output files rather than
  skipping them

## Reference

Brauer M, Freedman G, Frostad J, van Donkelaar A, Martin RV, Dentener F, Van
Dingenen R, Estep K, Amini H, Apte JS, Balakrishnan K, Barregard L, Broday DM,
Feigin V, Ghosh S, Hopke PK, Knibbs LD, Kokubo Y, Liu Y, Ma S, Morawska L,
Sangrador JLT, Shaddick G, Anderson HR, Vos T, Forouzanfar MH, Burnett RT,
Cohen A. Ambient air pollution exposure estimation for the Global Burden of
Disease 2013. Environmental Science & Technology. 2015 Nov 23. doi:
10.1021/acs.est.5b03709.
