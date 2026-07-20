# Accessibility Map — travel time to major cities

Estimated travel time (in minutes) to the nearest city of 50,000 or more people
in the year 2000, from the JRC Global Accessibility Map (GAM). A single static
global raster at ~1 km resolution. The flow downloads the source archive and
rewrites the packaged GeoTIFF as a validated Cloud Optimized GeoTIFF.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` — where the source archive (`access_50k.zip`) is downloaded
- `output_dir` — where the output COG (`access_50k.tif`) is written
- `overwrite_download` / `overwrite_process`, if true, overwrite existing files
  rather than skip them

No authentication is required; the archive is served anonymously.

## Source

Global Environment Monitoring Unit, Joint Research Centre of the European
Commission — https://forobs.jrc.ec.europa.eu/gam

## Reference

Nelson, A. (2008) Estimated travel time to the nearest city of 50,000 or more
people in year 2000. Global Environment Monitoring Unit — Joint Research Centre
of the European Commission, Ispra, Italy.
