# Malaria Atlas Project

A single flow for downloading and converting raster products from the
[Malaria Atlas Project](https://data.malariaatlas.org) GeoServer. One flow run
produces one product, selected via the `dataset` config field.

## Available datasets

| `dataset` value | description | shape |
|---|---|---|
| `pf_incidence_rate` | Malaria (*Plasmodium falciparum*) incidence rate, per 1,000 people, 2000-2021 | temporal (per-year) |
| `travel_time_to_cities_2015` | Travel time (minutes) to the nearest city, 2015 | static |
| `motorized_travel_time_to_healthcare_2020` | Motorized travel time (minutes) to the nearest healthcare facility, 2019 | static |
| `walking_travel_time_to_healthcare_2020` | Walking-only travel time (minutes) to the nearest healthcare facility, 2019 | static |

**Temporal** datasets are distributed as a single archive containing one
GeoTIFF per year; the `years` config field selects which years to extract.
**Static** datasets are a single GeoTIFF with no year dimension; `years` is
ignored.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `dataset` selects the Malaria Atlas data product to download
- `years` is a comma-separated list of years to process (temporal datasets only)
- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Adding a new dataset

Data is no longer directly available from a file server — it's served via a
GeoServer instance. To find a new product's download URL:

1. Find its GeoServer workspace and `ResourceId`. The WCS `GetCapabilities`
   document for a workspace
   - E.g. For the `Accessibility` workspace visit
   `https://data.malariaatlas.org/geoserver/Accessibility/ows?service=WCS&version=2.0.1&request=GetCapabilities`
   - `CoverageId` items list the resources in `<workspace>__<resource>` form.
   - Searching for `CoverageId` would include: `<wcs:CoverageId>Accessibility__202001_Global_Motorized_Travel_Time_to_Healthcare</wcs:CoverageId>` which wou
2. Add an entry to `DATASET_LOOKUP` in `main.py`
   - set `temporal` based on whether the archive contains one file per year or a single static file)
   - set `workspace` to the GeoServer workspace name
   - set `resource` to the GeoServer resource name (the part after the workspace in the `CoverageId`)
3. Create a corresponding `<dataset>_raster_ingest.json`

## Important notes

- Data is retrieved through the Malaria Atlas Project's GeoServer and
  converted to Cloud Optimized GeoTIFFs.
