# ACLED Conflict Event Count

Number of conflict events per 0.1 decimal degree grid cell, rasterized from
[ACLED (Armed Conflict Location & Event Data Project)](https://acleddata.com)
point data.

## Status

Not yet migrated to a Prefect flow. The existing raster was produced manually:
point data (CSV) was downloaded from ACLED and rasterized in QGIS. 

## Source

[ACLED](https://acleddata.com) — access to bulk data now requires
registration; see their [data export tool](https://acleddata.com/data-export-tool/)
for current download options.

## Reference

Raleigh, Clionadh, Andrew Linke, Håvard Hegre and Joakim Karlsen. 2010.
Introducing ACLED-Armed Conflict Location and Event Data. Journal of Peace
Research 47(5) 651-660.
