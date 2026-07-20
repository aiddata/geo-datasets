# Global Wind Atlas

Wind resource potential at 50m from the Global Wind Atlas (Technical
University of Denmark / World Bank Group / ESMAP, data provided by Vortex).
Two products:

- **Wind Speed** (`ws_raster_ingest.json`) — wind speed potential at 50m, m/s
- **Power Density** (`pd_raster_ingest.json`) — wind power density potential
  at 50m, W/m²

## Manual download

Each product is downloaded by hand via the site's API links (which redirect
to a figshare-hosted file and don't resolve with a plain HTTP request, so
this can't be automated):

- Wind speed (50m): https://globalwindatlas.info/api/gis/global/wind-speed/50
- Power density (50m): https://globalwindatlas.info/api/gis/global/power-density/50

Open each link in a browser to download the GeoTIFF.

## Source

[Global Wind Atlas](https://globalwindatlas.info/about/) — developed, owned,
and operated by the Technical University of Denmark (DTU) in partnership with
the World Bank Group, using data provided by Vortex, with funding from the
Energy Sector Management Assistance Program (ESMAP).

## Citation

Wind data obtained from the "Global Wind Atlas 2.0", a free, web-based
application developed, owned and operated by the Technical University of
Denmark (DTU) in partnership with the World Bank Group, utilizing data
provided by Vortex, with funding provided by the Energy Sector Management
Assistance Program (ESMAP). https://globalwindatlas.info.
