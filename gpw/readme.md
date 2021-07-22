
# GPW Version 4 Revision 11 - Population Count and Density (UN Adjusted)

Estimates of population count and density produec by SEDAC for the years 2000, 2005, 2010, 2015, and 2020, consistent with national censuses and population registers with respect to relative spatial distribution, but adjusted to match United Nations country totals.

For full details see:
- [population count](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11)
- [population density](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11)

**Please note we include steps to manually download data but suggest using the automated process for reproducibility.**


## Manual Download Steps:

1. Go to the [population count data download page](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11/data-download) and follow the prompt to login.
    - Create an EarthData account if needed
2. Return to the [population count data download page](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11/data-download) (may automatically return you) and create a download using the fields available:
    - Temporal: "Single Year"
    - FileFormat: "GeoTiff"
    - Resolution: "30 Seconds (approx. 1km)
    - Check the "Select All" box for years
    - Click "Create Download"

3. You may now download the files manually, and repeat the above steps on the [population density data download page](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11/data-download)




## Automated Download Steps:

1. Go to the [population count data download page](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11/data-download) and follow the prompt to login.
    - Create an EarthData account if needed
2. Return to the [population count data download page](https://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11/data-download) (may automatically return you)

3. Open your browsers developer tools / console and go to the network activity section (in Chrome this can usually be accessed by hitting F12 and selecting network tab)

4. Create a download using the fields available:
    - Temporal: "Single Year"
    - FileFormat: "GeoTiff"
    - Resolution: "30 Seconds (approx. 1km)
    - Check the "Select All" box for years
    - Click "Create Download"

5. Start and immediately cancel a download for any of the listed files.

6. Find the corresponding network activity for the download request (name should start with "gpw-v4" and be for a ZipFile)

7. Click on the "Headers" tab for the download request

8. Scroll down to the "Request Headers" section and find the "Cookie" item

9. Within the "Cookie" item, there should be a key called "sedac", copy the value associated with this key

10. Add this key as the value for the "sedac" variable in the download.sh script included in this repository

11. Edit download.sh to reflect the destination directories where you would like the files to be downloaded and unzipped to.
    - You may also comment out any of the wget download/unzip lines for files you do not want
    - Set the "unzip_only" variable to "true" if you only want to unzip the files and not download them (if you downloaded them using a different structure than the one used in download.sh, this script will not work)

12. Run the download.sh script
    - `bash download.sh`




## Previous versions:

V4.10

Download data manually from:

http://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals
http://sedac.ciesin.columbia.edu/data/set/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals

V3

Download data manually from:

http://sedac.ciesin.columbia.edu/data/set/gpw-v3-population-count
http://sedac.ciesin.columbia.edu/data/set/gpw-v3-population-density


