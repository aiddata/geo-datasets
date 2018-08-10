# CRU dataset version 4.01

download:

- main page: https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.01/cruts.1709081022.v4.01/

for each variable (dir at above link) go into dir and download file of format "cru_ts4.01.1901.2016.{var}.dat.nc.gz"
- example url for precipitation (var = pre): https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.01/cruts.1709081022.v4.01/pre/cru_ts4.01.1901.2016.pre.dat.nc.gz

- gunzip all the downloaded .gz files (gunzip --keep input_regex output_dir)

- run extract_data.R to get 10 different monthly variable dataset ranging from 1901 to 2016
- have to install R package 'cruts'
- the export dataset directory: /sciclone/aiddata10/REU/pre_geo/CRU (precipication/pre_extract/output)

to get yearly aggregation dataset:
- run build_monthly.py

data information: https://crudata.uea.ac.uk/cru/data/hrg/#info

If the page is not available, below has more details about the dataset

Availability
Some data is available from the CRU website. However, most recent data (CRU TS, CRU CY) is distributed via the BADC. DropBox may be used in extraordinary circumstances to allow timely access to new releases.

Where the status is marked as 'available on request', please email cru@uea.ac.uk, stating which dataset(s) you require and the area of research you intend to use them in. Please do not request those datasets marked as 'superseded', unless you are certain that you require them.



Conditions of use
The various datasets on the CRU website are provided for all to use, provided the sources are acknowledged. Acknowledgement should preferably be by citing one or more of the papers referenced. The website can also be acknowledged if deemed necessary.



References: Please always reference the listed papers when using these datasets.
Harris, I., Jones, P.D., Osborn, T.J. and Lister, D.H. (2014), Updated high-resolution grids of monthly climatic observations - the CRU TS3.10 Dataset. International Journal of Climatology 34, 623-642
doi:10.1002/joc.3711 (click doi to access paper)
Correction to the above paper: Revised Appendix 3 (CLD)
Mitchell, T.D. and Jones, P.D., 2005: An improved method of constructing a database of monthly climate observations and associated high-resolution grids. International Journal of Climatology 25, 693-712
doi:10.1002/joc.1181 (click doi to access paper)
Mitchell, T.D., Hulme, M. and New, M., 2002: Climate data for political areas. Area 34, 109-112
WHOLE PAPER AS PDF
Mitchell, T. D., et al, 2003: A comprehensive set of climate scenarios for Europe and the globe. Tyndall Centre Working Paper 55.
ABSTRACT AND WHOLE PAPER
New, M., Hulme, M. and Jones, P.D., 1999: Representing twentieth century space-time climate variability. Part 1: development of a 1961-90 mean monthly terrestrial climatology. Journal of Climate 12, 829-856
doi:10.1175/1520-0442(1999)012<0829:RTCSTC>2.0.CO;2 (click doi to access paper)
New, M., Hulme, M. and Jones, P.D., 2000: Representing twentieth century space-time climate variability. Part 2: development of 1901-96 monthly grids of terrestrial surface climate. Journal of Climate 13, 2217-2238
doi:10.1175/1520-0442(2000)013<2217:RTCSTC>2.0.CO;2 (click doi to access paper)
New, M., Lister, D., Hulme, M. and Makin, I., 2002: A high-resolution data set of surface climate over global land areas. Climate Research 21
doi:10.3354/cr021001 (click doi to access paper)


Data Formats
Early datasets used a packed format which minimised storage requirements, known as 'GRIM'. This presented all the data for each gridcell in turn. The advantage was that the missing data (oceans, Antarctica) was simply not included. The disadvantage was that it required the user to reassemble the grids at the other end: a task carrying implications for storage and processing. CRU TS 2.10 was the last version to be produced in this format; it has since been converted to ASCII gridded format (below).

Current gridded products (CRU TS) are presented either as ASCII grids, or in NetCDF format. The ASCII grids are 720 columns wide and 360 rows 'deep', with subsequent monthly grids concatenated onto earlier ones.



Variables: abbreviations, definitions, units
label	variable	units
cld	cloud cover	percentage (%)
dtr	diurnal temperature range	degrees Celsius
frs	frost day frequency	days
pet	potential evapotranspiration	millimetres per day
pre	precipitation	millimetres per month
rhm	relative humidity	percentage (%)
ssh	sunshine duration	hours
tmp	daily mean temperature	degrees Celsius
tmn	monthly average daily minimum temperature	degrees Celsius
tmx	monthly average daily maximum temperature	degrees Celsius
vap	vapour pressure	hectopascals (hPa)
wet	wet day frequency	days
wnd	wind speed	metres per second (m/s)


Coverage
All datasets on this page cover land surfaces only, with global datasets additionally excluding Antarctica.



Naming Conventions
The identifying label is made up of:

Institution of origin

CRU = Climatic Research Unit

TYN = Tyndall Centre for Climate Change Research

both at the University of East Anglia (UK)

Label denoting the type of data-set

CL = The average climate in the recent past. These are high-resolution grids. Allows spatial comparisons of environmental features; the dependency of certain features on mean climate may be assessed.

TS = Month-by-month variations in weather/climate over the last century or so. These are high-resolution grids. Allows variations in weather/climate to be compared with variations in other phenomena.

SC = A set of scenarios of possible climates in the future, using data from climate models. These are high-resolution grids. Allows environmental impact models to consider month-by-month changes in climate from the past, through the present, and into the future. Future uncertainties are represented through scenarios.

CY = Country averages, based on aggregating grid cells together from the grids in CL, TS, or SC (usually TS). Allows international comparisons to be made, in conjunction with socio-economic data such as GDP, population, land area, etc.


Version number
