#!/bin/bash

# Download and unzip GPW data

# -----------------------------------------------------------------------------
# variables to be edited by the user

# sedac cookie value acquired using steps documented in readme
sedac="ADD YOUR SEDAC COOKIE VALUE HERE"

# directory to download files to
dl_dir="/sciclone/aiddata10/REU/geo/raw/gpw/gpw_v4_rev11"

# directory to unzip files to
final_dir="/sciclone/aiddata10/REU/geo/data/rasters/gpw/gpw_v4_rev11"

# if you already downloaded files and just want to unzip them, set this to true
only_unzip=false

dl_args=""
if [ "$only_unzip" = true ]; then
    dl_args="-q --spider"
fi

# -----------------------------------------------------------------------------


# download documentation
wget --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${dst} https://sedac.ciesin.columbia.edu/downloads/docs/gpw-v4/gpw-v4-documentation-rev11.zip


# path to download population density files to
density_dl_dir=${dl_dir}/"density"
density_final_dir=${final_dir}/"density"

mkdir -p ${density_final_dir}

density_base_url="https://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11"


dl_path=${density_base_url}/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2000_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${density_dl_dir} ${dl_path}
unzip ${density_dl_dir}/`basename ${dl_path}` "*.tif" -d ${density_final_dir}

dl_path=${density_base_url}//gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2005_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${density_dl_dir} ${dl_path}
unzip ${density_dl_dir}/`basename ${dl_path}` "*.tif" -d ${density_final_dir}

dl_path=${density_base_url}//gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2010_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${density_dl_dir} ${dl_path}
unzip ${density_dl_dir}/`basename ${dl_path}` "*.tif" -d ${density_final_dir}

dl_path=${density_base_url}//gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2015_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${density_dl_dir} ${dl_path}
unzip ${density_dl_dir}/`basename ${dl_path}` "*.tif" -d ${density_final_dir}

dl_path=${density_base_url}//gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2020_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${density_dl_dir} ${dl_path}
unzip ${density_dl_dir}/`basename ${dl_path}` "*.tif" -d ${density_final_dir}



# path to download population count files to
count_dl_dir=${dl_dir}/"count"
count_final_dir=${final_dir}/"count"

mkdir -p ${count_final_dir}

count_base_url="https://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11"


dl_path=${count_base_url}/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11_2000_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${count_dl_dir} ${dl_path}
unzip ${count_dl_dir}/`basename ${dl_path}` "*.tif" -d ${count_final_dir}

dl_path=${count_base_url}/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11_2005_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${count_dl_dir} ${dl_path}
unzip ${count_dl_dir}/`basename ${dl_path}` "*.tif" -d ${count_final_dir}

dl_path=${count_base_url}/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11_2010_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${count_dl_dir} ${dl_path}
unzip ${count_dl_dir}/`basename ${dl_path}` "*.tif" -d ${count_final_dir}

dl_path=${count_base_url}/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11_2015_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${count_dl_dir} ${dl_path}
unzip ${count_dl_dir}/`basename ${dl_path}` "*.tif" -d ${count_final_dir}

dl_path=${count_base_url}/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals-rev11_2020_30_sec_tif.zip
wget ${dl_args} --no-check-certificate --header "Cookie: sedac=${sedac}" -P ${count_dl_dir} ${dl_path}
unzip ${count_dl_dir}/`basename ${dl_path}` "*.tif" -d ${count_final_dir}


