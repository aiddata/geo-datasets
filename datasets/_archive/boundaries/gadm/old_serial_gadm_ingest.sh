branch=$1
version=$2

src="${HOME}"/active/"$branch"

version_dir='gadm'${version}

data_dir='/sciclone/aiddata10/REU/data/boundaries/'${version_dir}

if [ ! -d $data_dir ]; then
    echo 'Could not find download directory for GADM version' ${version}
    exit 1
fi


for i in $data_dir/*/*.geojson; do

    abs_dir=$(dirname $i)

    # add to asdf
    python "${src}"/asdf/src/add_gadm.py ${branch} ${abs_dir} auto full

done

# useful for clearing shapefiles if ingest was partially run before prep was run again
# for i in *; do if [ -f ${i}/simplified.geojson ]; then find ${i} -not -name *.geojson -type f -delete; fi; done
