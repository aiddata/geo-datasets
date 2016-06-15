branch=$1
version=$2

src="${HOME}"/active/"$branch"

version_dir='gadm'${version}

data_dir='/sciclone/aiddata10/REU/data/boundaries_test/'${version_dir}

if [ ! -d $data_dir ]; then
    echo 'Could not find download directory for GADM version' ${version}
    exit 1
fi


for i in $data_dir/*/*.geojson; do

    name=$(basename ${i} .geojson)

    bnd_dir=$data_dir/$name

    abs_path=$(readlink -f ${bnd_dir})

    print $i
    print $abs_path
    # add to asdf
    echo 'pxython "${src}"/asdf/src/add_gadm.py ${branch} ${abs_path} ${version} auto'

done

# useful for clearing shapefiles if ingest was partially run before prep was run again
# for i in *; do if [ -f ${i}/simplified.geojson ]; then find ${i} -not -name *.geojson -type f -delete; fi; done
