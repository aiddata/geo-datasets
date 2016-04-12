branch=$1
version=$2

src="${HOME}"/active/"$branch"

version_dir='gadm'${version}

data_dir='/sciclone/aiddata10/REU/data/boundaries/'${version_dir}

if [ ! -d $data_dir ]; then
    echo 'Could not find download directory for GADM version' ${version}
    exit 1
fi


for i in $data_dir/*/*_adm*.geojson; do

    name=$(basename ${i} .geojson)

    bnd_dir=$data_dir/$name

    abs_path=$(readlink -f ${bnd_dir})

    # add to asdf
    python "${src}"/asdf-datasets/data_prep/gadm/fix_gadm.py ${branch} ${abs_path} ${version}
done

