
# http://sac.csic.es/spei/database.html
# http://digital.csic.es/handle/10261/128892

# download format
#   http://digital.csic.es/bitstream/10261/128892/<1>/spei>2>.nc
# where
#   <2> is 2 char str of int from 1-48 (leading zero when needed)
#   <1> is int of <2> +2 (eg, when <2> is 03, <1> is 5)

dst_dir='/sciclone/aiddata10/REU/raw/speidatabase'
mkdir -p $dst_dir

for i in {1..48}; do

    a=$((i+2))
    printf -v b "%02d" $i

    f=http://digital.csic.es/bitstream/10261/128892/"$a"/spei"$b".nc
    echo $f

    wget -c -N -P $dst_dir $f

done


