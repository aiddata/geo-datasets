# restructure data with unnecessary month/year folders for monthly or yearly datasets

# !!!
# comment out all but one section of code 
# and change path 
# if using this script directly
# !!!

# path to parent of year directory for dataset
path="/home/userz/Desktop/testz3"


# 1) monthly data - convert data with file structure data/year/month/file.ext to data/year/file_month.ext  
for i in $path/*;do 
    if [[ -d $i ]]; then
        year=`echo $i | sed 's/.*\///'`
        for j in $i/*;do 
            if [[ -d $j ]]; then
                month=`echo $j | sed 's/.*\///'`
                for k in $j/*;do
                    if [[ -f $k ]]; then
                        fext=`echo $k | sed 's/.*\.//'`
                        fname=`basename $k ."$fext"`
                        mv $k $path/$year/"$fname"_"$month"."$fext"
                    fi
                done
                rm -r $j
            fi
        done
    fi
done


# 2) yearly data - convert data with file structure data/year/file.ext to data/file_year.ext  
for i in $path/*;do 
    if [[ -d $i ]]; then
        year=`echo $i | sed 's/.*\///'`
        for j in $i/*;do 
            if [[ -f $j ]]; then
                fext=`echo $j | sed 's/.*\.//'`
                fname=`basename $j ."$fext"`
                mv $j $path/"$fname"_"$year"."$fext"
            fi
        done
        rm -r $i
    fi
done


# 3) yearly data - convert data with file structure data/year/00/file.ext to data/file_year.ext  
for i in $path/*;do 
    if [[ -d $i ]]; then
        year=`echo $i | sed 's/.*\///'`
        if [[ -d $i/"00" ]]; then
            for j in $i/"00"/*;do 
                if [[ -f $j ]]; then
                    fext=`echo $j | sed 's/.*\.//'`
                    fname=`basename $j ."$fext"`
                    mv $j $path/"$fname"_"$year"."$fext"
                fi
            done
        rm -r $i
        fi
    fi
done

