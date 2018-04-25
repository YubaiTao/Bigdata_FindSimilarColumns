#!/bin/bash

# input: The json file name

dir=/user/bigdata/nyc_open_data/
path=${dir}${1}
localfile=./${1}
outjsonfile=${1%%.json}_.json

# download the original json file to local
/usr/bin/hadoop fs -get $path

echo "hfs get success."

# generate formal read-friendly json from downloaded original json
python3 ./extractjson.py $localfile

# upload the local json to HDFS
/usr/bin/hadoop fs -put $outjsonfile

# rename the jsonfile to normal
/usr/bin/hadoop fs -mv $outjsonfile $1

echo "hfs put success."


if [ ! -d ./tmp ];
then
    mkdir tmp
    echo "create tmp folder."
fi

mv $1 ./tmp
mv $outjsonfile ./tmp
echo "intermediate data is moved to tmp."

echo "data is all served."
