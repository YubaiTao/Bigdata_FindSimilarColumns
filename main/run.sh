#!/bin/bash

inputfile=$1
# outputfile=${1%%.json}.out}
outputfile=regex_table.parquet
# outputfile2=regex_table.csv

# /usr/bin/hadoop fs -rm -r $outputfile
# rm $outputfile

/usr/bin/hadoop fs -rm -r $outputfile
# /usr/bin/hadoop fs -rm -r $outputfile2

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python $inputfile #$outputfile



# /usr/bin/hadoop fs -getmerge $1


# spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python $fileName /user/ecc290/HW1data/parking-violations-header.csv


