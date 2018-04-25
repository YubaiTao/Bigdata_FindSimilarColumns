## Load script for Bigdata project

#### Usage:
All dataset json file names are in ```jsonfiles.txt```, here we only use the file name. Command: ```./load.sh [json_file_name]```, for instance, for the first json file, the command is ```./load.sh 22rf-yxcy.json```.

This will generate a well-formatted json file in your HDFS path. You can check it by ```hfs -ls```. The new generated json file name is identical to the one in the command.

In pyspark, you can use ```df = spark.read.json([json_file_name])``` to read the data as a DataFrame. Use ```df.printSchema()``` to see the schema of the dataset.

