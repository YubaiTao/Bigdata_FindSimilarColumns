## Load script for Bigdata project

### Usage:

#### View dataset information:
// Todo

***

#### Load the chosen dataset:
All dataset json file names are in ```jsonfiles.txt```, here we only use the file name. Command: ```./load.sh [json_file_name]```, for instance, for the first json file, the command is ```./load.sh 22rf-yxcy.json```.

This will generate a well-formatted json file in your HDFS path. You can check it by ```hfs -ls```. The new generated json file name is identical to the one in the command.

If nothing went wrong, all printed messages should contain no error. However, for unknown reasons, few original files cannot be loaded. In this case, just abandon this dataset.

In pyspark, you can use ```df = spark.read.json([json_file_name])``` to read the data as a DataFrame. Use ```df.printSchema()``` to see the schema of the dataframe.

If you want to perform some local operation on the dataset, the json files are in ```tmp``` folder. Delete it will influence nothing. The file with ```_``` is the json lines of rows, the other one is the original downloaded from ```/user/bigdata/nyc_open_data/```.
