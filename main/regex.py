from pyspark import SparkContext
import re
import sys
import numpy as np
# from const import _Const
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.sql.types import *
from pyspark.sql import Row

# ------------- using constant --------------- 


def constant(f):
    def fset(self, value):
        raise TypeError
    def fget(self):
        return f()
    return property(fget, fset)

class _Const(object):
    '''
    FEATURE_NUM: The number of regex features.
    EQUAL_COMPARE_FLAG: True if two columns must have same size
    '''
    @constant
    def FEATURE_NUM():
        return 7
    @constant
    def EQUAL_COMPARE_FLAG():
        return True

CONST = _Const()

# ------------- Load all the columns we need --------------
file_list = ['25th-nujf.json',\
    '2cmn-uidm.json','2xir-kwzz.json','2xh6-psuq.json','39g5-gbp3.json',\
    '3mrr-8h5c.json',\
    '27h8-t3wt.json','28rh-vpvr.json',\
    '2fws-68t6.json','2q48-ip9a.json','2zbg-i8fx.json',\
    '32y8-s55c.json','33c5-b922.json', \
    'zkky-n5j3.json','yu9n-iqyk.json','yjub-udmw.json', \
    'yhuu-4pt3.json','x5tk-fa54.json']


# ------------- Regular Expression Matching --------------- 
# ------------- and convert them to vectors. --------------- 

'''
Modify the feature in the vector:
    1. Modify the const FEATURE_NUM in const.py
    2. Add the regular expression in the match_regex function
'''

# Input : an column item
# Output : an vector with only digit is 1
#          type: DenseVector
def match_regex(item):
    # Initialize the return vector
    v = [0] * CONST.FEATURE_NUM
    if item is None:
        return Vectors.dense(v)
    item = str(item)
    item = item.strip()

    v_index = -1
#    if item is None:
#        return Vectors.dense(v)

    state = '^.*?(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New[ ]Hampshire|New[ ]Jersey|New[ ]Mexico|New[ ]York|North[ ]Carolina|North[ ]Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode[ ]Island|South[ ]Carolina|South[ ]Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West[ ]Virginia|Wisconsin|Wyoming).*?$'
    state_abbr = '^.*?(AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PW|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY).*?$'
    post_code = '^.*?\b\d{5}(?:-\d{4})?\b.*?$'
    street = '^.*?\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?.*?$'
    pattern_phone = re.compile('^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$')
    pattern_email = re.compile('^[a-z]([a-z0-9]*[-_]?[a-z0-9]+)*@([a-z0-9]*[-_]?[a-z0-9]+)+[\.][a-z]{2,3}([\.][a-z]{2})?$')
    pattern_url = re.compile('((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*')
    # need to modify, currently same as ip pattern
    pattern_html_tag = re.compile('(\d{3}\.){1,3}\d{1,3}\.\d{1,3}')
    pattern_ip = re.compile('<[^>]+>')
    pattern_hex = re.compile('(0)?x[0-9a-fA-F]+')
    pattern_base62 = re.compile('^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$')
    pattern_state = re.compile(state)
    pattern_state_abbr = re.compile(state_abbr)
    pattern_post_code = re.compile(post_code)
    pattern_street = re.compile(street)
    if pattern_email.match(item) != None:
        v[0] = 1
        # return Vectors.dense(v)
    if pattern_url.match(item) != None:
        v[1] = 1
        # return Vectors.dense(v)
    if (pattern_state.match(item) != None or pattern_state_abbr.match(item) != None) or pattern_post_code.match(item) != None or pattern_street.match(item) != None:
        v[2] = 1
        # return Vectors.dense(v)
    if pattern_html_tag.match(item) != None:
        v[3] = 1
        # return Vectors.dense(v)
    if pattern_ip.match(item) != None:
        v[4] = 1
        # return Vectors.dense(v)
    if pattern_phone.match(item) != None:
        v[5] = 1
        # return Vectors.dense(v)
    if pattern_base62.match(item) != None:
        v[6] = 1
        # return Vectors.dense(v)
    if pattern_hex.match(item) != None:
        v[7] = 1
    return Vectors.dense(v)


def main(argv):
    spark = SparkSession.builder \
                    .appName('regex') \
                    .config("spark.some.config.option", "some-value") \
                    .getOrCreate()
    sc = spark.sparkContext
    # TODO:// convert the command arg to jsonfile names
    #         , actually the table identifiers
    # Following are three dummy hard coded json files
    # NOTICE: all json files must be loaded into hdfs in advance
    # file_list = ["./22rf-yxcy.json", "./22zm-qrtq.json", "./23rb-xz43.json"]
    vector_list = []
    for json_file in file_list:
        df = spark.read.json(json_file)
        col_names = df.columns
        col_size = df.select(col_names[0]).rdd.flatMap(lambda x: x).count()
        for col_name in col_names:
            col = df.select(col_name).rdd.flatMap(lambda x: x)
            col = col.map(match_regex).reduce(lambda x, y: x + y)
            col_vec = col/col_size # col is a DenseVector now
            col_vec = list(col_vec)
            # Transform the vector of np.float64 to native python type
            for c in range(len(col_vec)):
                col_vec[c] = col_vec[c].item()
            t = (json_file, col_name, col_vec)
            vector_list.append(t)
    # now use the list of tuples to create final dataframe
    schema = StructType([
        StructField("FileName", StringType(), True),
        StructField("Column", StringType(), True),
        StructField("Vector", ArrayType(FloatType()), True)])
    rdd = sc.parallelize(vector_list).map(lambda x: Row(x[0], x[1], x[2]))
    result_df = spark.createDataFrame(rdd, schema)
    result_df.write.parquet("./regex_table.parquet")

if __name__ == '__main__':
    main(sys.argv)


