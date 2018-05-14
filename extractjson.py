import sys
import json

"""
    input: local json file downloaded from HDFS
    output: json lines in one json file (with schema)

    Thus, this new generated json file can be read
    by spark directly.
"""


with open(sys.argv[1], 'r', encoding='utf-8') as f:
    alldata = json.load(f)

def getJsonLine(row, schema):
    dict = {}
    for i, item in enumerate(row):
        dict[schema[i]] = item
    return dict

schema = []
for colmeta in alldata['meta']['view']['columns']:
    schema.append(colmeta['name'])

outputfile = sys.argv[1][:-5] + '_.json'

counter = 0
with open(outputfile, 'w') as fp:
    for row in alldata['data']:
        counter += 1
        json.dump(getJsonLine(row, schema), fp)
        fp.write('\n')
        if counter < 1000:
            print(str(counter) + "\trows converted.")
        if (counter % 1000) == 0:
            print(str(counter) + "\trows converted.")

print("total " + str(counter) + "\trows converted.")

print('json conversion: Done')



