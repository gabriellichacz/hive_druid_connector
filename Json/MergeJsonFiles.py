import json

files=[
    '..\dane\hive_connector\data\data_1mln.json',
    '..\data\data_1mln.json'
    ]

def merge_JsonFiles(filename):
    result = list()
    for f1 in filename:
        with open(f1, 'r') as infile:
            result.extend(json.load(infile))

    with open('..\data\data_5mln_test.json', 'w') as output_file:
        json.dump(result, output_file)

merge_JsonFiles(files)