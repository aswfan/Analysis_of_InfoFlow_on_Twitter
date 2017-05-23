from ParseData import ParseData as pd
import json

if __name__ == '__main__':
    path = 'preforERD.json'

    with open(path, 'r') as file:
        data = json.load(file)
    # print json.dumps(data['result'], indent=4)
    
    for item in data['result']:
        pd.parseData(item, '')