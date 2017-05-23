from ParseData import ParseData as pd
import json

if __name__ == '__main__':
	# already done: MS17010, DoublePulsar, 
    path = 'data/WannaCry.json'

    with open(path, 'r') as file:
        data = json.load(file)
    for item in data:
    	pd.parseData(item)

    print ''
    print "Complete!"