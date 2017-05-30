from ParseData import ParseData as pd
import json

if __name__ == '__main__':
	# already done: MS17010, DoublePulsar, WannaCry
	# partly , 
    paths = ['data/EternalBlue1.json']
    for path in paths:
	    with open(path, 'r') as file:
	        data = json.load(file)
	    size = len(data)
	    i = 1
	    for item in data:
	    	progress = '{0:.2f}'.format(i * 100.0 / size) + '%'
	    	pd.parseData(item, progress)
	    	i += 1

	    print ''
	    print "Complete!"