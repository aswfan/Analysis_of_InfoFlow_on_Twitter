import json
from os import walk

def formatData(item, entities):
	if 'user_mentions' in entities.keys():
		item['user_mentions'] = entities['user_mentions']
	if 'hashtags' in entities.keys():
		item['hashtags'] = entities['hashtags']


if __name__ == '__main__':
	src_path = 'o_data/'
	dst_path = 'data/'
	for (dirpath, dirnames, filenames) in walk(src_path):
		for filename in filenames:
			path = src_path + filename

			with open(path, 'r') as file:
				data = json.load(file)

			# for item in data:
			for item in data:
				if 'entities' in item.keys():
					formatData(item, item['entities'])
				if 'retweeted_status' in item.keys():
					status = item['retweeted_status']
					if 'entities' in status.keys():
						formatData(status, status['entities'])

			path = dst_path + filename
			with open(path, 'w') as file:
				json.dump(data, file)
