import json

if __name__ == '__main__':
    path = 'data/ShadowBrokers.json'

    with open(path, 'r') as file:
        data = json.load(file)

    print json.dumps(data[0], indent=4)