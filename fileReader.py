import json

if __name__ == '__main__':
    path = 'data/ShadowBrokers.json'

    with open(path, 'r') as file:
        data = json.load(file)

    print json.dumps(data[0], indent=4)

    # s = '''dadf'dsd"asd"fdsa"s'''
    # s = s.replace('"', "'")
    # print s
    # i = 0
    # i += 1
    # v = str(i*1.0/5) + '%'
    # print v