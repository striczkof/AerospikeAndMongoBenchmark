import pymongo
import aerospike

"""client = aerospike.client({'hosts': [('192.168.0.3', 3002)]})
client.put('test', 'test2', {'name': 'test'})
print(client)
client.close()"""

host = '192.168.0.3'
port = '27017'

client = pymongo.MongoClient('mongodb://' + host + ':' + port)
print(client)
db = client['test']
col = db['test2']
client['test']['test2'].insert_one({'name': 'test', 'number': 1})
client['test']['test2'].drop()
client.close()