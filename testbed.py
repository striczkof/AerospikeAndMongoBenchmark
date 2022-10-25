import pymongo
import aerospike

"""client = aerospike.client({'hosts': [('192.168.0.3', 3002)]})
client.put('test', 'test2', {'name': 'test'})
print(client)
client.close()"""
key_counter = 10000

for i in range(key_counter + 1, 250 + 1):
    print(i)
key_counter = key_counter + 250
print(key_counter)