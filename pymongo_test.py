# by Alvin, made within 3 hour lmao
import random
import time

import pymongo

# Database name constant
DB_NAME = 'MongoDB'
# Namespace or collection name constant
NS_COL_NAME = 'test'
# Number of times to perform multiple CRUD operations
NUM_ITERATIONS = 100
# Number of times mass CRUD operations will be performed
MASS_CRUD_ITERATIONS = 250
# Host IP address
HOST = '192.168.0.3'
# Port number
PORT = '27017'
# Global client variable
client = None
# Global key counter variable
key_counter = 0

# In case of emergency, set this to True
# DID IT DIE? PURGE EVERYTHING, STUPID CODER!
DIED = False
# DIED = True

def purge() -> float:
    global client
    print('Purging everything...')
    client['db'][NS_COL_NAME].drop()
    client.close()
    exit(0)

# Assistant function to preload random bulk keys
def generate_bulk_keys() -> list:
    bulk_keys = []
    for _i in range(MASS_CRUD_ITERATIONS):
        bulk_keys.append((1, key_counter))
    return bulk_keys
    
# Assistant function to generate ordered bulk keys
def generate_bulk_keys_from_current(current_key: int, forward: bool) -> list:
    bulk_keys = []
    if forward:
        start = current_key + 1
        end = (current_key + 1) + MASS_CRUD_ITERATIONS
        inc = 1
    else:
        start = (current_key + 1) - MASS_CRUD_ITERATIONS
        end = current_key
        inc = -1
    for i in range(start, end + inc):
        bulk_keys.append(i)
    return bulk_keys

# All the functions must return the elapsed time in microseconds

# Database connection function
def connect() -> float:
    global client
    start_time = time.time()
    # Database specific code starts here
    client = pymongo.MongoClient('mongodb://' + HOST + ':' + PORT)
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

# Clean up and close function
def wipe_close() -> float:
    start_time = time.time()
    # Database specific code starts here
    client['db'][NS_COL_NAME].drop()
    client.close()
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

# CRUD operation functions start here
def create(is_mass: bool) -> float:
    global key_counter
    # Preloading bulk data
    if is_mass:
        bulk_data = []
        for i in range(key_counter + 1, (key_counter + 1) + MASS_CRUD_ITERATIONS):
            bulk_data.append({'key': i, 'name': 'testName', 'number': random.randint(0, 1000000)})
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass create
        client['db'][NS_COL_NAME].insert_many(bulk_data)
        key_counter = key_counter + MASS_CRUD_ITERATIONS
    else: # Single create
        client['db'][NS_COL_NAME].insert_one({'key': key_counter + 1, 'name': 'testName', 'number': random.randint(0, 1000000)})
        key_counter = key_counter + 1

    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def read(is_mass: bool) -> float:
    # Preload key array
    if is_mass:
        bulk_keys = generate_bulk_keys()
    global key_counter
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass read, literally just read everything
        client['db'][NS_COL_NAME].find({"key": {"$in": bulk_keys}})
        pass
    else: # Single read, read a single key from 1 to key_counter
        client['db'][NS_COL_NAME].find({"key": {"$eq": random.randint(1, key_counter)}})
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def update(is_mass: bool) -> float:
    global key_counter
    if is_mass:
        # Preload bulk keys
        bulk_keys = generate_bulk_keys()
    else:
        # Preload random key
        random_key = random.randint(1, key_counter)
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass update, literally just update everything
        client['db'][NS_COL_NAME].update_many({"key": {"$in": bulk_keys}}, {"$set": {"name": "asdas", "number": random.randint(0, 1000000)}})
    else: # Single update, find a random key from 1 to key_counter and update it
        client['db'][NS_COL_NAME].update_one({"key": {"$eq": random_key}}, {"$set": {"name": "testName", "number": random.randint(0, 1000000)}})
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def delete(is_mass: bool) -> float:
    global key_counter
    # Preload key array
    if is_mass:
        bulk_keys = generate_bulk_keys_from_current(key_counter, False)
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass delete, delete key_counter to key_counter - MASS_CRUD_ITERATIONS
        client['db'][NS_COL_NAME].delete_many({"key": {"$in": bulk_keys}})
        key_counter = key_counter - MASS_CRUD_ITERATIONS
    else: # Single delete last key
        client['db'][NS_COL_NAME].delete_one({"key": {"$eq": key_counter}})
        key_counter = key_counter - 1
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

# CRUD operation functions end here

# Main function, duh!
def main():
    print(f'This python script is going to benchmark {DB_NAME}')
    print(f'The CRUD operations will be performed {NUM_ITERATIONS} times each, and the average time will be calculated.')
    print('The connection and clean up operations will be performed only once.')
    print('The results will be printed in microseconds.')
    input('Press Enter to continue...')
    # Here we go!
    print(f'Connecting to {DB_NAME} server...')
    connect_time = connect()
    if DIED:
        purge()
    print(f'Connection time: {connect_time} microseconds')
    print('The namespace or collection should exist already.')
    print('Performing single CRUD operations...')
    print('Creating...')
    create_single_times = []
    for _i in range(NUM_ITERATIONS):
        create_single_times.append(create(False))
    print('Reading...')
    read_single_times = []
    for _i in range(NUM_ITERATIONS):
        read_single_times.append(read(False))
    print('Updating...')
    update_single_times = []
    for _i in range(NUM_ITERATIONS):
        update_single_times.append(update(False))
    print('Deleting...')
    delete_single_times = []
    for _i in range(NUM_ITERATIONS):
        delete_single_times.append(delete(False))
    print('Performing mass CRUD operations...')
    print('Creating...')
    create_mass_times = []
    for _i in range(NUM_ITERATIONS):
        create_mass_times.append(create(True))
    print('Reading...')
    read_mass_times = []
    for _i in range(NUM_ITERATIONS):
        read_mass_times.append(read(True))
    print('Updating...')
    update_mass_times = []
    for _i in range(NUM_ITERATIONS):
        update_mass_times.append(update(True))
    print('Deleting...')
    delete_mass_times = []
    for _i in range(NUM_ITERATIONS):
        delete_mass_times.append(delete(True))
    print('Cleaning up and closing...')
    wipe_close_time = wipe_close()
    # Output results
    print('\n\n\n')
    print(f'Benchmark results for {DB_NAME} with {NUM_ITERATIONS} iterations and mass operations performed for {MASS_CRUD_ITERATIONS} records:')
    print(f'Connection time: {round(connect_time / 1000000, 5)} seconds')
    print(f'Clean up and close time: {round(wipe_close_time / 1000000, 5)} seconds')
    print(f'Average single create time: {round(sum(create_single_times) / len(create_single_times), 5)} microseconds')
    print(f'Average single read time: {round(sum(read_single_times) / len(read_single_times), 5)} microseconds')
    print(f'Average single update time: {round(sum(update_single_times) / len(update_single_times), 5)} microseconds')
    print(f'Average single delete time: {round(sum(delete_single_times) / len(delete_single_times), 5)} microseconds')
    print(f'Average mass create time: {round(sum(create_mass_times) / len(create_mass_times), 5)} microseconds')
    print(f'Average mass read time: {round(sum(read_mass_times) / len(read_mass_times), 5)} microseconds')
    print(f'Average mass update time: {round(sum(update_mass_times) / len(update_mass_times), 5)} microseconds')
    print(f'Average mass delete time: {round(sum(delete_mass_times) / len(delete_mass_times), 5)} microseconds')
    print('Done!')
    
if __name__ == '__main__':
    main()