# by Alvin, made within 3 hour lmao
import random
import pymongo
import time

# Database name constant
DB_NAME = 'MongoDB'
# Namespace or collection name constant
NS_COL_NAME = 'test'
# Number of times to perform multiple CRUD operations
NUM_ITERATIONS = 100
# Number of times mass CRUD operations will be performed
MASS_CRUD_ITERATIONS = 500
# Host IP address
HOST = '192.168.0.3'
# Port number
PORT = '27017'
# Global client variable
client = None
# Global key counter variable
key_counter = 0

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
    # Preloading bulk data
    if is_mass:
        bulk_data = []
        for i in range(1, MASS_CRUD_ITERATIONS + 1):
            bulk_data.append({'key': i, 'name': 'testName', 'number': random.randint(0, 1000000)})
    global key_counter
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
    global key_counter
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass read, literally just read everything
        pass
    else: # Single read, read a single key from 1 to key_counter
        pass
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def update(is_mass: bool) -> float:
    global key_counter
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass update, literally just update everything
        pass
    else: # Single update, find a random key from 1 to key_counter and update it
        pass
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def delete(is_mass: bool) -> float:
    global key_counter
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass delete, delete key_counter to key_counter - MASS_CRUD_ITERATIONS
        for i in reversed(range(key_counter - MASS_CRUD_ITERATIONS + 1, key_counter + 1)):
            pass
    else: # Single delete last key
        pass
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