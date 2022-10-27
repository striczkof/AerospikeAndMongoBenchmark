# by Alvin, made within 3 hour lmao
import random
import time

import aerospike
from aerospike_helpers.operations import operations as op

# Database name constant
DB_NAME = 'Aerospike'
# Namespace or collection name constant
NS_COL_NAME = 'test'
# Number of times to perform multiple CRUD operations
NUM_ITERATIONS = 100
# Number of times mass CRUD operations will be performed
MASS_CRUD_ITERATIONS = 250
# Host IP address
HOST = '192.168.0.3'
# Port number
PORT = 3000
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
    client.truncate(NS_COL_NAME, None, 0)
    client.close()
    exit(0)

# Assistant function to preload random bulk keys
def generate_bulk_keys() -> list:
    bulk_keys = []
    for _i in range(MASS_CRUD_ITERATIONS):
        bulk_keys.append((NS_COL_NAME, 'key', random.randint(1, key_counter)))
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
        bulk_keys.append((NS_COL_NAME, 'key', i))
    return bulk_keys

# All the functions must return the elapsed time in microseconds

# Database connection function
def connect() -> float:
    global client
    start_time = time.time()
    # Database specific code starts here
    client = aerospike.client({'hosts': [(HOST, PORT)]}).connect()
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

# Clean up and close function
def wipe_close() -> float:
    global client
    start_time = time.time()
    # Database specific code starts here
    client.truncate(NS_COL_NAME, None, 0)
    client.close()
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

# CRUD operation functions start here
def create(is_mass: bool) -> float:
    global key_counter
    # Preloading bulk keys
    if is_mass:
        bulk_keys = generate_bulk_keys_from_current(key_counter, True)
        # Prepare batch operations and policy
        batch_operations = [op.write('name', 'John Doe'), op.write('num', random.randint(0, 1000000))]
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass create
        client.batch_operate(bulk_keys, batch_operations)
        key_counter = key_counter + MASS_CRUD_ITERATIONS
    else: # Single create
        key_counter = key_counter + 1
        client.put((NS_COL_NAME, 'key', key_counter), {'name': 'test_name', 'number': random.randint(0, 1000000)})

    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def read(is_mass: bool) -> float:
    if is_mass:
        # Preloading bulk keys
        bulk_keys = generate_bulk_keys()
    else:
        # Preload random key
        random_key = (NS_COL_NAME, 'key', random.randint(1, key_counter))
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass read, literally just read everything
        client.get_many(bulk_keys)
    else: # Single read, read a single key from 1 to key_counter
        client.get(random_key)
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def update(is_mass: bool) -> float:
    # Preloading bulk keys
    if is_mass:
        bulk_keys = generate_bulk_keys()
        # Prepare batch operations and policy
        batch_operations = [op.write('name', 'John Doe'), op.write('num', random.randint(0, 1000000))]
    else:
        # Preload random key
        random_key = (NS_COL_NAME, 'key', random.randint(1, key_counter))
    # Preloading write policy
    update_policy = {'exists': aerospike.POLICY_EXISTS_UPDATE}
    global client
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass update, literally just update everything
        client.batch_operate(bulk_keys, batch_operations, update_policy)
    else: # Single update, find a random key from 1 to key_counter and update it
        client.put(random_key, {'number': random.randint(0, 1000000)})
    # Database specific code ends here
    return 1000000 * (time.time() - start_time)

def delete(is_mass: bool) -> float:
    global key_counter
    # Preloading bulk keys from key_counter - (MASS_CRUD_ITERATIONS - 1) to key_counter
    if is_mass:
        bulk_keys = generate_bulk_keys_from_current(key_counter, False)
    global client
    start_time = time.time()
    # Database specific code starts here
    if is_mass: # Mass delete, delete key_counter to key_counter - MASS_CRUD_ITERATIONS
        client.batch_remove(bulk_keys)
        key_counter = key_counter - MASS_CRUD_ITERATIONS
        pass
    else: # Single delete last key
        client.remove((NS_COL_NAME, 'key', key_counter))
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
    print(f'Current key counter: {key_counter}')
    print('Performing mass CRUD operations...')
    print('Creating...')
    create_mass_times = []
    for _i in range(MASS_CRUD_ITERATIONS):
        create_mass_times.append(create(True))
    print('Reading...')
    read_mass_times = []
    for _i in range(MASS_CRUD_ITERATIONS):
        read_mass_times.append(read(True))
    print('Updating...')
    update_mass_times = []
    for _i in range(MASS_CRUD_ITERATIONS):
        update_mass_times.append(update(True))
    print('Deleting...')
    delete_mass_times = []
    for _i in range(MASS_CRUD_ITERATIONS):
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
    print(f'Average mass read time: {round(sum(read_mass_times) / len(read_mass_times), 5)} microseconds ')
    print(f'Average mass update time: {round(sum(update_mass_times) / len(update_mass_times), 5)} microseconds')
    print(f'Average mass delete time: {round(sum(delete_mass_times) / len(delete_mass_times), 5)} microseconds')
    print('Done!')

if __name__ == '__main__':
    main()