import numpy as np
from time import time

from utils import test_time

@test_time
def prepare_for_zset(data_json):
  zset_data = {}
  for key, value in data_json.items():
    zset_data[str(value)] = key
  
  return zset_data

@test_time
def load_zset_chunk(db, zset_name, zset_data):
  db.zadd(zset_name, zset_data)

@test_time
def load_zset_rows(db, zset_name, zset_data):
  for key, value in zset_data.items():
    db.zadd(zset_name, {key: value})

@test_time
def del_zset_chunk(db, zset_name):
  db.delete(zset_name)

def test_simple_zrem(db, zset_name, data_json):
  print('--- Remove one element using zrem ---')
  to_remove = np.random.randint(0, db.zcard(zset_name), size=5)
  for idx in to_remove:
    idx = str(idx)
    key_to_rem = str(data_json[idx])
    start = time()
    db.zrem(zset_name, key_to_rem)
    stop = time()
    stop_start = round(stop - start, 4)
    print(f'Removed element using zrem in {stop_start} seconds')
  
  start = time()
  db.zrem(zset_name, 'test_string')
  stop = time()
  stop_start = round(stop - start, 4)
  print(
    f'Test removal of non-existent elemet \'test_string\' took {stop_start} seconds'
  )

def zremrangebyscore_simple(db, zset_name, min_bound, max_bound):
  start = time()
  db.zremrangebyscore(zset_name, min=min_bound, max=min_bound)
  stop = time()
  stop_start = round(stop - start, 4)
  print(f'Removed from {min_bound} to {max_bound} in {stop_start} seconds')

def test_zrembyscore(db, zset_name):
  print('\n--- Remove elements using zrembyscore ---')
  min_bounds = [0, 10_000, 200_000, 700_000, 700_005]
  max_bounds = [5000, 50_000, 600_000, 700_000, 700_005]
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    zremrangebyscore_simple(db, zset_name, min_bound, max_bound)

def test_zrems(db, zset_name, data_json):
  print('Testing different removal approaches...')
  test_simple_zrem(db, zset_name, data_json)
  test_zrembyscore(db, zset_name)

def test_zrange_simple(db, zset_name, min_bound, max_bound):
  start = time()
  zset_read_result = db.zrange(zset_name, min_bound, max_bound)
  stop = time()
  stop_start = round(stop - start, 4)
  print(
    f'Got elements from {min_bound} to {max_bound} in {stop_start} seconds (by insertion order)'
  )
  
  return zset_read_result

def test_zrange(db, zset_name):
  print('--- Test zrange ---')
  min_bounds = [0, 10, 100, 1000, 100_000]
  max_bounds = [-1, 20, 200, 4000, 200_000]
  zranges = []
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    zrange = test_zrange_simple(db, zset_name, min_bound, max_bound)
    zranges.append(zrange)
  
  return zranges

def test_zrangebyscore_simple(db, zset_name, min_bound, max_bound):
  start = time()
  zset_read_result = db.zrangebyscore(zset_name, min=min_bound, max=max_bound)
  stop = time()
  stop_start = round(stop - start, 4)
  print(
    f'Got elements from {min_bound} to {max_bound} in {stop_start} seconds (by score)'
  )
  
  return zset_read_result

def test_zrangebyscore(db, zset_name):
  print('\n--- Test zrangebyscore ---')
  min_bounds = [0, 10, 500, 13_000, 300_000]
  max_bounds = [db.zcard(zset_name), 20, 600, 14_000, 400_000]
  zrangesbyscore = []
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    zset_read_reslt = test_zrangebyscore_simple(db, zset_name, min_bound, max_bound)
    zrangesbyscore.append(zset_read_reslt)
  
  return zrangesbyscore

def test_getters(db, zset_name):
  print('Tesing different get approaches...')
  zranges = test_zrange(db, zset_name)
  zrangesbyscore = test_zrangebyscore(db, zset_name)
  
  return zranges, zrangesbyscore

def test_zcount_simple(db, zset_name, min_bound, max_bound):
  start = time()
  zcount_result = db.zcount(zset_name, min_bound, max_bound)
  stop = time()
  stop_start = round(stop - start, 4)
  print(
    f'Counted elements from {min_bound} to {max_bound} in {stop_start} seconds (by score)'
  )
  
  return zcount_result

def test_zcount(db, zset_name):
  print('--- Testing zcount ---')
  counts = []
  min_bounds = [0, 10, 600, 7500, 60_000, 250_000]
  max_bounds = [db.zcard(zset_name), 20, 700, 8500, 70_000, 350_000]
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    zcount_result = test_zcount_simple(db, zset_name, min_bound, max_bound)
    counts.append(zcount_result)
  
  return counts

def test_zcard_zcount(db, zset_name):
  print('--- Test zcard and zcount ---')
  start = time()
  set_cardinality = db.zcard(zset_name)
  print(f'Sorted set cardinality is: {set_cardinality}')
  stop = time()
  stop_start = round(stop - start, 4)
  print(f'Zcard executes in {stop_start} seconds\n')
  
  counts = test_zcount(db, zset_name)
  print('Count results: ', counts)

def zset_tests(db, zset_name, zset_data, data_json):
  print('----- Tests for sorted set -----')
  load_zset_chunk(db, zset_name, zset_data)
  del_zset_chunk(db, zset_name)

  load_zset_rows(db, zset_name, zset_data)
  print('\n\n')
  test_zrems(db, zset_name, data_json)
  db.delete(zset_name)

  print('\n\n')
  db.zadd(zset_name, zset_data)
  zranges, zrangesbyscore = test_getters(db, zset_name)
  del zranges
  del zrangesbyscore

  print('\n\n')
  test_zcard_zcount(db, zset_name)
  
  db.delete(zset_name)
  print('\n\n')
