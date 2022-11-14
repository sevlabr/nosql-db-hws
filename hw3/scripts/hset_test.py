import numpy as np
from time import time

from utils import test_time

@test_time
def set_hset_rows(db, data_json):
  for idx in range(int(len(data_json) / 100)):
  # for idx in range(len(data_json)):
    idx = str(idx)
    data_row = data_json[idx]
    key = data_row['Date/Time']
    fields = ['Lat', 'Lon', 'Base']
    values = [data_row[field] for field in fields]
    for field, value in zip(fields, values):
      db.hset(key, field, value)

@test_time      
def set_single_hset(db, hset_name, data_json):
  for idx in range(int(len(data_json) / 100)):
  # for idx in range(len(data_json)):
    db.hset('rides', idx, str(data_json[str(idx)]))

def test_simple_single_hget(db, hset_name, min_bound, max_bound):
  results = []
  start = time()
  for i_key in range(min_bound, max_bound):
    results.append(
      db.hget(hset_name, i_key)
    )
  stop = time()
  stop_start = round(stop - start, 4)
  print(
    f'Got elements from {min_bound} to {max_bound} in {stop_start} seconds'
  )
  
  return results

def test_single_hget(db, hset_name):
  print('--- Testing HGet on single HSet ---')
  results = []
  min_bounds = [0, 50, 500, 3000]
  max_bounds = [10, 100, 1000, 4000]
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    results.append(
      test_simple_single_hget(db, hset_name, min_bound, max_bound)
    )
  
  return results

def test_single_hgetall(db, hset_name):
  print('\n--- Test HGetAll on a single set ---')
  times = np.zeros(10)
  results = []
  for i in range(10):
    start = time()
    results.append(db.hgetall(hset_name))
    stop = time()
    times[i] = stop - start
  
  avg_time = round(np.mean(times), 4)
  print(
    f'Average time for HGetAll ({db.hlen(hset_name)} elements) is {avg_time} seconds'
  )
  
  return results

def test_hdel_range(db, hset_name, min_bound, max_bound):
  start = time()
  for i_key in range(min_bound, max_bound):
    db.hdel(hset_name, i_key)
  stop = time()
  del_time = round(stop - start, 4)
  print(
    f'Deleted HSet elements with keys from {min_bound} to {max_bound} in {del_time} seconds'
  )

def test_single_hdel(db, hset_name):
  min_bounds = [0, 50, 500, 3000]
  max_bounds = [10, 100, 1000, 4000]
  for min_bound, max_bound in zip(min_bounds, max_bounds):
    test_hdel_range(db, hset_name, min_bound, max_bound)

def test_del_hset_rows(db):
  start = time()
  db.flushall()
  stop = time()
  flush_time = round(stop - start, 4)
  print(f'\nDeleted all HSet rows in {flush_time} seconds')

def hset_tests(db, hset_name, data_json):
  print('----- Tests for HashSet -----')
  print('--- Test adding elements to a single HSet ---')
  set_single_hset(db, hset_name, data_json) # 2m 12.4 s; 56 MB
  print(f'Number of added elements: {db.hlen(hset_name)}\n')
  print('--- Test getters from a single HSet ---')
  single_get_res = test_single_hget(db, hset_name)
  single_getall_res = test_single_hgetall(db, hset_name)
  del single_get_res
  del single_getall_res
  print('\n--- Test removing from a single HSet ---')
  test_single_hdel(db, hset_name)
  db.flushall()
  
  print('\n\n--- Test adding elements to multiple HSets ---')
  set_hset_rows(db, data_json) # 6m 24.4 s; 56 MB
  test_del_hset_rows(db)
  print('\n\n')
