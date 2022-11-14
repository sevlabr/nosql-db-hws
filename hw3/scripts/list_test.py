import numpy as np
from time import time

from utils import test_time

@test_time
def prepare_for_list(data_json):
  list_data = []
  for _, item in data_json.items():
    list_data.append(str(item))
  
  return list_data

@test_time
def test_list_push(db, list_name, list_data):
  db.rpush(list_name, *list_data) # 4.4 s; 69 MB

@test_time
def test_list_pop(db, list_name):
  return db.lpop(list_name, db.llen(list_name))

def test_lindex_simple(db, list_name, index):
  times = np.zeros(100)
  results = []
  for i in range(100):
    start = time()
    results.append(
      db.lindex(list_name, index)
    )
    stop = time()
    times[i] = stop - start
  
  avg_time = round(np.mean(times), 6)
  print(
    f'LIndex on element with index {index} works in {avg_time} seconds'
  )

def test_lindex(db, list_name):
  results = []
  indices = [100, 1000, 10_000, 100_000, 700_000]
  for i in indices:
    results.append(
      test_lindex_simple(db, list_name, i)
    )
  
  return results

def test_simple_lrem(db, list_name, data_json, index):
  start = time()
  db.lrem(list_name, 1, str(data_json[str(index)]))
  stop = time()
  
  lrem_time = round(stop - start, 6)
  print(
    f'Deleted element with index {index} in {lrem_time} seconds'
  )
  
def test_lrem(db, list_name, data_json):
  ranges = []
  range10 = [val for val in range(5)]
  range100 = [val for val in range(100, 105)]
  range1000 = [val for val in range(1000, 1005)]
  range10000 = [val for val in range(10_000, 10_005)]
  range100000 = [val for val in range(100_000, 100_005)]
  for rng in [range10, range100, range1000, range10000, range100000]:
    ranges.extend(rng)
    
  for index in ranges:
    test_simple_lrem(db, list_name, data_json, index)

def test_ltrim_simple(db, list_name, start, stop):
  start_time = time()
  db.ltrim(list_name, start, stop)
  stop_time = time()
  trim_time = round(stop_time - start_time, 6)
  
  print(
    f'Trimmed list to [{start}..{stop}] in {trim_time} seconds'
  )

def test_ltrim(db, list_name):
  trim_ranges = [
    (10, 700_000),
    (20, 100_000),
    (30, 50_000),
    (40, 10_000),
    (50, 1000)
  ]
  
  for start, stop in trim_ranges:
    test_ltrim_simple(db, list_name, start, stop)

def list_tests(db, list_name, list_data, data_json):
  print('----- Tests for LinkedList -----')
  print('--- Pushing elements ---')
  test_list_push(db, list_name, list_data)
  print(f'Pushed {db.llen(list_name)} elements\n')
  
  print('--- Popping elements ---')
  lpop_result = test_list_pop(db, list_name)
  print(f'Poped {len(lpop_result)} elements\n')
  del lpop_result
  
  print('--- Test LIndex ---')
  db.rpush(list_name, *list_data)
  lindex_result = test_lindex(db, list_name)
  print('\n')
  del lindex_result
  
  print('--- Test LRem ---')
  test_lrem(db, list_name, data_json)
  db.flushall()
  print('\n')
  
  print('--- Test LTrim ---')
  test_ltrim(db, list_name)
  
  db.flushall()
