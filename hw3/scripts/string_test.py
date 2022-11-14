import json
import numpy as np
from time import time
from tabulate import tabulate

from utils import test_time

@test_time
def convert_json_to_str(data_json):
  return json.dumps(data_json, default=str)

@test_time
def save_as_str(db, data_json):
  str_data = convert_json_to_str(data_json)
  db.set('string_data', str_data)

@test_time
def get_str(db, key_name='string_data'):
  return db.get(key_name)

@test_time
def delete_key(db, key):
  db.delete(key)

def test_str(db, data_json, n_repeat=10):
  exec_times = np.zeros((n_repeat, 3))
  for n in range(n_repeat):
    save_start = time()
    save_as_str(db, data_json)
    save_stop = time()
    
    get_start = time()
    json_from_str = get_str(db)
    get_stop = time()
    
    delete_start = time()
    delete_key(db, 'string_data')
    delete_stop = time()
    
    exec_times[n][0] = save_stop - save_start
    exec_times[n][1] = get_stop - get_start
    exec_times[n][2] = delete_stop - delete_start
    
  return np.mean(exec_times, axis=0)

def simple_tests_for_string(db, data_json):
  print('----- Tests for string -----')
  save_as_str(db, data_json)
  json_from_str = get_str(db)
  del json_from_str
  delete_key(db, 'string_data')
  print('\n\n')

def string_performance_tests(db, data_json):
  print('----- String performance tests -----')
  # m_set, m_get, m_del
  str_results = test_str(db, data_json, n_repeat=10)
  str_results = list(map(lambda x: round(x, 3), str_results))
  print('\n\n')
  
  print('----- String performance test result -----')
  data = [str_results]
  print(tabulate(data, headers=["Set", "Get", "Delete"]))
  
  return str_results
