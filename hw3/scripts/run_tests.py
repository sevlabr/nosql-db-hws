import redis

from utils import load_json
from string_test import (
  simple_tests_for_string,
  string_performance_tests
)
from zset_test import (
  prepare_for_zset,
  zset_tests,
)
from hset_test import hset_tests
from list_test import (
  prepare_for_list,
  list_tests
)

if __name__ == '__main__':
  data_path = './data/uber-raw-data-jul14.json'
  data_json = load_json(data_path)

  zset_name = 'trips'
  zset_data = prepare_for_zset(data_json)

  hset_name = "rides"
  
  list_data = prepare_for_list(data_json)
  list_name = 'voyages'
  
  db = redis.Redis(host='localhost', port=6379)
  
  print('\n\n')
  simple_tests_for_string(db, data_json)
  str_results = string_performance_tests(db, data_json)
  
  print('\n\n')
  zset_tests(db, zset_name, zset_data, data_json)
  
  hset_tests(db, hset_name, data_json)
  
  list_tests(db, list_name, list_data, data_json)
  