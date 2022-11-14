import json
from time import time

def test_time(func_to_test):
  def wrapper(*args, **kwargs):
    start = time()
    func_result = func_to_test(*args, **kwargs)
    stop = time()
    exec_time = round(stop - start, 4)
    print(f'Execution time for function {func_to_test.__name__} is {exec_time} seconds')
    
    return func_result
  
  return wrapper

@test_time
def load_json(data_path):
  with open(data_path, 'r') as data_file:
    data_json = json.load(data_file)
  
  return data_json
