import rediscluster

from utils import load_json, test_time

@test_time
def prepare_data_for_cluster(data_json):
  cluster_data = []
  for _, item in data_json.items():
    key = item['Date/Time']
    val = '"Lat": ' + str(item['Lat'])\
          + ', "Lon": ' + str(item['Lon'])\
          + ', "Base": ' + str(item['Base'])
    
    cluster_data.append((key, val))
    
  return cluster_data

@test_time
def fill_cluster(rc, cluster_data, ttl_val=3600):
  for key, val in cluster_data:
    rc.set(key, val)
    rc.expire(key, ttl_val)

def get_cluster_data(rc, data_json, indices=[213, 14241, 214523, 67232]):
  for index in indices:
    key = data_json[str(index)]['Date/Time']
    value = rc.get(key)
    ttl_val = rc.ttl(key)
    print(f'Key index in JSON: {index}')
    print(f'Key: {key}')
    print(f'Value: {value}')
    print(f'Time to live: {ttl_val} seconds')

@test_time
def test_cluster_delete(rc):
  rc.flushall()
  
if __name__ == "__main__":
  data_path = './data/uber-raw-data-jul14.json'
  data_json = load_json(data_path)
  cluster_data = prepare_data_for_cluster(data_json)
  
  host = "172.21.0.2"
  startup_nodes = [
    {"host": host, "port": "7000"},
    {"host": host, "port": "7001"},
    {"host": host, "port": "7002"},
  ]

  rc = rediscluster.RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
  
  fill_cluster(rc, cluster_data)
  get_cluster_data(rc, data_json)
  test_cluster_delete(rc)
