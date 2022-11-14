#!/bin/bash

echo 'Setting up Redis Cluster...'
mkdir cluster
cd cluster

for i in {7000..7002}
do
  echo 'Setting up port' $i

  mkdir $i
  cd ./$i
  touch redis.conf

  prot_mode="protected-mode no\n"
  port="port ${i}\n"

  cls_en="cluster-enabled yes\n"
  cls_cfg="cluster-config-file nodes.conf\n"
  cls_node_timeout="cluster-node-timeout 5000\n"

  mkdir data
  save_dir="dir /data/cluster/${i}/data\n"
  rdb_fn="dbfilename dump.rdb\n"
  ao="appendonly yes\n"
  append_fn="appendfilename \"appendonly.aof\"\n"

  final_str="${prot_mode}${port}${cls_en}${cls_cfg}${cls_node_timeout}${save_dir}${rdb_fn}${ao}${append_fn}"
  printf "${final_str}" > redis.conf

  echo 'Success!'

  cd ./..
done

echo 'Run redis-server...'

for i in {7000..7002}
do
  echo 'Port' $i

  cd ./$i
  cmd_redis_server="redis-server ./redis.conf --daemonize yes"
  eval $cmd_redis_server

  echo 'Success!'

  cd ./..
done

echo 'Starting cluster...'
ip="172.21.0.2"
cmd_redis_cli="redis-cli --cluster create ${ip}:7000 ${ip}:7001 ${ip}:7002 --cluster-yes"
eval $cmd_redis_cli
echo 'Cluster started!'
