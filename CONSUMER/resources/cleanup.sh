#!/bin/bash

set -e

echo "Deleting Kafka consumer group..."

kubectl exec -it \
  -n log-processing \
  log-processing-cluster-log-processing-cluster-pool-0 \
  -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --delete \
     --group log-processing-group || true

echo "Deleting log files from Minikube..."

minikube ssh -- "sudo rm -f /data/log-processing/*.log && sudo ls -l /data/log-processing"

echo "Cleaning Redis..."

kubectl exec -i \
  -n log-processing \
  redis-master-0 \
  -- sh -c 'redis-cli --scan --pattern "offset-marker:*" | xargs -r redis-cli DEL'

echo
echo "Cleanup complete."