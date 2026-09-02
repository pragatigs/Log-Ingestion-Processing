# #!/bin/bash

# set -e

# echo "Deleting Kafka consumer group..."

# kubectl exec -n log-processing \
#   log-processing-cluster-log-processing-cluster-pool-0 \
#   -- /bin/bash -c '
#     for group in $(/opt/kafka/bin/kafka-consumer-groups.sh \
#       --bootstrap-server localhost:9092 \
#       --list); do

#       echo "Deleting consumer group: $group"

#       /opt/kafka/bin/kafka-consumer-groups.sh \
#         --bootstrap-server localhost:9092 \
#         --delete \
#         --group "$group" || true

#     done
#   '

# echo "Deleting log files from Minikube..."

# minikube ssh -- "sudo rm -f /data/log-processing/*.log && sudo ls -l /data/log-processing"

# echo "Cleaning Redis..."

# kubectl exec -i \
#   -n log-processing \
#   redis-master-0 \
#   -- sh -c 'redis-cli --scan --pattern "offset-marker:*" | xargs -r redis-cli DEL'

# echo
# echo "Cleanup complete."

#!/bin/bash

set -e

NAMESPACE="log-processing"
KAFKA_POD="log-processing-cluster-log-processing-cluster-pool-0"
TOPIC="log-processing"

echo "======================================"
echo "Cleaning Kafka consumer groups..."
echo "======================================"

kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -- /bin/bash -c '
    for group in $(/opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --list); do

        echo "Deleting consumer group: $group"

        /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --delete \
            --group "$group" || true
    done
'

echo
echo "======================================"
echo "Deleting Kafka topic data..."
echo "======================================"

kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -- \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic "$TOPIC" || true

echo
echo "Recreating Kafka topic..."

kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -- \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$TOPIC" \
    --partitions 5 \
    --replication-factor 1

echo
echo "======================================"
echo "Deleting Minikube log files..."
echo "======================================"

minikube ssh -- \
    "sudo rm -f /data/log-processing/*.log && \
     sudo ls -l /data/log-processing"

echo
echo "======================================"
echo "Cleaning Redis..."
echo "======================================"

kubectl exec -i \
    -n "$NAMESPACE" \
    redis-master-0 \
    -- sh -c \
    'redis-cli --scan --pattern "offset-marker:*" | xargs -r redis-cli DEL'

echo
echo "======================================"
echo "Cleanup complete."
echo "======================================"