#!/bin/bash

kubectl run kcat \
  -n log-processing \
  --rm -i \
  --restart=Never \
  --image=edenhill/kcat:1.7.1 \
  --command -- sh <<'SCRIPT'

kcat -P -b log-processing-cluster-kafka-bootstrap:9092 -t log-processing -p 0 <<EOF
{"id":1,"level":"INFO","message":"User login successful"}
{"id":5,"level":"INFO","message":"Order created successfully"}
{"id":9,"level":"INFO","message":"Email notification sent"}
{"id":13,"level":"INFO","message":"Inventory updated successfully"}
EOF

kcat -P -b log-processing-cluster-kafka-bootstrap:9092 -t log-processing -p 1 <<EOF
{"id":2,"level":"DEBUG","message":"Fetching user profile from database"}
{"id":6,"level":"DEBUG","message":"Cache miss for product catalog"}
{"id":10,"level":"DEBUG","message":"JWT token validated"}
{"id":14,"level":"DEBUG","message":"Retrying failed HTTP request"}
{"id":18,"level":"DEBUG","message":"Parsing incoming JSON payload"}
{"id":21,"level":"DEBUG","message":"Loading application configuration"}
{"id":22,"level":"DEBUG","message":"Connection pool initialized"}
EOF

kcat -P -b log-processing-cluster-kafka-bootstrap:9092 -t log-processing -p 2 <<EOF
{"id":3,"level":"ERROR","message":"Database connection timeout"}
{"id":7,"level":"ERROR","message":"Payment gateway returned HTTP 500"}
{"id":11,"level":"ERROR","message":"Failed to write audit log"}
{"id":15,"level":"ERROR","message":"Unable to acquire database lock"}
{"id":19,"level":"ERROR","message":"Redis connection refused"}
EOF

kcat -P -b log-processing-cluster-kafka-bootstrap:9092 -t log-processing -p 3 <<EOF
{"id":4,"level":"WARN","message":"Disk usage exceeded 85%"}
{"id":8,"level":"WARN","message":"API rate limit approaching threshold"}
{"id":12,"level":"WARN","message":"Kafka consumer lag detected"}
{"id":16,"level":"WARN","message":"Memory usage above 90%"}
{"id":20,"level":"WARN","message":"Configuration file missing optional property"}
{"id":23,"level":"WARN","message":"SSL certificate expires in 15 days"}
{"id":24,"level":"WARN","message":"High CPU utilization detected"}
{"id":25,"level":"WARN","message":"Retry queue size is growing"}
EOF

echo "Done!"

SCRIPT