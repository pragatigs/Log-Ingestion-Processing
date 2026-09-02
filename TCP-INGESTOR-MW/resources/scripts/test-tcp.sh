#!/bin/bash

HOST="localhost"
PORT="5001"

send_client() {
    CLIENT_ID=$1

    {
        echo "2026-08-30T18:10:01.123Z INFO  [payment-service] [client-$CLIENT_ID] Payment request received orderId=ORD-1001"
        echo "2026-08-30T18:10:02.456Z DEBUG [payment-service] [client-$CLIENT_ID] Payment processed successfully orderId=ORD-1001"
        echo "2026-08-30T18:10:03.789Z WARN  [payment-service] [client-$CLIENT_ID] Payment processing took longer than expected duration=1250ms"
        echo "2026-08-30T18:10:04.321Z ERROR [payment-service] [client-$CLIENT_ID] Failed to connect to payment gateway host=payment-gateway"
    } | nc "$HOST" "$PORT"
}

for i in {1..10}; do
    send_client "$i" &
done

wait

echo "All log clients finished."