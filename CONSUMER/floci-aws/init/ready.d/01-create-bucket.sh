#!/bin/sh
set -eu

echo "Creating S3 bucket..."

aws s3 mb s3://log-processing || true

echo "Done."