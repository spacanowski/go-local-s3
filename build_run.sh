#!/bin/bash

GOOS=linux go build -o s3

docker build -t s3local .

mkdir /tmp/local-s3

docker run -dp 8082:8082 -v /tmp/local-s3:/data s3local