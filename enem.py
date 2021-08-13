#!/usr/bin/python3

gcloud dataproc jobs submit pyspark \
    --cluster cluster-ubuntu \
    --region us-central1 \
    enem.py \
    -- --bucket=$1