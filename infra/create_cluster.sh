#!/bin/bash
gcloud dataproc clusters create health-cluster \
  --region=us-central1 \
  --zone=us-central1-a \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-size=50GB \
  --num-workers=2 \
  --worker-machine-type=e2-standard-2 \
  --worker-boot-disk-size=50GB \
  --image-version=2.2-debian12 \
  --max-idle=2h
