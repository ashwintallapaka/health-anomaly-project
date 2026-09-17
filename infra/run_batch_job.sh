#!/bin/bash
set -e

REGION="us-central1"
CLUSTER="batch-cluster-$(date +%Y%m%d-%H%M%S)"
BUCKET="gs://health-anomaly-data"
RUN_DATE=$(date +%Y-%m-%d)
LOG="/tmp/batch_run.log"
PROJECT_DIR="/home/abhaykulkarni/health-anomaly-project"

cd "$PROJECT_DIR"

echo "=== Batch run started $(date) ===" >> "$LOG"
# --- Stage 1: Collection — export yesterday's accumulated readings to GCS ---
echo "Exporting MongoDB history to GCS" >> "$LOG"
source "$PROJECT_DIR/venv/bin/activate"
python3 "$PROJECT_DIR/export_history_to_gcs.py" >> "$LOG" 2>&1

# Execution: create ephemeral cluster
echo "Creating cluster $CLUSTER" >> "$LOG"
gcloud dataproc clusters create "$CLUSTER" \
  --region="$REGION" \
  --zone="${REGION}-a" \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-size=50GB \
  --num-workers=2 \
  --worker-machine-type=e2-standard-2 \
  --worker-boot-disk-size=50GB \
  --image-version=2.2-debian12 \
  --max-idle=30m >> "$LOG" 2>&1

# Submit batch job — output written to a dated folder so runs accumulate
echo "Submitting batch job (output: $BUCKET/batch_output/$RUN_DATE)" >> "$LOG"
gcloud dataproc jobs submit pyspark spark/batch_processing.py \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  -- "$BUCKET/historical/date=*/readings.csv" \
     "$BUCKET/batch_output/$RUN_DATE" >> "$LOG" 2>&1

# Tear down
echo "Deleting cluster $CLUSTER" >> "$LOG"
gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --quiet >> "$LOG" 2>&1

echo "=== Batch run finished $(date) ===" >> "$LOG"
