#!/bin/bash
function cleanup {
	echo 'cleaning up...'
	for p in "${pids[@]}" ; do
		kill "$p";
	done
	gcloud dataflow jobs cancel $(gcloud dataflow jobs list --format="table(creationTime.date('%Y-%m-%d'), id, name, state)" | awk 'FNR == 2 {print $2}')
}
trap cleanup EXIT

pids=()
python3 dataflow.py --project=$CLOUDPROJECT --temp_location=$GCS-TEMP --staging_location=$GCS-STAGING --streaming --runner=dataflow --disk_size_gb=532 --region us-central1 &
pids+=($!)
python3 publish.py 
pids+=($!)
