#!/bin/bash
CLUSTERNAME=$1
date
echo "Begin creation of cluster"
#Standard 108 
#time gcloud dataproc clusters create $CLUSTERNAME --zone us-central1-f --master-machine-type n1-standard-16 --master-boot-disk-size 500 --num-workers 6 --worker-machine-type n1-standard-32 --worker-boot-disk-size 500 --image-version 1.1 --scopes 'https://www.googleapis.com/auth/cloud-platform' --project shc-afe-dev --initialization-actions 'gs://shc-afe-dev.appspot.com/prod/scripts/setupR.sh' --initialization-action-timeout 30m
#High Memory 208
gcloud.cmd dataproc clusters create $CLUSTERNAME --zone us-central1-b --master-machine-type n1-standard-32 --master-boot-disk-size 500 --num-workers 6 --worker-machine-type n1-standard-16 --worker-boot-disk-size 500 --project shc-afe-dev
echo "End cluster creation"
date
