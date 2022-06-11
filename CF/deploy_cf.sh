gcloud functions deploy crime_data \
    --project=bitcoindata-352508 \
    --region=europe-west1 \
    --entry-point=main \
    --memory=512MB \
    --runtime=python38 \
    --service-account=ew-468@bitcoindata-352508.iam.gserviceaccount.com	 \
    --env-vars-file=./vars.yaml \
    --trigger-http \
    --timeout=540s