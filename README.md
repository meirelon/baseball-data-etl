# Baseball Data ETL

This project will get the most recent day of statcast data from Baseball Savant
and dump the table into a Google Bigquery database of your choice.

## Getting Started
In order to run this daily, you will need to set up a Google Cloud Function,
then schedule the function to trigger daily with Google Cloud Scheduler.

## Run Locally
This will return the latest day of statcast data as a DataFrame
```
git clone https://github.com/meirelon/statcast.git
cd statcast
pip install -r requirements.txt
python deps/statcast.py
```

## Build Cloud Function
```
git clone https://github.com/meirelon/statcast.git
cd statcast
gcloud beta functions deploy statcast_request --set-env-vars=PROJECT_ID=[YOUR-PROJECT-ID],DESTINATION_TABLE=[YOUR-DESTINATION-TABLE]  --runtime python37 --trigger-http
```
