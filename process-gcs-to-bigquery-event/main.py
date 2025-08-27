import functions_framework
from google.cloud import storage, bigquery
import json

# Initialize the clients for both services
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define your BigQuery dataset and table
BIGQUERY_DATASET = "BQWorkshop"      
BIGQUERY_TABLE = "customers"          

@functions_framework.cloud_event
def process_gcs_to_bigquery(cloud_event):
    """
    Background Cloud Function that is triggered by a new file in a GCS bucket.
    It reads the JSON file and writes its content to BigQuery.

    Args:
        cloud_event: The CloudEvent object.
    """

    # Get the bucket and file names from the event payload
    file_data = cloud_event.data
    bucket_name = file_data["bucket"]
    file_name = file_data["name"]

    if not file_name.startswith("cloud-function/"):
        print(f"Skipping file: {file_name} as it is not in the 'cloud-function/' folder. \n")
        return "Skipped"

    print(f"Processing file: {file_name} from bucket: {bucket_name} \n")

    try:
        # Get the file's content from Google Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Download the content as a string and decode it
        content = blob.download_as_string().decode("utf-8")

        # Parse the JSON content
        data = json.loads(content)

        # Prepare the data to be inserted into BigQuery.
        rows_to_insert = data

        # Get a reference to the table
        table_ref = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE)

        # Insert the rows into the table
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print(f"Encountered errors while inserting rows: {errors} \n")
            return f"Failed to insert rows into BigQuery. Errors: {errors} \n"
        else:
            print(f"Successfully inserted data from {file_name} into BigQuery. \n")
            return "Success \n"

    except Exception as e:
        print(f"An error occurred: {e} \n")
        return f"Error: {e} \n"
