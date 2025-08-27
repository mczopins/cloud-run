import functions_framework
import json
from google.cloud import storage, bigquery

# Initialize the clients for both services
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define your BigQuery dataset and table
BIGQUERY_DATASET = "BQWorkshop"  # Zmień na nazwę Twojego zbioru danych
BIGQUERY_TABLE = "customers"      # Zmień na nazwę Twojej tabeli
BUCKET_NAME="raw-mczopinski-477-20240625075327-c56qqpusxw"
GCS_PATH="cloud-function/"

@functions_framework.http
def process_gcs_to_bigquery(request):
    """
    HTTP Cloud Function that reads a file from GCS and writes its content to BigQuery.

    Expected query parameters:
    - 'file': The name of the file to read.
    """

    # Get bucket and file names from the HTTP request query parameters
    file_name = request.args.get('file')

    if not file_name:
        return "Error: Missing 'file' query parameter.", 400

    try:
        # Get the file's content from GCS
        bucket = storage_client.bucket(BUCKET_NAME)
        gcs_file_name=GCS_PATH + file_name
        blob = bucket.blob(gcs_file_name)
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
            print(f"Encountered errors while inserting rows: {errors}")
            return "Failed to insert rows into BigQuery. \n", 500
        else:
            return "Successfully read file and inserted data into BigQuery. \n", 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"Error: {e} \n", 500
