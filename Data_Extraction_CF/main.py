import functions_framework


project_id = 
location =  # Format is "us" or "eu"
processor_id =  # Create processor before running sample
gcs_output_uri =  # Must end with a trailing slash `/`. Format: gs://bucket/directory/subdirectory/
processor_version_id = # Form OCR processor ID Optional. Example: pretrained-ocr-v1.0-2020-09-23

# TODO(developer): You must specify either `gcs_input_uri` and `mime_type` or `gcs_input_prefix`
# gcs_input_uri =  # Format: gs://bucket/directory/file.pdf
input_mime_type = "application/pdf"
gcs_input_prefix =  # Format: gs://bucket/directory/
# field_mask = "text,entities,pages.pageNumber"  # Optional. The fields to return in the Document object.



from typing import Optional

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import InternalServerError
from google.api_core.exceptions import RetryError
from google.cloud import documentai  # type: ignore
from google.cloud import storage
from cloudevents.http import CloudEvent
from google.cloud.documentai_toolbox import document
import logging
import google.cloud.logging
from time import sleep

client = google.cloud.logging.Client(project=project_id)
client.setup_logging()

def batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_output_uri: str,
    processor_version_id: Optional[str] = None,
    gcs_input_uri: Optional[str] = None,
    input_mime_type: Optional[str] = None,
    gcs_input_prefix: Optional[str] = None,
    field_mask: Optional[str] = None,
    timeout: int = 400,
) -> None:
    # You must set the `api_endpoint` if you use a location other than "us".
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    if gcs_input_uri:
        # Specify specific GCS URIs to process individual documents
        gcs_document = documentai.GcsDocument(
            gcs_uri=gcs_input_uri, mime_type=input_mime_type
        )
        logging.info("gcs_document: ",gcs_document)
        # Load GCS Input URI into a List of document files
        gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
        input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)
    else:
        # Specify a GCS URI Prefix to process an entire directory
        gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_prefix)
        input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)

    # Cloud Storage URI for the Output Directory
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=gcs_output_uri, field_mask=field_mask
    )

    # Where to write results
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

    if processor_version_id:
        # The full resource name of the processor version, e.g.:
        # projects/{project_id}/locations/{location}/processors/{processor_id}/processorVersions/{processor_version_id}
        name = client.processor_version_path(
            project_id, location, processor_id, processor_version_id
        )
    else:
        # The full resource name of the processor, e.g.:
        # projects/{project_id}/locations/{location}/processors/{processor_id}
        name = client.processor_path(project_id, location, processor_id)

    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    # BatchProcess returns a Long Running Operation (LRO)
    operation = client.batch_process_documents(request)

    # Continually polls the operation until it is complete.
    # This could take some time for larger files
    # Format: projects/{project_id}/locations/{location}/operations/{operation_id}
    # try:
    #     print(f"Waiting for operation {operation.operation.name} to complete...")
    #     operation.result(timeout=timeout)
    # # Catch exception when operation doesn't finish before timeout
    # except (RetryError, InternalServerError) as e:
    #     print(e.message)

    # NOTE: Can also use callbacks for asynchronous processing
    #
    def my_callback(future):
        result = future.result()
    logging.info("Running parser processor")
    operation.add_done_callback(my_callback)

    while True:

        if operation.operation.done:
            logging.info("Parsing completed")
            gcs_bucket_name = # Bucket name of stored json file
            output_file_prefix = # location to store the extracted tables
            gcs_prefix = # Sub directory or folder name of the stored json file 
            logging.info("Table Extraction started")
            table_extraction(gcs_prefix, output_file_prefix, gcs_bucket_name)
            break
        else:
            logging.info("Table Extraction in progress")
            sleep(10)


def write_table_to_md(gcs_bucket_name,temp_df, output_filename, output_file_prefix):
    """
    Writes data from a JSON file to a text file in GCS.

    Args:
    gcs_bucket_name (str): Name of the GCS bucket.
    text_content (str): Contains actual string.
    text_file_name (str): Name of the text file to create in GCS.

    """

    # Create a storage client
    storage_client = storage.Client()

    # Get a reference to the bucket
    bucket = storage_client.bucket(gcs_bucket_name)

    # Create a blob object representing the text file
    blob = bucket.blob(f'{output_file_prefix}/{output_filename}')

    # Upload the text content as UTF-8 encoded data
    blob.upload_from_string(temp_df.to_markdown(), content_type="text/plain")

    logging.info(f"JSON data written to text file: gs://{gcs_bucket_name}/{output_file_prefix}/{output_filename}")

def table_extraction(gcs_prefix: str, output_file_prefix: str, gcs_bucket_name: str) -> None:
    logging.info("Table extraction started")
    storage_client = storage.Client()

    # Get GCS bucket
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blobs_specific = list(bucket.list_blobs(prefix=gcs_prefix))
    blob_names = [blob.name for blob in blobs_specific]
    for files in blob_names:
        file_name_=files.split("/")[-1].split(".")[-2]
        wrapped_document = document.Document.from_gcs_uri(gcs_uri=f"gs://{gcs_bucket_name}/{files}")
        logging.info("Tables in Document")
        for page in wrapped_document.pages:
            for table_index, table in enumerate(page.tables):
                df = table.to_dataframe()
                output_filename = f"{file_name_}-{page.page_number}-{table_index}.md"

                write_table_to_md(gcs_bucket_name,df, output_filename,output_file_prefix)

@functions_framework.cloud_event
def parse_doc(cloud_event: CloudEvent):
    batch_process_documents(project_id=project_id,
    location=location,
    processor_id=processor_id,
    gcs_output_uri=gcs_output_uri,
    processor_version_id=processor_version_id,
    input_mime_type=input_mime_type,
    gcs_input_prefix=gcs_input_prefix)

    return "success"
