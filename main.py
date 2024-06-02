from googleapiclient import discovery
from google.cloud import storage
from pydicom import dcmread
from PIL import Image
import os
import json

# Set up your GCP environment variables
os.environ['GCP_PROJECT'] = 'da-kalbe'
os.environ['DICOM_STORE_LOCATION'] = 'asia-southeast2'
os.environ['DICOM_STORE_DATASET'] = 'dicom-dataset'
os.environ['DICOM_STORE_ID'] = 'pspls-LungCT-Diagnosis'
os.environ['DESTINATION_BUCKET'] = 'da-test-dicom'

# Initialize Healthcare API client
healthcare_client = discovery.build('healthcare', 'v1')

# Initialize Cloud Storage client
storage_client = storage.Client()

def export_dicom_instance(event, context):
    """
    This function receives a Pub/Sub message containing a DICOM instance URI
    from Cloud Healthcare API, exports the DICOM instance to a GCS bucket,
    converts it to PNG, and saves the PNG to the same GCS bucket.
    """

    # Extract the DICOM instance URI from the Pub/Sub message
    message = json.loads(event['data'])
    dicom_instance_uri = message['dicom_instance_uri']

    # Export the DICOM instance to GCS
    export_request = {
        "gcsDestination": {
            "uriPrefix": f"gs://{os.environ['DESTINATION_BUCKET']}/dicom"
        }
    }

    response = healthcare_client.projects().locations().datasets().dicomStores().instances().export(
        name=dicom_instance_uri, body=export_request
    ).execute()

    # Extract the GCS URI of the exported DICOM file
    exported_dicom_uri = response['gcsDestination']['uri']

    # Download the DICOM file from GCS
    bucket_name = exported_dicom_uri.split('/')[2]
    blob_name = '/'.join(exported_dicom_uri.split('/')[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    dicom_data = blob.download_as_string()

    # Convert the DICOM file to PNG
    ds = dcmread(dicom_data)
    image = Image.fromarray(ds.pixel_array)

    # Save the PNG file to GCS
    png_filename = os.path.splitext(blob_name)[0] + '.png'
    blob = bucket.blob(png_filename)
    blob.upload_from_string(image.tobytes(), content_type='image/png')

    print(f"Exported DICOM instance to GCS: {exported_dicom_uri}")
    print(f"Converted to PNG and saved to GCS: gs://{bucket_name}/{png_filename}")

# Pub/Sub Message
event = {
    'data': json.dumps({
        'dicom_instance_uri': 'projects/your-project-id/locations/your-region/datasets/your-dataset-id/dicomStores/your-dicom-store-id/instances/your-dicom-instance-id'
    })
}

export_dicom_instance(event, {})
