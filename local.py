from googleapiclient import discovery
from google.cloud import storage
from pydicom import dcmread
from PIL import Image
import os
import json

# Mock environment variables
os.environ['GCP_PROJECT'] = 'da-kalbe'  # Replace with your project ID
os.environ['DICOM_STORE_LOCATION'] = 'asia-southeast2'  # Replace with your location
os.environ['DICOM_STORE_DATASET'] = 'test1'  # Replace with your dataset ID
os.environ['DICOM_STORE_ID'] = 'test-dicom'  # Replace with your DICOM store ID
os.environ['DESTINATION_BUCKET'] = 'da-test-dicom'  # Replace with your bucket name

# Mock event data
event = {
    'data': json.dumps({
        'bucket': 'da-test-dicom',
        'name': './dicom_00000001_001.dcm'
    })
}

# Mock context data
context = {}

def export_dicom_instance(event, context):
    # ... your existing code ...
    
    # Modify this part to test locally without actual GCP resources
    # 1. Use a local DICOM file instead of fetching from GCS
    # 2. Replace the actual export request with a mock response
    # 3. Handle DICOM to PNG conversion locally

    # 1. Local DICOM file
    dicom_file_path = './dicom_00000001_001.dcm'  # Replace with your local DICOM file

    # 2. Mock export response
    response = {
        'status': 'SUCCESS',
        'message': 'Mock export completed successfully'
    }

    # 3. Local DICOM to PNG conversion
    try:
        ds = dcmread(dicom_file_path)
        print(f"DICOM pixel_array shape: {ds.pixel_array.shape}")

        image = Image.fromarray(ds.pixel_array)
        png_filename = 'temp.png'
        image.save(png_filename)

        print(f"Created PNG file: {png_filename}")

    except Exception as e:
        print(f"Error processing {dicom_file_path}: {str(e)}")

    return response

# Execute the function
result = export_dicom_instance(event, context)
print(f"Result: {result}")