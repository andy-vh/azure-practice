import os
import uuid

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

try:
    print("Azure Blob storage v12 - Python quickstart sample")
    # Quick start code goes here
    # load secrets from .env file
    load_dotenv()
    connect_str = os.getenv('CONNECTION_STRING')

    # create a container
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = 'quickstart' + str(uuid.uuid4())

    container_client = blob_service_client.create_container(container_name)

    # upload blobs to a container
    # Create a file in local data directory to upload and download
    local_path = "./data"
    local_file_name = "quickstart" + str(uuid.uuid4()) + ".txt"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
    file = open(upload_file_path, 'w')
    file.write("Hello, World!")
    file.close()

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data)

    # List blobs in the container
    print("\nListing blobs...")

    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)

    # Download blobs
    # Download the blob to a local file
    # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
    download_file_path = os.path.join(local_path, str.replace(local_file_name, '.txt', 'DOWNLOAD.txt'))
    print("\nDownloading blob to \n\t" + download_file_path)

    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    # Delete a container
    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(upload_file_path)
    os.remove(download_file_path)

    print("Done")

except Exception as ex:
    print('Exception:')
    print(ex)