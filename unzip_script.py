import os
import io
import json
import zipfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# 1. Setup Authentication
# We load the credentials from the Environment Variable we set in GitHub Secrets
creds_json = json.loads(os.environ['GDRIVE_CREDENTIALS'])
creds = service_account.Credentials.from_service_account_info(creds_json)
service = build('drive', 'v3', credentials=creds)

def unzip_process(file_id):
    print(f"Starting process for File ID: {file_id}")
    
    # 2. Get File Metadata (to find the parent folder)
    file_metadata = service.files().get(fileId=file_id, fields='name, parents').execute()
    file_name = file_metadata.get('name')
    parent_folder_id = file_metadata.get('parents')[0] # Assumes file is in a folder
    
    print(f"Found file: {file_name} in folder: {parent_folder_id}")

    # 3. Download the Zip File
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    print("Downloading...")
    while done is False:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

    # 4. Extract
    print("Unzipping in memory...")
    try:
        z = zipfile.ZipFile(fh)
        for name in z.namelist():
            print(f"Extracting: {name}")
            extracted_data = z.read(name)
            
            # Create a temporary file on disk to upload
            with open(name, "wb") as f:
                f.write(extracted_data)

            # 5. Upload back to Drive
            file_metadata = {
                'name': name,
                'parents': [parent_folder_id]
            }
            media = MediaFileUpload(name, resumable=True)
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"Uploaded: {name}")
            
            # Cleanup local file
            os.remove(name)
            
    except zipfile.BadZipFile:
        print("Error: The file is not a valid zip file.")

if __name__ == "__main__":
    # Get the file ID passed from the GitHub Action input
    target_file_id = os.environ['TARGET_FILE_ID']
    unzip_process(target_file_id)