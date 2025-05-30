import dropbox
import io
import os
from dotenv import load_dotenv

load_dotenv()

def get_dbx():
    """Get authenticated Dropbox client"""
    dbx_refresh = os.getenv('DROPBOX_REFRESH_TOKEN')
    if not dbx_refresh:
        raise ValueError("DROPBOX_REFRESH_TOKEN environment variable is not set")
    
    dbx = dropbox.Dropbox(
        app_key='tfbae8qdzocvn5s',
        app_secret='kw1l0pqbaddnfd8',
        oauth2_refresh_token=dbx_refresh
    )
    
    root_id = dbx.users_get_current_account().root_info.root_namespace_id
    dbx = dbx.with_path_root(dropbox.common.PathRoot.root(root_id))
    return dbx

def save_file_to_dropbox(local_file_path, dropbox_path):
    """Save a local file to Dropbox"""
    try:
        dbx = get_dbx()
        
        with open(local_file_path, 'rb') as f:
            file_content = f.read()
        
        # Upload to Dropbox
        dbx.files_upload(file_content, dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        print(f"File uploaded to Dropbox: {dropbox_path}")
        return dropbox_path
        
    except Exception as e:
        print(f"Error uploading to Dropbox: {e}")
        raise e

def get_file_from_dropbox(dropbox_path, return_response=False):
    """Get a file from Dropbox"""
    try:
        dbx = get_dbx()
        md, response = dbx.files_download(path=dropbox_path)
        
        if return_response:
            return response
        
        output = io.BytesIO()
        output.write(response.content)
        output.seek(0)
        return output
        
    except Exception as e:
        print(f"Error downloading from Dropbox: {e}")
        raise e

def save_report_to_dropbox(local_pdf_path, report_name):
    """Save a report PDF to the Vector Reports folder"""
    file_name = os.path.basename(local_pdf_path)  # This will now include the timestamp
    dropbox_path = f"/Vector Official/Vector Leasing/Reports/{file_name}"
    
    return save_file_to_dropbox(local_pdf_path, dropbox_path)
