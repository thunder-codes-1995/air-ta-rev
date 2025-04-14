import paramiko
import os
import gzip
import shutil

from dotenv import load_dotenv

load_dotenv()

def find_remote_file(sftp, remote_directory, date_pattern, file_suffix):
    for filename in sftp.listdir(remote_directory):
        if date_pattern in filename and filename.endswith(file_suffix):
            return os.path.join(remote_directory, filename)
    return None



def download(date_pattern):

    hostname = os.getenv("SV_HOSTNAME")
    username = os.getenv("SV_USERNAME")
    private_key_path = os.path.expanduser(os.getenv("SV_PRIVATE_KEY_PATH"))
    passphrase = os.getenv("SV_PASSPHRASE")  # Private key passphrase
    remote_directory = '../'
    local_gz_file_path = "tmp/file.gz"
    local_extracted_file_path = "tmp/file.DATA"


    key = paramiko.RSAKey.from_private_key_file(private_key_path, password=passphrase)
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, pkey=key)

    sftp = paramiko.SFTPClient.from_transport(transport)

    # Find file that fits that date
    remote_file_path = find_remote_file(sftp, remote_directory, date_pattern, "AMA.SVA.FTP.DATA.gz")
    if not remote_file_path:
        print(f"Could not find a file containing '{date_pattern}' and ending with 'AMA.SVA.FTP.DATA.gz'.")
        sftp.close()
        transport.close()
        return

    print("Downloading...")
    # download .gz file
    sftp.get(remote_file_path, local_gz_file_path)

    sftp.close()
    transport.close()

    print("Downloaded successfully.")



    # extract .gz file
    with gzip.open(local_gz_file_path, 'rb') as f_in:
        with open(local_extracted_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Extracted")


