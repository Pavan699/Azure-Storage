'''
@Author: Pavan Nakate
@Date: 2022-01-07 11:28
@Last Modified by: Pavan Nakate
@Last Modified time: 2022-01-07
@Title : File-Storage 
'''
from azure.storage.fileshare import ShareClient
from azure.storage.fileshare import ShareFileClient
from azure.storage.fileshare import ShareDirectoryClient
import json

#opening json file having all the creadential data of azure storage accout
with open('data.json','r') as jf:
    data = json.load(jf)

connection_str = data['connection_string']
conn_string = connection_str

class FileStorageOperations:
    """
    Description:
        Class for create,upload,download and list the files from the azure file storage
    """

    def createfileshare():
        """
        Description:
            Function for creating the file share
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            share = ShareClient.from_connection_string(conn_str=conn_string, share_name="myshare")
            share.create_share()

        except Exception as e:
            print(e)

    def uploadfiletofileshare():
        """
        Description:
            Function for uploading the file from local to azure file storage
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            file_client = ShareFileClient.from_connection_string(conn_str=conn_string, share_name="myshare", file_path="myfile")

            with open("/home/pavan-linux/StudInfoAll", "rb") as source_file:
                file_client.upload_file(source_file)

        except Exception as e:
            print(e)

    def downloadfileshare():
        """
        Description:
            Function for downloads the file from azure to the loacal system
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            file_client = ShareFileClient.from_connection_string(conn_str=conn_string, share_name="myshare", file_path="myfile")

            with open("DEST_FILE", "wb") as file_handle:
                data = file_client.download_file()
                data.readinto(file_handle)

        except Exception as e:
            print(e)

    def listfileshare():
        """
        Description:
            Function list all the file share and files in azure file shear
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            parent_dir = ShareDirectoryClient.from_connection_string(conn_str=conn_string, share_name="myshare", directory_path="parentdir")

            my_list = list(parent_dir.list_directories_and_files())
            print(my_list)

        except Exception as e:
            print(e)

if __name__ == "__main__":
    filestorage = FileStorageOperations
    filestorage.createfileshare()
    #filestorage.uploadfiletofileshare()
    #filestorage.downloadfileshare()
    #filestorage.listfileshare()