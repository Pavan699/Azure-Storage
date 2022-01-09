'''
@Author: Pavan Nakate
@Date: 2022-01-06 8:30
@Last Modified by: Pavan Nakate
@Last Modified time: 2022-01-07
@Title : Blob-Storage 
'''
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobClient
import json

#opening json file having all the creadential data of azure storage accout
with open('data.json','r') as jf:
    data = json.load(jf)

connection_str = data['connection_string']
conn_string = connection_str

class BlobStorageOperations:
    """
    Description:
        Class for create container,upload file,download file,listing files & deleteing container from the azure bolo-storage
    """

    def createcontainer():
        """
        Description:
            Function for creating the Blob container
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            container_client = ContainerClient.from_connection_string(conn_str=conn_string, container_name="mycontainer")
            container_client.create_container()
            print("Container is created")
        except Exception as e:
            print(e)


    def uploadfiletoblob():
        """
        Description:
            Function for uploading the file into blob container
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="mycontainer", blob_name="myblob")

            with open("/home/pavan-linux/StudInfoAll", "rb") as data:
                blob.upload_blob(data)
                print("File is uploaded")

        except Exception as e:
            print(e)


    def downloadblob():
        """
        Description:
            Function for downloading the file from blob storage
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            blob = BlobClient.from_connection_string(conn_str=conn_string, container_name="mycontainer", blob_name="myblob")

            with open("./BlockDestination.txt", "wb") as my_blob:
                blob_data = blob.download_blob()
                blob_data.readinto(my_blob)

        except Exception as e:
            print(e)


    def listblobs():
        """
        Description:
            Function for listing the files from blob storage
        Parameter:
            using conn_string for the connecting to storage account of azure
        Return:
            None
        """
        try:
            container = ContainerClient.from_connection_string(conn_str=conn_string, container_name="mycontainer")

            blob_list = container.list_blobs()
            for blob in blob_list:
                print(blob.name + '\n')

        except Exception as e:
            print(e)

        finally:
            """
            Description:
                finally block for deleting the container
            """
            # Delete the container
            container.delete_container()

if __name__ == "__main__":
    blobcontainer= BlobStorageOperations
    blobcontainer.createcontainer()
    #blobcontainer.uploadfiletoblob()
    #blobcontainer.downloadblob()
    #blobcontainer.listblobs()