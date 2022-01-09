from azure.storage.filedatalake import DataLakeServiceClient
import json

#opening json file having all the creadential data of azure storage accout
with open('data.json','r') as jf:
    data = json.load(jf)

connection_str = data['connection_string']
acc_name = data['account_name']

class DataLakeOperations:
    """
    Description:
        Class for create datalake,create directory,upload file,download file,rename directory,delete directory and list directories.
    """

    def initialize_storage_account(storage_account_name=acc_name, storage_account_key=connection_str):
        """
        Description:
            Function is to initialize the storage account
        Parameter:
            storage_account_name = name of staorage account
            storage_account_key = key of that account
        Return:
            None
        """
        try:  
            global service_client

            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)
            print("Access is doen")
        except Exception as e:
            print(e)

    def create_file_system():
        """
        Description:
            Function for creating the file system
        Parameter:
            None
        Return:
            None
        """
        try:
            global file_system_client

            file_system_client = service_client.create_file_system(file_system="myfilesystem")
            print("File-System is created")
    
        except Exception as e:
            print(e)

    def create_directory():
        """
        Description:
            Function for creating the directory in that file system
        Parameter:
            None
        Return:
            None
        """
        try:
            file_system_client.create_directory("mydirectory")
            print("Directory is created")
    
        except Exception as e:
            print(e)


    def upload_file_to_directory():
        """
        Description:
            Function for uploading the file on the directory from local system
        Parameter:
            path = taking input path from the user
        Return:
            None
        """
        try:

            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")

            directory_client = file_system_client.get_directory_client("mydirectory")
        
            file_client = directory_client.create_file("uploaded-file.txt")
            path = input("Enter the path of file to upload : ")
            local_file = open(path,'r')

            file_contents = local_file.read()

            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

            file_client.flush_data(len(file_contents))
            print("File is uploaded")

        except Exception as e:
            print(e)

    def upload_file_to_directory_bulk():
        """
        Description:
            Function for uploading the file on the directory from local system
        Parameter:
            None
        Return:
            None
        """
        try:

            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")

            directory_client = file_system_client.get_directory_client("mydirectory")
        
            file_client = directory_client.get_file_client("uploaded-file.txt")

            local_file = open("/home/pavan-linux/StudInfoUP",'r')

            file_contents = local_file.read()

            file_client.upload_data(file_contents, overwrite=True)
            print("Bulk-of-file is uploaded")

        except Exception as e:
            print(e)

    def download_file_from_directory():
        """
        Description:
            Function for download the file from datalake directoey to local system
        Parameter:
            None
        Return:
            None
        """
        try:
            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")

            directory_client = file_system_client.get_directory_client("mydirectory")
        
            local_file = open("/home/pavan-linux/Azure/outputdatalake",'wb')

            file_client = directory_client.get_file_client("uploaded-file.txt")

            download = file_client.download_file()

            downloaded_bytes = download.readall()

            local_file.write(downloaded_bytes)

            local_file.close()
            print("File is downloaded")

        except Exception as e:
            print(e)

    def list_directory_contents():
        """
        Description:
            Function for listing the files/directories from datalake
        Parameter:
            None
        Return:
            None
        """
        try:
        
            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")

            paths = file_system_client.get_paths(path="mydirectory")
            print("List of directories : ")
            for path in paths:
                print(path.name + '\n')

        except Exception as e:
            print(e)

    def rename_directory():
        """
        Description:
            Function for renameing the directory
        Parameter:
            None
        Return:
            None
        """
        try:
       
            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")
            directory_client = file_system_client.get_directory_client("mydirectory")
       
            new_dir_name = "mydirectoryrenamed"
            directory_client.rename_directory(new_name=directory_client.file_system_name + '/' + new_dir_name)
            print("Directorey is Renamed")

        except Exception as e:
            print(e)

    def delete_directory():
        """
        Description:
            Function for deleting the directory
        Parameter:
            None
        Return:
            None
        """
        try:
            file_system_client = service_client.get_file_system_client(file_system="myfilesystem")
            directory_client = file_system_client.get_directory_client("mydirectoryrenamed")

            directory_client.delete_directory()
            print("Directory is deleted")

        except Exception as e:
            print(e)

if __name__ == "__main__":
    datalake = DataLakeOperations
    datalake.initialize_storage_account()
    #datalake.create_file_system()
    #datalake.create_directory()
    #datalake.upload_file_to_directory()
    #datalake.upload_file_to_directory_bulk()
    #datalake.download_file_from_directory()
    #datalake.list_directory_contents()
    #datalake.rename_directory()
    #datalake.delete_directory()