'''
@Author: Pavan Nakate
@Date: 2022-01-07 09:56
@Last Modified by: Pavan Nakate
@Last Modified time: 2022-01-07
@Title : SQL-Table Storage  
'''
from azure.cosmosdb.table.tableservice import TableService
import json

#opening json file having all the creadential data of azure storage accout
with open('data.json','r') as jf:
    data = json.load(jf)

#getting all the credential data
acc_name = data['account_name']
acc_key = data['account_key']
connection_str = data['connection_string']

#creating a table service 
table_service = TableService(account_name=acc_name, account_key=acc_key)
table_service = TableService(connection_string=connection_str)

class SQLTableOperations:
    """
    Description:
        Class for create_table,insert_data,update_data,query_data,delete_data and delete_table from SQL-Table storage
    """

    def createtable():
        """
        Description:
            Function for creating SQL-table
        Parameter:
            None
        Return:
            None
        """
        try:
            table_service.create_table('tasktable')
            print("Table created")

        except Exception as e:
            print(e)

    def insertentity():
        """
        Description:
            Function for insert the entity(data) in created table
        Parameter:
            None
        Return:
            None
        """
        try:
            task = {'PartitionKey': 'tasksSeattle', 'RowKey': '001',
                'description': 'Take out the trash', 'priority': 200}
            table_service.insert_entity('tasktable', task)
            print("Entity is added")

        except Exception as e:
            print(e)

    def updateentity():
        """
        Description:
            Function for update the entity(data) in given table
        Parameter:
            None
        Return:
            None
        """
        try:
            task = {'PartitionKey': 'tasksSeattle', 'RowKey': '001',
                    'description': 'Take out the garbage', 'priority': 250}
            table_service.update_entity('tasktable', task)
            print("Entity is updated ")

        except Exception as e:
            print(e)

    def updateORinsert():
        """
        Description:
            Function for update(if present) or insert(if absent) the entity(data) in table
        Parameter:
            None
        Return:
            None
        """
        try:
            task = {'PartitionKey': 'tasksSeattle', 'RowKey': '003',
                    'description': 'Buy detergent', 'priority': 300}
            table_service.insert_or_replace_entity('tasktable', task)
            print("Entity is updated or inserted")

        except Exception as e:
            print(e)

    def updatebatch():
        """
        Description:
            Function for update the entities in table
        Parameter:
            None
        Return:
            None
        """
        try:
            task1 = {'PartitionKey': 'tasksSeattle', 'RowKey': '006',
                    'description': 'Go grocery shopping', 'priority': 400}
            task2 = {'PartitionKey': 'tasksSeattle', 'RowKey': '007',
                    'description': 'Clean the bathroom', 'priority': 100}

            with table_service.batch('tasktable') as batch:
                batch.insert_entity(task1)
                batch.insert_entity(task2)
            print("batch is added")

        except Exception as e:
            print(e)

    def queryforentity():
        """
        Description:
            Function for getting diescription of table and data
        Parameter:
            None
        Return:
            None
        """
        try:
            task = table_service.get_entity('tasktable', 'tasksSeattle', '001')
            print(task.description)
            print(task.priority)

        except Exception as e:
            print(e)

    def deleteentity():
        """
        Description:
            Function for deleting the entity(data) in table
        Parameter:
            None
        Return:
            None
        """
        try:
            table_service.delete_entity('tasktable', 'tasksSeattle', '001')
            print("Entity is deleted")

        except Exception as e:
            print(e)

    def deletetable():
        """
        Description:
            Function for deleting the created table
        Parameter:
            None
        Return:
            None
        """
        try:
            table_service.delete_table('tasktable')
            print("Table is deleted")

        except Exception as e:
            print(e)

if __name__ == "__main__":
    sql = SQLTableOperations
    sql.createtable()
    #sql.insertentity()
    #sql.updateentity()
    #sql.updateORinsert()
    #sql.updatebatch()
    #sql.queryforentity()
    #sql.deleteentity()
    #sql.deletetable()