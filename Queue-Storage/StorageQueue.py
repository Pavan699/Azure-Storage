'''
@Author: Pavan Nakate
@Date: 2022-01-07 3:56
@Last Modified by: Pavan Nakate
@Last Modified time: 2022-01-07
@Title : Queue Storage  
'''
from azure.storage.queue import QueueClient
import json

#opening json file having all the creadential data of azure storage accout
with open('data.json','r') as jf:
    data = json.load(jf)

connection_str = data['connection_string']
conn_string = connection_str

class QueueStorageOperations:
    """
    Description:
        Class for create_queue,sending masseges and receving masseges to Queue storage
    """

    def createqueue():
        """
        Description:
            Function for creating the queue
        Parameter:
            None
        Return:
            None
        """
        try:
            queue = QueueClient.from_connection_string(conn_str=conn_string, queue_name="myqueue")
            queue.create_queue()
        except Exception as e:
            print(e)

    def sendingmassege():
        """
        Description:
            Function for senfing messages to the queue
        Parameter:
            None
        Return:
            None
        """
        try:
            queue = QueueClient.from_connection_string(conn_str=conn_string, queue_name="myqueue")
            queue.send_message("I'm using queues!")
            queue.send_message("This is my second message")

        except Exception as e:
            print(e)

    def recevingmassege():
        """
        Description:
            Function for receving messages from the queue
        Parameter:
            None
        Return:
            None
        """
        try:
            queue = QueueClient.from_connection_string(conn_str=conn_string, queue_name="myqueue")
            response = queue.receive_messages()

            for message in response:
                print(message.content)
                queue.delete_message(message)

        except Exception as e:
            print(e)

    def deletemassege():
        """
        Description:
            Function for delete message from the queue
        Parameter:
            None
        Return:
            None
        """
        try:
            queue = QueueClient.from_connection_string(conn_str=conn_string, queue_name="myqueue")
            response = queue.delete_message()

        except Exception as e:
            print(e)

if __name__ == "__main__":
    queue = QueueStorageOperations
    queue.createqueue()
    #queue.sendingmassege()
    #queue.recevingmassege()
    #queue.deletemassege()
