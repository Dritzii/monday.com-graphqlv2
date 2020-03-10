import requests
import csv
import io
import os

from datetime import date
from azure.storage.blob import BlockBlobService
from io import BytesIO



class config():
    def __init__(self, delimiter, quote):
        self.token = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjM5NjY4OTU2LCJ1aWQiOjExNTU5MDM1LCJpYWQiOiIyMDIwLTAzLTA5IDIxOjU4OjU4IFVUQyIsInBlciI6Im1lOndyaXRlIn0.ynf2bpoQ0GIPqyqADCArodqwbu9K_1Y8exaERUK15nY"
        self.base_url = "https://api.monday.com/v2/"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": self.token
        }
        self.delimiter = delimiter
        self.quote = quote
        self.quote_normals = csv.QUOTE_NONNUMERIC
        self.account_name = 'devblobdatazendesk'
        self.account_key = 'Ve77nc2T1Ieo0xGhzb86OBTPFM8L5KTGZkpQ4PAqdgrEpNx9Ej7VqZEc6Giemsf+hXriYK8xKMSonVP7REJUFQ=='
        self.file_path = os.getcwd() + '/'
        self.delimiter = delimiter
        self.quote = quote
        self.quote_normals = csv.QUOTE_NONNUMERIC
        self.blob_bool = True
        self.rmv_file = False


    def blob_upload(self, typename):
        print("connecting to blob storage")
        blob_service = BlockBlobService(account_name = self.account_name,account_key = self.account_key)
        blob_service.create_blob_from_path('csv-blob', blob_name= typename + '.csv',
        file_path=self.file_path + typename + '.csv',timeout=360)
        print("blob uploaded")

    def get_board_names(self):
        body = '{"query":"{boards(state:all){id name board_folder_id }}"}'
        data = requests.post(self.base_url,headers=self.headers,data=body).json()
        count = -1
        with io.open("get_board_names" + '.csv','w',newline='',encoding='utf-8') as new_file:
            writer = csv.writer(new_file, delimiter= self.delimiter ,quotechar= self.quote, quoting=self.quote_normals)
            writer.writerow(['id','name','board_folder_id'])
            for each in range(len(data['data']['boards'])):
                count +=1
                print(count)
                writer.writerow([data['data']['boards'][count]['id'],
                                data['data']['boards'][count]['name'],
                                data['data']['boards'][count]['board_folder_id']])
            if self.blob_bool:
                self.blob_upload("get_board_names")
                if self.rmv_file:
                    os.remove("get_board_names" + '.csv')
            else:
                print("not uploading")
    def test(self):
        body = '{"query": "{items (limit: 5000) {board {id} created_at id name creator_id creator {id}  }}"}'
        data = requests.post(self.base_url,headers=self.headers,data=body).json()
        print(data)

    def get_column_value_names(self): 
        with io.open("get_column_value_names" + '.csv','w',newline='',encoding='utf-8') as new_file:
            writer = csv.writer(new_file, delimiter= self.delimiter ,quotechar= self.quote, quoting=self.quote_normals)
            writer.writerow(['board_id','created_at','id','name','creator_id','updated_at','column_values'])
            body = '{"query": "{items (limit: 10000) {board {id} created_at id name creator_id updated_at column_values {text} }}"}'
            data = requests.post(self.base_url,headers=self.headers,data=body).json()
            for each in data['data']['items']:
                writer.writerow([each['board']['id'],
                            each['created_at'],
                            each['id'],
                            each['name'],
                            each['creator_id'],
                            each['updated_at'],
                            each['column_values']])
            if self.blob_bool:
                self.blob_upload("get_column_value_names")
                if self.rmv_file:
                    os.remove("get_column_value_names" + '.csv')
            else:
                print("not uploading")
                        
    def get_user_names(self):
        body = '{"query":"{users(kind:all){name birthday country_code email id teams {id}}}"}'
        data = requests.post(self.base_url,headers=self.headers,data=body).json()
        count = -1
        with io.open("get_board_names" + '.csv','w',newline='',encoding='utf-8') as new_file:
            writer = csv.writer(new_file, delimiter= self.delimiter ,quotechar= self.quote, quoting=self.quote_normals)
            writer.writerow(['name','birthday','country_code', 'email','id','teams'])
            for each in data['data']['users']:
                print(each)
                count +=1
                print(count)
                writer.writerow([each['name'],
                                each['birthday'],
                                each['country_code'],
                                each['email'],
                                each['teams']])
            if self.blob_bool:
                self.blob_upload("get_board_names")
                if self.rmv_file:
                    os.remove("get_board_names" + '.csv')
            else:
                print("not uploading")

if __name__ == "__main__":
    config(',','"').get_user_names()
    config(',','"').get_board_names()
    config(',','"').get_column_value_names()