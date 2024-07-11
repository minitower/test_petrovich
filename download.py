import pandas as pd
import wget
import os

class FileDownloader():
    """
    Class for download files from Yandex drive storage and 
    convert them to parquet files for compression
    """
    def __init__(self, url_arr: list):
        """
        Init function for download files from Yandex drive
        storage and save it in ./data folder
        
        url_arr (list): list of url adresses in test file
        """
        self.url_arr = url_arr
        self.csv_path = './data/csv'
        os.makedirs(self.csv_path, exist_ok=True)
        self.parquet_path = './data/parquet'
        os.makedirs(self.parquet_path, exist_ok=True)
        
    def preprocess(self, file_name:str):
        """
        Function for convert csv file to parquet
        
        file_name (Path): name of file to convert
        """
        df = pd.read_csv(self.csv_path+'/'+file_name+'.csv')
        df.columns = ['client_id','screen_resolution', 
                      'client_hints', 'ga_session_id',
                      'ga_session_number', 'page_location', 
                      'page_title', 'user_id', 'event_name', 
                      'items', 'user_agent', '`_ga`',
                      'ga4cat', 'ga4act', 'ga4lab',
                      'eventTimestamp', 'u__typeDevice']
        df.to_parquet(self.parquet_path+'/'+file_name+'.parquet')
    
    def getFileName(self, url:str):
        """
        Function for geting the file name from URL
        
        url (str): URL of file on Yandex drive storage
        """
        return url.split('/')[-1].split('.')[0]

    def remove(self, file_name:str):
        """
        Function for remove file from local storage
        
        file_name (str): name of file to remove
        """
        os.remove(self.csv_path+'/'+file_name+'.csv')
    
    def download(self):
        """
        Function for download file from Yandex drive storage
        """
        for url in self.url_arr:
            file_name = self.getFileName(url)
            wget.download(url, self.csv_path + '/' + file_name+'.csv')
            self.preprocess(file_name)
            self.remove(file_name)