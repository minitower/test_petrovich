import pandas as pd
import numpy as np
from urllib import parse
import pyarrow
import pyarrow.parquet as pq
import os

class TablePreprocess():
    """
    Class for concat files to each other and create some columns
    for next BI analysis
    """
    def __init__(self, url_arr: list) -> None:
        """
        Init function for tablePreprocess class

        Args:
            url_arr (list): list with URL of Yandex disk storage
        """
        self.file_arr = self.filenameFromURL(url_arr)
        
    def filenameFromURL(self, url_arr: list):
        """
        Function for create file_arr from url array
        
        Args:
            url_arr (list): list with URL of Yandex disk storage
        """
        return ['./data/parquet/'+url.split('/')[-1].split('.')[0]+'.parquet' 
                for url in url_arr]
        
    def concatDataFrames(self):
        """
        Function for concat data frames with each other
        by date parameter
        """
        for i, file_name in enumerate(self.file_arr):
            if file_name.find('2024-06-25')!=-1:
                os.rename(file_name, './data/parquet/25/'+str(i))
            elif file_name.find('2024-06-26')!=-1:
                os.rename(file_name, './data/parquet/26/'+str(i))
        self.combine_parquet_files('./data/parquet/25/', 
                                   './data/parquet_preprocess/25.parquet')
        self.combine_parquet_files('./data/parquet/26/', 
                                   './data/parquet_preprocess/26.parquet')
    
    def combine_parquet_files(self, input_folder, target_path):
        """
        Function for combine parquet files together
        
        Source:
            https://gist.github.com/l1x/76dab6445b6d55396c622f915c755a17
        """
        try:
            files = []
            for file_name in os.listdir(input_folder):
                files.append(pq.read_table(os.path.join(input_folder, file_name)))
            with pq.ParquetWriter(target_path,
                    files[0].schema,
                    version='2.6',
                    compression='gzip',
                    use_dictionary=True,
                    data_page_size=2097152,
                    write_statistics=True) as writer:
                for f in files:
                    writer.write_table(f)
        except Exception as e:
            print(e)
    
    def loadData(self, file_name:str):
        """
        Function for load data from parquet files
        """
        def utmFromURL(url):
            parsed_url = parse.urlparse(url)
            return parse.parse_qs(parsed_url.query)
        
        def utmSourceFromQuery(query):
            try:
                return dict(query)['utm_source'][0]
            except KeyError:
                pass
        
        def utmMediumFromQuery(query):
            try:
                return dict(query)['utm_medium'][0]
            except KeyError:
                pass
                
        def utmCampaignFromQuery(query):
            try:
                return dict(query)['utm_campaign'][0]
            except KeyError:
                pass
            
        def utmContentFromQuery(query):
            try:
                return dict(query)['utm_content'][0]
            except KeyError:
                pass
            
        def utmTermFromQuery(query):
            try:
                return dict(query)['utm_term'][0]
            except KeyError:
                pass
            
        def utmIdFromQuery(query):
            try:
                return dict(query)['utm_id'][0]
            except KeyError:
                pass
            
        df = pd.read_parquet(file_name)
        df['ga4lab'] = df['ga4lab'].map(lambda x: parse.unquote(x), na_action='ignore')
        df['page_location'] = df['page_location'].map(lambda x: parse.unquote(x), na_action='ignore')
        df['url_query'] = df.apply(
            lambda x: utmFromURL(x.page_location), axis=1)
        df['utm_source'] = df.apply(
            lambda x: utmSourceFromQuery(x.url_query), axis=1)
        df['utm_medium'] = df.apply(
            lambda x: utmMediumFromQuery(x.url_query), axis=1)
        df['utm_campaign'] = df.apply(
            lambda x: utmCampaignFromQuery(x.url_query), axis=1)
        df['utm_content'] = df.apply(
            lambda x: utmContentFromQuery(x.url_query), axis=1)
        df['utm_term'] = df.apply(
            lambda x: utmTermFromQuery(x.url_query), axis=1)
        df['utm_id'] = df.apply(
            lambda x: utmIdFromQuery(x.url_query), axis=1)
        try:
            df.to_parquet(file_name)
        except pyarrow.lib.ArrowNotImplementedError:
            df['url_query']=np.nan
            df.to_parquet(file_name)
        