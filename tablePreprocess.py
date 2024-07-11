import pandas as pd
import numpy as np
from urllib import parse

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
        df_arr_25 = []
        df_arr_26 = []
        for file_name in self.file_arr:
            if file_name.find('2024-06-25')!=-1:
                df = pd.read_parquet(file_name)
                df_arr_25.append(df)
            elif file_name.find('2024-06-26')!=-1:
                df = pd.read_parquet(file_name)
                df_arr_26.append(df)
                
        df_25 = pd.concat(df_arr_25)
        df_26 = pd.concat(df_arr_26)
        
        df_25.to_parquet('./data/parquet_preprocess/25.parquet')
        df_26.to_parquet('./data/parquet_preprocess/26.parquet')
    
    def loadData(self):
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
            
        for file_name in self.file_arr:
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
            df.to_parquet(file_name)
        
        self.concatDataFrames()
        