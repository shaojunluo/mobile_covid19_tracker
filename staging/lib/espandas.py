"""Reading and writing pandas DataFrames to ElasticSearch"""
import pandas as pd
import numpy as np

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

class Espandas(object):
    """Reading and writing pandas DataFrames to ElasticSearch"""
    def __init__(self, es):
        """
        Construct an espandas reader/writer
        :params es: elasticsearch client
        """
        self.client = es
        self.successful_ = None
        self.failed_ = None
        self.uid_name = None

    def es_read(self, keys, index, doc_type):
        """
        Read from an ElasticSearch index and return a DataFrame
        :param keys: a list of keys to extract in elasticsearch
        :param index: the ElasticSearch index to read
        :param doc_type: the ElasticSearch doc_type to read
        """
        
        self.successful_ = 0
        self.failed_ = 0

        # Collect records for all of the keys
        records = []
        for key in keys:
            try:
                record = self.client.get(index=index, doc_type=doc_type, id=key)
                self.successful_ += 1
                if '_source' in record:
                    records.append(record['_source'])
            except NotFoundError as nfe:
                print('Key not found: %s' % nfe)
                self.failed_ += 1

        # Prepare the records into a single DataFrame
        df = None
        if records:
            df = pd.DataFrame(records).fillna(value=np.nan)
            df = df.reindex(sorted(df.columns), axis=1)
        return df


    def es_write(self, df, index, uid_name = None, geo_col_dict=None,thread_count = 1):
        """
        Insert a Pandas DataFrame into ElasticSearch
        :param df: the DataFrame, must contain the column 'indexId' for a unique identifier
        :param index: the ElasticSearch index
        """
        
        if not isinstance(df, pd.DataFrame):
            raise ValueError('df must be a pandas DataFrame')

        if not self.client.indices.exists(index=index):
            print('index does not exist, creating index')
            self.client.indices.create(index)
        
        if (uid_name is not None)  and (len(df[uid_name]) != len(pd.unique(df[uid_name]))):
            raise ValueError('uid must be unique')
        
        # helper functions to parse geotypes
        def parse_geoshape(record, geo_col_dict):
            # null case
            if geo_col_dict is None:
                return record
            # duplicate original record
            record_new = record
            # transform geoshape
            for col, geo_type in geo_col_dict.items():
                record_new[col] = {'type' : geo_type , "coordinates" : record[col]} 
            return record_new

        # generate input dictionary
        def generate_dict(df, uid_name, index, geo_col_dict = None):
            """
            Generator for creating a dict to be inserted into ElasticSearch
            for each row of a pd.DataFrame
            :param df: the input pd.DataFrame to use, must contain an '_id' column
            """
            records = df.to_dict(orient='records')
            # parse geotypes before return 
            for r in records:
                record = parse_geoshape(r,geo_col_dict)
                if uid_name is None:
                    yield {'_index': index, '_source': record}
                            
                else:
                    yield {'_index': index,
                           '_id': record[uid_name],
                           '_source': record}
        
        # use parallel_bulk streamming
        actions = helpers.parallel_bulk(self.client, 
                                        generate_dict(df,uid_name,index), 
                                        thread_count= thread_count)
        for success, info in actions:
            if not success: 
                print('Doc failed', info)
        
        # use 1 thread for safe process.
        # helpers.bulk(self.client, generate_dict)
