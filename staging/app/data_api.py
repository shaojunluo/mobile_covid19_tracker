from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timedelta
import json

class mobileCovidTracker:
    
    def __init__(self, config, prefix = ''):
        with open(config,'r') as f:
            self.params = json.load(f)
        
        self.start_session()
        self.prefix = prefix
        # task dictionaries
        self.tasks = {'healthStatus': self.__process_heath_status,
                      'selfReport': self.__process_selfreport,
                      'gps':  self.__process_gps_signal,
                      'maps': self.__process_map_points
                     }
            
    def __del__(self):
        self.close_session()
        
    
    # start elastic search session
    def start_session(self):
        es_param = self.params['elasticsearch']
        self.es_rec = Elasticsearch([es_param['records']['host'] +':'+ es_param['records']['port']],timeout=600) 
        self.es_usr = Elasticsearch([es_param['users']['host'] +':'+ es_param['users']['port']],timeout=600)
        self.es_map = Elasticsearch([es_param['maps']['host'] +':'+ es_param['maps']['port']],timeout=600)
        
    def close_session(self):
        self.es_rec.transport.connection_pool.close()
        self.es_usr.transport.connection_pool.close()
        self.es_map.transport.connection_pool.close()
        
    def process_requests(self, input_str):
        # handling input
        try:
            dict_in = json.loads(input_str)
        except:
            return json.dumps({'err': 5, 'msg': "json parse failed"})
        # parse request type
        req_type = dict_in['type']
        # executing
        response = self.tasks[req_type](dict_in)
        return response
    
    # ============================ Functions for processing tasks ====================#
    def __generate_record(self,index, m_id, track):
        fields = track.split(',')
        # get to m-second
        time_stamp = int(fields[0]+'000')
        record = {'mobileId': m_id, 
                  'dateSource': 'app',
                  'acquisitionTime':time_stamp,
                  'location': fields[1] + ',' + fields[2],
                  'movingRate': 0.0,
            }
        return {'_index': index, '_source': record}
    
    def __process_gps_signal(self, input_dict):
        # determine index to put
        index = self.prefix + datetime.today().strftime('%m_%d')
        m_id = input_dict['mobileId']
        data = [self.__generate_record(index, m_id, t) for t in input_dict['track']]
        # put into elasticsearch
        helpers.bulk(self.es_rec, data)
        return json.dumps({'type': input_dict['type'], 'err': '0','msg': 'OK'}) 
    
    def __process_heath_status(self, input_dict):
        index = self.prefix + 'most_recent_risky_ids'
        body = {"size" : 1,
                "query": {
                    "term": {
                        "mobileId" : input_dict['mobileId']
                    }
                }
            }
        result = self.es_usr.search(index = index, body = body)
        if len(result['hits']['hits']) > 0:
            status = result['hits']['hits'][0]['_source']['status']
        else:
            status = 0
        response = {'type': input_dict['type'],
                    'err': 0,
                    'status': status,
                    'msg': 'OK'}
        return json.dumps(response)
    
    def __process_selfreport(self,input_dict):
        index = self.prefix + 'infected'
        # generate self report record
        record = {'mobileId': input_dict['mobileId'],
                  'symptomDate': input_dict['symptomDate'],
                  'testDate': input_dict['testDate'],
                  'source': 'selfreport'
        }
        self.es_usr.index(index = index, body = record)
        return json.dumps({'type': input_dict['type'], 'err': '0','msg': 'OK'})
    
    def __generate_map_queries(self, input_dict, time_start, time_end):
        # generate space query
        dist = int(4e7/(2**(input_dict['zoomlevel'])))
        space_queries = {"must" : {"match_all":{}},
                            "filter" : {
                                "geo_distance" : {
                                    "distance" : f"{dist}km",
                                    "location" : input_dict['location']
                                    }
                                }
                            }
        # generate map for every day
        time_queries = {"range" : {
                        "minTime" : {
                            "gte": time_start.strftime('%Y-%m-%dT%H:%M:%S'),
                            "lte": time_end.strftime('%Y-%m-%dT%H:%M:%S')
                        }
                    }
                }
        # combine queries
        body = {"size" : 1000,
            "query": {
                "bool": {
                    "must": [
                        time_queries,
                        {"bool": space_queries}
                    ]
                }
            }
        }
        return body
    
    def __generate_points(self, result):
        response = []
        for hit in result['hits']['hits']:
            res = hit['_source']
            response.append(f"{res['location'][1]},{res['location'][0]},{res['delta']}")
        return response
    
    def __process_map_points(self, input_dict):
        layer = input_dict['layer']
        index = self.prefix + f'red_zones_layer_{layer}'
        points = []
        # generate geoquery
        for day in range(1,8):
            query_day = datetime(2020,4,30)-timedelta(days = day)
            # day and night time
            for h in [6,18]:
                # day time
                time_start = datetime(query_day.year, query_day.month,query_day.day, h)
                time_end =  time_start + timedelta(hours = 12)
                body = self.__generate_map_queries(input_dict,time_start, time_end)
                # generate map for day time:
                result = self.es_map.search(index = index, body = body)
                locs = self.__generate_points(result)
                points.append({'time_start': int(time_start.timestamp()),
                               'time_end': int(time_end.timestamp()),
                               'location': locs})
        return json.dumps({'type': input_dict['type'], 'err':0, 'msg':'OK', 'points': points})
    
# test cases:
if __name__ == "__main__":
    client = mobileCovidTracker('config_app.json',prefix = 'fortaleza_')
    # read examples
    with open('test_examples.json','r') as f:
        examples = json.load(f) 
    # running examples
    for req in examples:
        print('Request:')
        print(json.dumps(req))
        print('Response:')
        print(client.process_requests(json.dumps(req)))