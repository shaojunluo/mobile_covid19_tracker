import json
import requests
from django.http import HttpResponse
import json


def login(req):
    dic = get_post_dic(req)
    usr = dic['usr']
    pwd = dic['pwd']
    return_dic = {
        "err": 0,
        "Status": "login ok",
        "mobileId": "89b99652-5f66-4203-85fd-378ef008ebe5"
    }
    return HttpResponse(json.dumps(return_dic))


def registration(req):
    dic = get_post_dic(req)
    usr = dic['usr']
    pwd = dic['pwd']
    return_dic = {
        "err": 0,
        "Status": "reg ok",
        "mobileId": "89b99652-5f66-4203-85fd-378ef008ebe5"
    }
    return HttpResponse(json.dumps(return_dic))


def health_status(req):
    dic = get_post_dic(req)
    uuid = dic['mobileId']
    return_dic = {
        "err": 0,
        "Status": 1,
        "Msg": "Get Status OK"
    }
    return HttpResponse(json.dumps(return_dic))


def upload_track(req):
    dic = get_post_dic(req)
    uuid = dic['mobileId']
    track = dic['track']
    return_dic = {
        "err": 0,
        "Msg": "upload OK"
    }
    return HttpResponse(json.dumps(return_dic))


def heat_point(req):
    dic = get_post_dic(req)
    lat = dic['lat']
    lng = dic['lng']
    uuid = dic['mobileId']
    return_dic = {
        "err": 0,
        "points": [
            {
                "time_start": 123456789,
                "time_end": 234567890,
                "location": [
                    "51.509979999999999,-0.13300000000000001,1",
                ]
            },
            {
                "time_start": 345678901,
                "time_end": 456789012,
                "location": [
                    "51.520000000000003,-0.13400000000000001,10",
                ]
            },
            {
                "time_start": 567890123,
                "time_end": 678901234,
                "location": [
                    "51.540000000000003,-0.13500000000000001,10",
                ]
            },
        ]
    }
    return HttpResponse(json.dumps(return_dic))

def get_post_dic(req):
    body = req.body
    return json.loads(body)
