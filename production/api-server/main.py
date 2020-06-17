from fastapi import FastAPI
import json
from elasticsearch import Elasticsearch, helpers

app = FastAPI()

es_usr = Elasticsearch("http://localhost:9200", timeout=600)
es_index = "fortaleza_infected"


@app.get("/api")
def read_root():
    return {"name": "api-server", "status": "active"}


@app.get("/api/users/{device_id}/selfreport")
def get_selfreport(device_id: str):
    query = {"query": {"term": {"mobileId": {"value": device_id}}}}
    try:
        return es_usr.search(index=es_index, body=query)
    except Exception as e:
        return {"error": str(e)}
