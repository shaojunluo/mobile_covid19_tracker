API SERVER
===============

### Requirements

Install the dependencies.

```bash
$ cd api-server
$ pip install -r requirements.txt
```

### Development Mode

Start the development mode as

```bash
$ cd api-server
$ uvicorn main:app --reload
```

Send a request to see if the api server is alive.

```bash
$ curl localhost:8000/api
```

You should see the output

```json
{
  "name":"api-server",
  "status":"active"
}
```