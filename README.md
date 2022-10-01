# dbsvc

No one wants to expose their database directly, this is a _very_ basic, work in progress generic database service to start from.

Provides CRUD operations for tables.

Service's default database is an in-memory sqlite db, it's schema is a very basic M:M Shot:Asset relationship for testing.

### Example Usage
Setup environment
```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements/requirements.in
```

Run the service
```sh
PYTHONPATH:$PYTHONPATH:. python -m dbsvc run
```

Query it
```sh
# Insert some rows and query one of them
curl -X POST -H 'Content-Type: application/json' -d '{"values": [{"id": 1, "name": "s100"}, {"id": 2, "name": "s200"}]}' 'localhost:8080/Shot/'
curl -X GET -H 'Content-Type: application/json' -d '{"filters": {"eq": {"id": 1}}}' 'localhost:8080/Shot/'
# Update a row and confirm it was the only one affected
curl -X PUT -H 'Content-Type: application/json' -d '{"values": {"name": "s300"}, "filters": {"eq": {"id": 2}}}' 'localhost:8080/Shot/'
curl -X GET 'localhost:8080/Shot/'
# Delete a row and confirm it's gone
curl -X DELETE -H 'Content-Type: application/json' -d '{"filters": {"eq": {"id": 1}}}' 'localhost:8080/Shot/'
curl -X GET 'localhost:8080/Shot/'
```