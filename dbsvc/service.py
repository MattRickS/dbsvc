from datetime import datetime, tzinfo
import functools
import json
import re
import traceback

import bottle
import marshmallow.exceptions
from marshmallow import ValidationError, fields, missing, Schema, validate
from marshmallow_enum import EnumField

from dbsvc import api, constants, exceptions


def parse_auth_token():
    auth_header = bottle.request.headers["Authorization"]
    assert auth_header.startswith("Bearer ")
    string_token = auth_header[7:]
    return json.loads(string_token)


def interface(request_cls: Schema, response_cls: Schema = None, status: int = 200):
    def _wrap(func):
        # *args is to allow pass through for `self` if present
        @functools.wraps(func)
        def _parser(*args, **path_kwargs):
            path_kwargs.update(bottle.request.query)
            path_kwargs.update(bottle.request.json or {})
            path_kwargs["auth_token"] = parse_auth_token()
            try:
                try:
                    req = request_cls().load(path_kwargs)
                except marshmallow.exceptions.ValidationError as e:
                    bottle.response.status = 400
                    resp = {
                        "message": "ValidationError",
                        "errors": e.normalized_messages(),
                        "status": 400,
                    }
                else:
                    resp = func(*args, req)
                    # Response schemas (if used) are not client side errors so
                    # should result in a 500 response if failing
                    if response_cls is not None:
                        resp = response_cls().load(resp)
                    bottle.response.status = status
            except (
                exceptions.InvalidFilters,
                exceptions.InvalidJoin,
                exceptions.InvalidSchema,
            ) as e:
                bottle.response.status = 400
                resp = {"message": str(e), "status": 400}
            except Exception as e:
                bottle.response.status = 500
                # TODO: Logger
                traceback.print_exc()
                resp = {"message": "InternalServerError", "status": 500}

            bottle.response.content_type = "application/json"
            return resp

        return _parser

    return _wrap


TABLE_COLUMN_PATTERN = r"(\w+\.)?([\w]+|\*)"


class FilterDataValidator(validate.Validator):
    def __call__(self, value):
        if not isinstance(value, dict):
            raise ValidationError("Filters must be a dictionary")

        invalid_keys = set(value).difference(constants.VALID_FILTER_KEYS)
        if invalid_keys:
            raise ValidationError(f"Invalid filter keys: {sorted(invalid_keys)}")

        for key, val in value.items():
            if key == constants.FILTER_OR:
                if not isinstance(val, list):
                    raise ValidationError(f"Filter '{key}' requires a list of filters")
                for subfilters in val:
                    self.__call__(subfilters)
            elif not isinstance(val, dict):
                raise ValidationError(
                    f"Comparison filter '{key}' requires a dict of column: value pairs"
                )
            else:
                for colname, colvalue in val.items():
                    if not re.match(TABLE_COLUMN_PATTERN, colname):
                        raise ValidationError(f"Invalid table/column format: {colname}")
                    # if isinstance(colvalue, dict):
                    #     raise ValidationError("")


TableField = functools.partial(fields.Str, validate=validate.Regexp(r"\w+"))
ColumnField = functools.partial(fields.Str, validate=validate.Regexp(r"\w+"))
TableColumnField = functools.partial(fields.Str, validate=validate.Regexp(TABLE_COLUMN_PATTERN))
FiltersField = functools.partial(fields.Dict, validate=FilterDataValidator())


class TokenSchema(Schema):
    username = fields.String()
    department = fields.String()
    expiry = fields.Integer()
    authctx = fields.String()
    groups = fields.List(fields.String())
    extra = fields.Dict()


class CreateRequest(Schema):
    tablename = TableField()
    values = fields.List(fields.Dict(keys=ColumnField()))
    auth_token = fields.Nested(TokenSchema)


class ReadRequest(Schema):
    tablename = TableField()
    colnames = fields.List(TableColumnField(), load_default=(constants.ALL_COLUMNS,))
    filters = FiltersField(load_default=None)
    joins = fields.Dict(load_default=None)
    limit = fields.Int(validate=validate.Range(1, 1000), load_default=None)
    ordering = fields.List(
        fields.Tuple((ColumnField(), EnumField(constants.Order))), load_default=None
    )
    auth_token = fields.Nested(TokenSchema)


class UpdateRequest(Schema):
    tablename = TableField()
    values = fields.Dict(keys=ColumnField())
    filters = FiltersField(load_default=None)
    auth_token = fields.Nested(TokenSchema)


class DeleteRequest(Schema):
    tablename = TableField()
    filters = FiltersField(load_default=None)
    auth_token = fields.Nested(TokenSchema)


class BatchCommandSchema(Schema):
    cmd = fields.Str(validate=validate.OneOf(["create", "read", "update", "delete"]))
    kwargs = fields.Dict()


class BatchRequest(Schema):
    commands = fields.List(fields.Nested(BatchCommandSchema), validate=validate.Length(min=1))
    auth_token = fields.Nested(TokenSchema)


class EntityResource:
    def __init__(self, db: api.Database) -> None:
        self._db = db

    @interface(CreateRequest, status=201)
    def create(self, request: CreateRequest):
        tablename = request["tablename"]
        values = self._modify_create_values(tablename, request["values"], request["auth_token"])
        ids = self._db.create(tablename, values)
        return {"ids": ids}

    @interface(ReadRequest)
    def read(self, request: ReadRequest):
        rows = self._db.read(
            request["tablename"],
            colnames=request["colnames"],
            filters=request["filters"],
            joins=request["joins"],
            limit=request["limit"],
            ordering=request["ordering"],
        )
        return {"entities": [self._response_entity(row) for row in rows]}

    def _validate_update_values(self, tablename: str, values: dict, auth_token: TokenSchema):
        permitted_fields = {
            "Output": [
                "is_approved",
                "approved_at",
                "approved_by",
                "is_starred",
                "starred_at",
                "starred_by",
                "status",  # Only if a mutable value...
                "status_at",
                "status_by",
            ],
            # XXX: How to enforce tool access? Would be a pain to do separate login for that
            "Shot": [
                "frame_start",  # Only be certain tools
                "frame_end",  # Only be certain tools
            ],
        }
        invalid = set(values).difference(permitted_fields.get(tablename, ()))
        if invalid:
            raise Exception(f"Does not have permissions to update fields: {invalid}")

    @interface(UpdateRequest)
    def update(self, request: UpdateRequest):
        tablename = request["tablename"]
        values = self._modify_update_values(tablename, request["values"], request["auth_token"])
        count = self._db.update(tablename, values, filters=request["filters"])
        return {"count": count}

    @interface(DeleteRequest)
    def delete(self, request: DeleteRequest):
        count = self._db.delete(request["tablename"], filters=request["filters"])
        return {"count": count}

    @interface(BatchRequest)
    def batch(self, request: DeleteRequest):
        commands = request["commands"]
        for cmd in commands:
            if cmd["cmd"] == "create":
                kwargs = cmd["kwargs"]
                kwargs["list_of_values"] = self._modify_create_values(
                    kwargs["tablename"], kwargs["list_of_values"], request["auth_token"]
                )
            elif cmd["cmd"] == "update":
                kwargs = cmd["kwargs"]
                kwargs["values"] = self._modify_update_values(
                    kwargs["tablename"], kwargs["values"], request["auth_token"]
                )
        results = self._db.batch(commands)
        return {"results": results}

    def _response_entity(self, row):
        return {
            key: int(val.timestamp()) if isinstance(val, datetime) else val
            for key, val in row.items()
        }

    def _modify_create_values(self, tablename, list_of_values, auth_token):
        username = auth_token["username"]
        CREATOR_FIELD = "created_by"
        if self._db.has_field(tablename, CREATOR_FIELD):
            for entity in list_of_values:
                entity[CREATOR_FIELD] = username

        return list_of_values

    def _modify_update_values(self, tablename, values, auth_token):
        username = auth_token["username"]

        for key, value in values.items():
            if key.endswith("_at"):
                # Expected standard is to use "*_at" and "*_by" for metadata of time and user
                user_field = key[:-3] + "_by"
                if self._db.has_field(tablename, user_field):
                    values[user_field] = username
                if isinstance(value, int):
                    values[key] = datetime.fromtimestamp(value)

        return values


def build(uri: str, schema: api.Schema, id_manager: api.IDManager = None) -> bottle.Bottle:
    db = api.Database(uri, schema, id_manager=id_manager)
    resource = EntityResource(db)

    service = bottle.Bottle()

    path = "/<tablename>/"
    service.get(path, callback=resource.read)
    service.post(path, callback=resource.create)
    service.put(path, callback=resource.update)
    service.delete(path, callback=resource.delete)
    service.post("/$batch/", callback=resource.batch)

    return service
