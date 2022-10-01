import functools
import re
import traceback

import bottle
import marshmallow.exceptions
from marshmallow import ValidationError, fields, Schema, validate
from marshmallow_enum import EnumField

from dbsvc import api, constants, exceptions


def interface(request_cls: Schema, response_cls: Schema = None, status: int = 200):
    def _wrap(func):
        # *args is to allow pass through for `self` if present
        @functools.wraps(func)
        def _parser(*args, **path_kwargs):
            path_kwargs.update(bottle.request.query)
            path_kwargs.update(bottle.request.json or {})
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
            else:
                bottle.response.status = status

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

Ordering = fields.Tuple((ColumnField(), EnumField(constants.Order)))


class CreateRequest(Schema):
    tablename = TableField()
    values = fields.List(fields.Dict(keys=ColumnField()))


class ReadRequest(Schema):
    tablename = TableField()
    colnames = fields.List(fields.Nested(TableColumnField), load_default=(constants.ALL_COLUMNS,))
    filters = FiltersField(load_default=None)
    joins = fields.Dict(load_default=None)
    limit = fields.Int(validate=validate.Range(1, 1000), load_default=None)
    ordering = fields.List(fields.Nested(Ordering), load_default=None)


class UpdateRequest(Schema):
    tablename = TableField()
    values = fields.Dict(keys=ColumnField())
    filters = FiltersField(load_default=None)


class DeleteRequest(Schema):
    tablename = TableField()
    filters = FiltersField(load_default=None)


class EntityResource:
    def __init__(self, db: api.Database) -> None:
        self._db = db

    @interface(CreateRequest, status=201)
    def create(self, request: CreateRequest):
        # TODO: Generate and return IDs
        self._db.create(request["tablename"], request["values"])

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
        return {"entities": list(rows)}

    @interface(UpdateRequest)
    def update(self, request: UpdateRequest):
        self._db.update(request["tablename"], request["values"], filters=request["filters"])

    @interface(DeleteRequest, status=204)
    def delete(self, request: DeleteRequest):
        self._db.delete(request["tablename"], filters=request["filters"])


def build() -> bottle.Bottle:
    # TODO: Configurable database args
    db = api.Database("sqlite://")
    resource = EntityResource(db)

    service = bottle.Bottle()

    path = "/<tablename>/"
    service.get(path, callback=resource.read)
    service.post(path, callback=resource.create)
    service.put(path, callback=resource.update)
    service.delete(path, callback=resource.delete)

    return service
