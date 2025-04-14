from flask import request
from flask_restful import Resource
from flask_wtf import FlaskForm


# Empty validation class will return True always (default validation class)
class EmptyValidationClass(FlaskForm):
    pass


class BaseController(Resource):
    validation_class = EmptyValidationClass

    def validate(self, data):
        form = self.get_validation_class()(data, meta={"csrf": False})
        is_valid = form.validate()
        return {"is_valid": is_valid, "errors": form.errors, "form": form}

    def get_view_parameter(self, key):
        return request.__dict__.get("view_args").get(key)

    def get_validation_class(self):
        if not self.validation_class:
            raise ValueError("validation class does not exist")
        return self.validation_class

    def get(self, endpoint: str):
        validated = self.validate(request.args)

        if not validated.get("is_valid"):
            raise ValueError(validated.get("errors"))

        form = validated.get("form")
        return form
