from rest_framework import serializers


class IntegerOrHyphenField(serializers.Field):
    def to_internal_value(self, data):
        if data == "-":
            return "-"

        try:
            return int(data)
        except ValueError:
            raise serializers.ValidationError("Must be an integer or '-'.")

    def to_representation(self, value):
        return str(value)
