from core.fields.serializer import IntegerOrHyphenField
from core.validators import is_reprsenting_date, is_reprsenting_time
from rest_framework import serializers


class LegSerializer(serializers.Serializer):
    origin = serializers.CharField(max_length=3, min_length=3)
    destination = serializers.CharField(max_length=3, min_length=3)
    flight_number: serializers.CharField()
    dept_date: serializers.IntegerField()
    dept_time: serializers.IntegerField()

    def validate_dept_date(self, value):
        if not is_reprsenting_date(value):
            raise serializers.ValidationError(f"{value} is not valid date")
        return value

    def validate_dept_time(self, value):
        if not is_reprsenting_time(value):
            raise serializers.ValidationError(f"{value} is not valid time")
        return value


class SegmentSerializer(LegSerializer):
    carrier_code = serializers.CharField(max_length=2, min_length=2)


class CabinSerializer(serializers.Serializer):
    code = serializers.CharField(max_length=1)
    total_booking = IntegerOrHyphenField()
    total_group_booking = IntegerOrHyphenField()
    # overbooking_level = IntegerOrHyphenField()
    capacity = IntegerOrHyphenField()
    allotment = serializers.IntegerField(allow_null=True)
    available_seats = serializers.IntegerField(allow_null=True)
    load_factor = serializers.SerializerMethodField()

    def get_load_factor(self, obj) -> int:
        if type(obj["capacity"]) is str or type(obj["total_booking"]) is str:
            return 0
        return round(obj["total_booking"] / obj["capacity"] * 100)
