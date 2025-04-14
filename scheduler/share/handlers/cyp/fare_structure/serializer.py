from core.fields.serializer import IntegerOrHyphenField
from rest_framework import serializers


class MarketSerializer(serializers.Serializer):
    origin = serializers.CharField()
    destination = serializers.CharField()


class CabinSerializer(serializers.Serializer):
    code = serializers.CharField()


class ClassSerializer(serializers.Serializer):
    code = serializers.CharField()


class FareSerializer(serializers.Serializer):
    fare_basis = serializers.CharField()
    surcharge_amt = IntegerOrHyphenField()
    base_fare = serializers.IntegerField()
    currency = serializers.CharField()
    q = IntegerOrHyphenField()
    q_currency = serializers.CharField()
    yq = IntegerOrHyphenField()
    yq_currency = serializers.CharField()
    yr = serializers.CharField()
    yr_currency = IntegerOrHyphenField()
    fare_family: int = serializers.IntegerField()
    first_ticketing_date = IntegerOrHyphenField()
    last_ticketing_date = IntegerOrHyphenField()
    observed_at = IntegerOrHyphenField()
    travel_day_of_week = serializers.CharField()
    country_of_sale = serializers.CharField()
    min_stay = IntegerOrHyphenField()
    advance_purchase_days = IntegerOrHyphenField()
