from utils.validator import Validator
from attr import define, field, asdict
from typing import List, Union, Tuple
import attrs


@define
class Competitor:
    origin: str = field()
    destination: str = field()
    competitors: List[str] = field(factory=list, validator=attrs.validators.instance_of(list))

    @competitors.validator
    def validate_competitors(self, _, value):
        assert len(value) > 0
        for item in value: Validator.is_string(item)

    @origin.validator
    def validate_origin(self, _, value):
        Validator.is_string(value)

    @destination.validator
    def validate_destination(self, _, value):
        Validator.is_string(value)


@define
class DefaultCurrency:
    value: str = field(validator=attrs.validators.instance_of(str))


@define
class Market:
    currency: str = field()
    direction: str = field()
    numberOfDays: int = field()
    startOffset: int = field()
    stayDuration: int = field()
    orig: str = field()
    dest: str = field()

    @direction.validator
    def validate_direction(self, _, value):
        Validator.is_in(value, ['RT', 'OW'])

    @numberOfDays.validator
    def validate_number_of_days(self, _, value):
        Validator.is_number(value, 1)

    @startOffset.validator
    def validate_start_offset(self, _, value):
        Validator.is_number(value, 0)

    @stayDuration.validator
    def validate_stay_duration(self, _, value):
        Validator.is_number(value, 0)

    @currency.validator
    def validate_stay_duration(self, _, value):
        Validator.is_string(value)

    @orig.validator
    def validate_stay_duration(self, _, value):
        Validator.is_string(value)

    @dest.validator
    def validate_stay_duration(self, _, value):
        Validator.is_string(value)


@define
class ConfigurationEntry:
    key: str = field(validator=attrs.validators.instance_of(str))
    value: List[Union[Competitor, DefaultCurrency, Market]] = field(factory=list)
    description: str = field(validator=attrs.validators.instance_of(str), default='')


@define
class _Configuration:
    customer: str = field(validator=attrs.validators.instance_of(str))
    configurationEntries: List[ConfigurationEntry] = field()


class Configuration:

    def __init__(self, config):
        entries = self.create_entries(config)
        self.data = _Configuration(customer=config['customer'], configurationEntries=entries)

    def get_entry(self, data, key):
        return list(filter(lambda obj: obj['key'] == key, data['configurationEntries']))[0]['value']

    def create_competitors(self, data) -> None:
        competitors = self.get_entry(data, 'COMPETITORS')
        competitors = [Competitor(**competitor) for competitor in competitors]
        self.competitors = competitors
        return competitors

    def competitor_at(self, index: int, key: str = None):
        obj = self.competitors[index]
        if not key: return obj
        return getattr(obj, key)

    def market_at(self, index: int, key: str = None) -> Market:
        obj = self.markets[index]
        if not key: return obj
        return getattr(obj, key)

    def market_for_competitor(self, index: int) -> Tuple[str, str]:
        """ get market for a competitor """
        competitor = self.competitor_at(index)
        return competitor.origin, competitor.destination

    def market_competitor(self, idx: int = None, origin: str = None, destination: str = None) -> Union[List[str], None]:
        """ 
            get competitors for a market 
            if index is provided get market by index otherwise get market by origin|destination
        """
        if origin and destination:
            markets = list(
                filter(lambda obj: obj.origin == origin and obj.destination == destination, self.competitors))
            if not markets: return
            return markets[0].competitors
        market = self.competitor_at(idx)
        return market.competitors

    def create_default_currency(self, data) -> None:
        currency = self.get_entry(data, 'DEFAULT_CURRENCY')
        currency = DefaultCurrency(currency)
        self.default_currency = currency
        return currency

    def create_markets(self, data) -> None:
        markets = self.get_entry(data, 'MARKETS')
        markets = [Market(**market) for market in markets]
        self.markets = markets
        return markets

    def create_entries(self, data) -> None:
        competitors = self.create_competitors(data)
        markets = self.create_markets(data)
        default_currency = self.create_default_currency(data)
        keys = ['COMPETITORS', "MARKETS", 'DEFAULT_CURRENCT']
        return [ConfigurationEntry(key=key, value=value) for value, key in
                zip([competitors, markets, default_currency], keys)]

    @property
    def entries(self) -> List[ConfigurationEntry]:
        return self.data.configurationEntries

    @property
    def customer(self) -> str:
        return self.data.customer

    @property
    def currency(self) -> str:
        return self.default_currency.value

    def as_dict(self):
        return {
            'customer': self.customer,
            'competitors': [asdict(competitor) for competitor in self.competitors],
            'markets': [asdict(market) for market in self.markets],
            'default_currency': asdict(self.default_currency)
        }
