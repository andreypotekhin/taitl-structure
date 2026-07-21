from datetime import datetime as DateTime
from decimal import Decimal

from structure import Schema
from structure.plugin.pyspark import array, date, decimal, double, long, string


class Address(Schema):
    street: str


class Order(Schema):
    name: str = string()
    quantity: int = long()
    ratio: float = double()
    total: Decimal = decimal(12, 2)
    ordered_on: date
    observed_at: DateTime
    tags: list[str] = array(string())
    address: Address
