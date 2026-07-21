from structure import Schema
from structure.plugin.pyspark import *


class DeviceType(Schema):
    id = string(nullable=False)
    nature = string(nullable=False)
    type = string(nullable=False)
    make = string(nullable=False)
    platform = string(nullable=False)
    model = string(nullable=False)
    version = string(nullable=False)


class Software(Schema):
    id = string(nullable=False)
    nature = string(nullable=False)
    name = string(nullable=False)
    vendor = string(nullable=False)
    version = string(nullable=False)


class App(Software):
    pass


class OS(Software):
    pass


class Scanner(Software):
    pass


class Device(Schema):
    id = string(nullable=False)
    device_type_id = string(nullable=False)
    owner_id = string(nullable=False)
    os_id = string(nullable=False)
    apps = array(struct(App), contains_null=False, nullable=False)
    vuln_ids = array(string(), contains_null=False, nullable=False)
