# Bridge a provider-specific geometry payload

Use this pattern when one transform owns a provider representation and a later step or external system needs a binary
payload. The Binary field is intentional: it prevents Structure from treating a value created by one provider as a
value understood by another provider.

## Situation

An incoming WKT column is parsed with Sedona and must be sent to a system that expects Sedona EWKB. The downstream
system, not Structure, owns the EWKB decoder and compatibility test.

```python
from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark import sedona


class RawBoundary(Schema):
    wkt = string(nullable=False)


class EncodedBoundary(Schema):
    payload = binary(nullable=False)


@transform(geo_provider="sedona")
class EncodeBoundary(Transform):
    source = input(RawBoundary)
    encoded = output(EncodedBoundary)

    @step(input=source, output=encoded, geo_provider="sedona")
    def encode(self, row: RawBoundary) -> EncodedBoundary:
        shape = sedona.st_geomfromwkt(row.wkt, 4326)
        return EncodedBoundary(payload=sedona.st_asewkb(shape))
```

The output is Binary, not Geometry. It can cross a transform or provider boundary, but its meaning does not travel with
the field declaration. Record the codec, producer version, SRID behavior, and receiver test in the owning application.

## Receive deliberately

If a later Sedona step owns the same EWKB contract, it may decode the payload with `sedona.st_geomfromewkb`. If a native
Spark step or another provider receives the bytes, do not assume it can decode EWKB. Use its documented decoder only
after verifying the exact source/destination pair, including SRID and dimensionality preservation.

For standard WKB, carry the SRID separately when it matters: WKB itself does not include that fact. Structure does not
insert `st_setsrid` or choose a codec on the application's behalf.

## When not to use this recipe

Do not use Binary merely to bypass type checks inside one provider-scoped transform. Keep Geometry or Geography typed
while one provider owns it. Use a Binary boundary only for an external protocol, persistence format, or intentional
provider handoff.
