from structure import Date, Integer, Long, Schema, String, field


class Race(Schema):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)
    date = field(Date(), nullable=False)
    river = field(String(), nullable=False)
    venue = field(String(), nullable=False)
    city = field(String(), nullable=False)
    country = field(String(), nullable=False)


class Gate(Schema):
    race_id = field(String(), nullable=False)
    number = field(Integer(), nullable=False)
    direction = field(String(), nullable=False)
    sector = field(String(), nullable=False)


class Paddler(Schema):
    race_id = field(String(), nullable=False)
    id = field(String(), nullable=False)
    bib = field(Integer(), nullable=False)
    name = field(String(), nullable=False)
    country = field(String(), nullable=False)
    division = field(String(), nullable=False)


class RaceWinner(Schema):
    race_id = field(String(), nullable=False)
    run_id = field(String(), nullable=False)
    paddler_id = field(String(), nullable=False)
    rank = field(Integer(), nullable=False)
    adjusted_millis = field(Long(), nullable=False)
