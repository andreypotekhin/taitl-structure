from enum import Enum


class Join(Enum):
    LEFT = "left"
    INNER = "inner"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
