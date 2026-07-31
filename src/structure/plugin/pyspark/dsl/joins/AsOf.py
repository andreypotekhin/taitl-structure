from enum import Enum


class AsOf(Enum):
    BACKWARD = "backward"
    FORWARD = "forward"
    NEAREST = "nearest"
