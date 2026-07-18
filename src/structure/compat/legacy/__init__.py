"""Compatibility for retired method-level transform syntax."""

import inspect

from structure.core.dsl.model.transforms.transform_api import _decorate_transform_method, transform as _transform


def transform(target=None, **kwargs):
    """Retain the former class-or-method ``@transform`` dispatcher."""

    def decorate(item):
        if inspect.isfunction(item):
            return _decorate_transform_method(item, kwargs)
        return _transform(item, **kwargs)

    if target is None:
        return decorate
    return decorate(target)


__all__ = ["transform"]
