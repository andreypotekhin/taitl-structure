from __future__ import annotations

from typing import cast

import click

from structure.app.configuration.api import ConfigError, Configuration, StructureConfig


class ResolveCliConfig:

    def __call__(self, overrides: dict[str, object]) -> StructureConfig:
        values = {key: value for key, value in overrides.items() if value not in (None, (), False)}
        if "source_roots" in values:
            values["source_roots"] = list(cast(tuple[str, ...], values["source_roots"]))
        if "compat_targets" in values:
            values["compat_targets"] = self._compat_targets(str(values["compat_targets"]))
        try:
            return Configuration.resolve()(overrides=values)
        except ConfigError as error:
            raise click.ClickException(error.diagnostic.render()) from error

    def _compat_targets(self, value: str) -> list[str]:
        return [item.strip() for item in value.split(",") if item.strip()]
