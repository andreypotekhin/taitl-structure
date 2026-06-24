from structure.app.cli.api.CliActionsEndpoint import CliActionsEndpoint
from structure.app.cli.api.cli import cli

cli_actions = CliActionsEndpoint()

__all__ = [
    "CliActionsEndpoint",
    "cli",
    "cli_actions",
]
