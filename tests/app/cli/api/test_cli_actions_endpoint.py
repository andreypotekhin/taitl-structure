from structure.app.cli.api import CliActionsEndpoint, cli_actions


def test_cli_actions_endpoint_groups_fresh_command_instances() -> None:
    assert isinstance(cli_actions, CliActionsEndpoint)
    assert cli_actions.discover_project() is not cli_actions.discover_project()
    assert cli_actions.render_configured_pyspark_project() is not cli_actions.render_configured_pyspark_project()
    assert cli_actions.render_explain_report() is not cli_actions.render_explain_report()
