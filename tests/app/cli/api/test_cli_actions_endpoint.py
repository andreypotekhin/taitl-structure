from structure.app.cli.api import CliActions


def test_cli_actions_endpoint_groups_fresh_command_instances() -> None:
    assert CliActions.discover_project() is not CliActions.discover_project()
    assert CliActions.render_configured_pyspark_project() is not CliActions.render_configured_pyspark_project()
    assert CliActions.render_explain_report() is not CliActions.render_explain_report()
