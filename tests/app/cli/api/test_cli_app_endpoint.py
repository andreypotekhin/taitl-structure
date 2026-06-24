from structure.app.cli.api import CliApp


def test_cli_app_endpoint_groups_fresh_command_instances() -> None:
    assert CliApp.discover_project() is not CliApp.discover_project()
    assert CliApp.render_configured_pyspark_project() is not CliApp.render_configured_pyspark_project()
    assert CliApp.render_explain_report() is not CliApp.render_explain_report()
