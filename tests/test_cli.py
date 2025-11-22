import shlex

import pytest
from click.testing import CliRunner

from yato.cli import run

runner = CliRunner()


def yato_cli(command_string):
    """Helper function to run the CLI with a command string."""
    command_list = shlex.split(command_string)
    result = runner.invoke(run, command_list)
    return result.output.rstrip()


@pytest.mark.parametrize(
    "command_string, expected_message_part",
    [
        ("", "Usage:"),
        ("--help", "Usage:"),
        ("tests/files/case0", "Running 3 objects..."),
        ("--db mock.duckdb --schema transform tests/files", "Running 6 objects..."),
    ],
)
def test_yato_cli_run(command_string, expected_message_part):
    """Test the yato CLI run command."""
    result = yato_cli(command_string)
    assert result is not None
    assert expected_message_part in result, f"Expected '{expected_message_part}' in the output, but got: {result}"


def test_yato_cli_run_ui_enabled(monkeypatch):
    """Test the yato CLI run command with UI option."""
    # Mock the console to avoid actual UI interaction
    monkeypatch.setattr("duckdb.__version__", "1.2.0")  # Ensure the version is compatible
    result = yato_cli("--ui tests/files/case0")
    assert "DuckDB UI requires version >= 1.2.1." in result
