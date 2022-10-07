import pathlib

from typer.testing import CliRunner

from gtfs_extractor import cli, __app_name__, __version__

runner = CliRunner()

script_path = pathlib.Path(__file__).parent.resolve()


def test_version() -> None:
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert f"{__app_name__} v{__version__}\n" in result.stdout


def test_extract(tmpdir: pathlib.Path) -> None:
    test_gtfs_folder: str = script_path.joinpath("../tests/files/ic_ice_gtfs_germany").__str__()

    result = runner.invoke(
        cli.app,
        [
            "--logging",
            "WARN",
            "extract-bbox",
            "--input-folder",
            test_gtfs_folder,
            "--output-folder",
            tmpdir.__str__(),
            "--bbox",
            "8.573179,49.352003,8.79405,49.459693",
        ],
    )
    assert result.exit_code == 0
