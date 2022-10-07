import pathlib
from typing import List

from py._path.local import LocalPath
from typer.testing import CliRunner

from gtfs_extractor import cli, __app_name__, __version__

runner = CliRunner()

script_path = pathlib.Path(__file__).parent.resolve()


def test_version() -> None:
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert f"{__app_name__} v{__version__}\n" in result.stdout


def test_extract(tmpdir: LocalPath) -> None:
    test_gtfs_folder: str = script_path.joinpath("../tests/files/ic_ice_gtfs_germany").__str__()

    result = runner.invoke(
        cli.app,
        [
            "--logging",
            "INFO",
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

    file: pathlib.PosixPath
    output_files: List = [file for file in pathlib.Path(tmpdir.__str__()).glob("*.txt")]
    assert len(output_files) == 8

    actual_files: List = [file.name for file in output_files]
    expected_files: List = [
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
        "calendar.txt",
        "routes.txt",
        "feed_info.txt",
        "calendar_dates.txt",
        "agency.txt",
    ]
    assert len(actual_files) == len(expected_files)
    assert all([file in expected_files for file in actual_files])

    for file in output_files:
        with open(file, "r") as fp:
            x = len(fp.readlines())
            if file.name == "stop_times.txt":
                assert x == 2234
            elif file.name == "stops.txt":
                assert x == 372
            elif file.name == "trips.txt":
                assert x == 147
            elif file.name == "calendar.txt":
                assert x == 21
            elif file.name == "routes.txt":
                assert x == 19
            elif file.name == "feed_info.txt":
                assert x == 2
            elif file.name == "calendar_dates.txt":
                assert x == 7
            elif file.name == "agency.txt":
                assert x == 2
