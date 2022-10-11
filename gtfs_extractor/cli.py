"""This module provides the RP To-Do CLI."""
import os
from pathlib import Path

from typing import Optional, Union, List
import typer
from . import __app_name__, __version__, logger
from .extractor.bbox import Bbox
from .extractor.extractor import Extractor
from .extractor.gtfs import GTFS
from .logging import initialize_logging
from dask.diagnostics import ProgressBar

dask_pbar = ProgressBar()

app = typer.Typer()
cpu_count: Union[None, int] = os.cpu_count()
if cpu_count is None or cpu_count == 1:
    cpu_count = 1
else:
    cpu_count -= 1


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


# @app.command()
# def extract_agency(input_folder, output_folder: str, agencies: str) -> None:
#     keep_agencies = [x.strip() for x in agencies.split(",")]
#     logger.info(keep_agencies)


@app.command()
def extract_bbox(
    input_object: str = typer.Option(..., help="Directory or zip File from which the GFTS files are read"),
    output_folder: str = typer.Option(..., help="Directory to which the GFTS files are written"),
    bbox: str = typer.Option(
        ..., help='The bbox for selecting the GTFS data to keep. Example: "8.573179,49.352003,8.79405,49.459693"'
    ),
) -> None:
    coordinates: List[float] = [float(x.strip()) for x in bbox.split(",")]
    keep_bbox: Bbox = Bbox(*coordinates)
    extractor: Extractor = Extractor(input_object=Path(input_object), output_folder=Path(output_folder))
    files: List = extractor.extract_by_bbox(bbox=keep_bbox)
    extractor.close()
    logger.info(f"Successfully processed {input_object} to the following files:")
    file: Path
    for file in files:
        logger.info(file.__str__())


@app.command()
def metadata(
    input_object: str = typer.Option(..., help="Directory or zip File from which the GFTS files are read"),
) -> None:
    gtfs: GTFS = GTFS(input_object=Path(input_object))
    dates = gtfs.service_date_range()
    logger.info(f"Service date window from '{dates[0]}' to '{dates[1]}'")


@app.callback()
def main(
    logging: Optional[str] = "INFO",
    cores: Optional[int] = cpu_count,
    progress: Optional[bool] = typer.Option(True, help="Deactivate the progress bars."),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    if logging is None:
        logging = "INFO"
    initialize_logging(logging)
    if progress:
        dask_pbar.register()
    return
