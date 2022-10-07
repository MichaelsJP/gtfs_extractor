import errno
import os
import tempfile
import zipfile

from pathlib import Path
from typing import Any, Union

from gtfs_extractor import logger
from gtfs_extractor.exceptions.extractor_exceptions import GtfsIncompleteException


class GtfsFiles:
    # Required
    agency: Union[Path, None] = None
    calendar_dates: Union[Path, None] = None
    calendar: Union[Path, None] = None
    feed_info: Union[Path, None] = None
    routes: Union[Path, None] = None
    stop_times: Union[Path, None] = None
    stops: Union[Path, None] = None
    trips: Union[Path, None] = None

    # Optional - not complete
    frequencies: Union[Path, None] = None
    shapes: Union[Path, None] = None
    transfers: Union[Path, None] = None

    def set_files(self, file_path: Path):
        file_name: str = file_path.name
        if "agency" in file_name:
            self.agency = file_path
        elif "calendar_dates" in file_name:
            self.calendar_dates = file_path
        elif "calendar" in file_name:
            self.calendar = file_path
        elif "feed_info" in file_name:
            self.feed_info = file_path
        elif "routes" in file_name:
            self.routes = file_path
        elif "stop_times" in file_name:
            self.stop_times = file_path
        elif "stops" in file_name:
            self.stops = file_path
        elif "trips" in file_name:
            self.trips = file_path
        elif "frequencies" in file_name:
            self.frequencies = file_path
        elif "shapes" in file_name:
            self.shapes = file_path
        elif "transfers" in file_name:
            self.transfers = file_path

    def required_is_complete(self):
        if all(
            [
                self.agency,
                self.calendar_dates,
                self.calendar,
                self.feed_info,
                self.routes,
                self.stop_times,
                self.stops,
                self.trips,
            ]
        ):
            return True
        return False


class GTFS:
    def __init__(self, input_folder: Path):
        self.temporary_folder_context: Any[tempfile.TemporaryDirectory, None] = None
        self.files: GtfsFiles = GtfsFiles()
        if input_folder.is_file():
            input_folder = self._extract_gtfs_file(input_folder)
        if not input_folder.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), input_folder)
        for test in input_folder.glob("*.txt"):
            self.files.set_files(test)
            print(test.name)
        if not self.files.required_is_complete():
            raise GtfsIncompleteException()

    def close(self):
        if isinstance(self.temporary_folder_context, tempfile.TemporaryDirectory):
            self.temporary_folder_context.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _extract_gtfs_file(self, input_file: Path) -> Path:
        self.temporary_folder_context = tempfile.TemporaryDirectory()
        extract_path: Path = Path(self.temporary_folder_context.name)
        if not input_file.suffix == ".zip":
            # TODO raise wrong file
            logger.error("Input path is a file but not a .zip file. Exiting.")
            raise Exception
        logger.info("Input is a .zip file. It will be extracted to a temporary location.")
        with zipfile.ZipFile(input_file, "r") as zip_ref:
            zip_ref.extractall(extract_path)
        return extract_path

    def _check_completeness(self):
        ...
