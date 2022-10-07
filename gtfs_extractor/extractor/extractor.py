import csv
import errno
import os
import shutil

import dask
import dask.dataframe as ddf
import pandas as pd
from pathlib import Path
from typing import List, Set, Union, Dict, Tuple

from gtfs_extractor import logger
from gtfs_extractor.exceptions.extractor_exceptions import GtfsFileNotFound
from gtfs_extractor.extractor.bbox import Bbox
from gtfs_extractor.extractor.gtfs import GTFS
from dask.diagnostics import ProgressBar

ProgressBar().register()


class Extractor(GTFS):
    def __init__(self, input_folder: Path, output_folder: Path) -> None:
        super().__init__(input_folder)
        if not output_folder.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), output_folder)
        self._output_folder: Path = output_folder

    @staticmethod
    @dask.delayed
    def __delayed_filter(rows: pd.DataFrame, ids: Set, column: Union[str, int]) -> pd.DataFrame:
        if isinstance(column, int):
            column = rows.columns[column]
        mask = rows[column].isin(ids)
        rows[column].where(mask, inplace=True)
        return rows[rows[column].notna()]

    @dask.delayed
    def __filter_stops_by_bbox(self, stops: pd.DataFrame, bbox: Bbox) -> List:
        mask: pd.Series = stops.apply(lambda row: bbox.contains(row["stop_lat"], row["stop_lon"]), axis=1)
        stops["stop_id"].where(mask, inplace=True)
        stops.dropna(inplace=True)
        return stops["stop_id"].tolist()

    def __filter_using_custom_column(
        self,
        file_path: Path,
        ids: Set,
        column: Union[str, int] = 0,
        usecols: List = None,
        dtype: Union[str, Dict] = "object",
        return_columns: List = None,
        write_out: bool = False,
        scheduler: str = "multiprocessing",
    ) -> Tuple:
        if not file_path.exists():
            raise GtfsFileNotFound(file_path=file_path.__str__())
        csv_chunks: ddf.DataFrame = ddf.read_csv(file_path, usecols=usecols, dtype=dtype, low_memory=False)
        results: Union[List, pd.DataFrame] = list(
            dask.compute(*[self.__delayed_filter(d, ids, column) for d in csv_chunks.to_delayed()], scheduler=scheduler)
        )
        results = pd.concat(results, axis=0)
        if write_out:
            output_path = self._output_folder.joinpath(file_path.name)
            results.to_csv(output_path, index=False, doublequote=True, quoting=csv.QUOTE_ALL)
        if isinstance(return_columns, List) and len(return_columns) > 0:
            return tuple(set(results[return_column].tolist()) for return_column in return_columns)
        return tuple()

    def _get_stops_in_bbox(self, bbox: Bbox) -> Set:
        csv_chunks: ddf.DataFrame = ddf.read_csv(
            self._gtfs_files.stops,
            usecols=["stop_id", "stop_lat", "stop_lon"],
            low_memory=False,
            dtype={"stop_id": "object"},
        )
        lists_of_trips = list(
            dask.compute(
                *[self.__filter_stops_by_bbox(d, bbox) for d in csv_chunks.to_delayed()], scheduler="multiprocessing"
            )
        )
        stops = set([item for sublist in lists_of_trips for item in sublist])
        return stops

    def _get_trips_of_stops(self, stops_to_keep: Set) -> Set:
        return self.__filter_using_custom_column(
            self._gtfs_files.stop_times,
            stops_to_keep,
            usecols=["stop_id", "trip_id"],
            column="stop_id",
            return_columns=["trip_id"],
            write_out=False,
        )[0]

    def _filter_trips(self, trips_to_keep: Set) -> Tuple:
        return self.__filter_using_custom_column(
            self._gtfs_files.trips,
            trips_to_keep,
            column="trip_id",
            dtype="object",
            return_columns=["route_id", "service_id"],
            write_out=True,
        )

    def _filter_routes(self, routes_to_keep: Set) -> Set:
        return self.__filter_using_custom_column(
            self._gtfs_files.routes,
            routes_to_keep,
            column="route_id",
            return_columns=["agency_id"],
            write_out=True,
        )[0]

    def _filter_stop_times_using_trips(self, trip_ids_to_keep: Set) -> Set:
        return self.__filter_using_custom_column(
            self._gtfs_files.stop_times, trip_ids_to_keep, column="trip_id", return_columns=["stop_id"], write_out=True
        )[0]

    def _filter_agencies(self, agency_ids_to_keep: Set) -> None:
        self.__filter_using_custom_column(
            self._gtfs_files.agency, agency_ids_to_keep, column="agency_id", write_out=True
        )

    def _filter_calendar_dates_using_services(self, service_ids_to_keep: Set) -> None:
        self.__filter_using_custom_column(
            self._gtfs_files.calendar_dates, service_ids_to_keep, column="service_id", write_out=True
        )

    def _filter_calendar_using_services(self, service_ids_to_keep: Set) -> None:
        self.__filter_using_custom_column(
            self._gtfs_files.calendar, service_ids_to_keep, column="service_id", write_out=True
        )

    def _filter_frequencies_using_trips(self, trip_ids_to_keep: Set) -> None:
        self.__filter_using_custom_column(
            self._gtfs_files.frequencies, trip_ids_to_keep, column="trip_id", write_out=True
        )

    def _filter_stops(self, stop_ids_to_keep: Set) -> None:
        self.__filter_using_custom_column(self._gtfs_files.stops, stop_ids_to_keep, column="stop_id", write_out=True)

    def _process_common_files(self, service_ids_to_keep: Set, trip_ids_to_keep: Set) -> None:
        self._filter_calendar_dates_using_services(service_ids_to_keep)
        self._filter_calendar_using_services(service_ids_to_keep)
        self._filter_frequencies_using_trips(trip_ids_to_keep)

        # Keep the stop_times used by the trips
        stop_ids_to_keep: Set = self._filter_stop_times_using_trips(trip_ids_to_keep)
        self._filter_stops(stop_ids_to_keep)
        # self._filter_transfers_using_stops(stop_ids_to_keep)

        logger.info(len(stop_ids_to_keep), " stops to keep")

        # Copy the feed info
        shutil.copyfile(self._gtfs_files.feed_info.name, self._output_folder.joinpath(self._gtfs_files.feed_info.name))

    def extract_by_agency(self, agencies: List[str]) -> None:
        ...

    def extract_by_bbox(self, bbox: Bbox) -> List[str]:
        logger.debug("Filter stops within bbox")
        stop_ids_in_bbox = self._get_stops_in_bbox(bbox)
        logger.info("Found {} stops in bbox".format(len(stop_ids_in_bbox)))

        logger.debug("Filter trips from selected stops")
        trip_ids: Set = self._get_trips_of_stops(stop_ids_in_bbox)
        logger.info("Found {} trips in bbox".format(len(trip_ids)))

        logger.debug("Filter routes from selected trips")
        route_ids_to_keep: Set
        service_ids_to_keep: Set
        route_ids_to_keep, service_ids_to_keep = self._filter_trips(trip_ids)
        logger.info("Found {} routes in bbox".format(len(route_ids_to_keep)))

        logger.debug("Filter agencies")
        agency_ids_to_keep: Set
        agency_ids_to_keep = self._filter_routes(route_ids_to_keep)
        logger.info("Keeping {} agencies in bbox".format(len(agency_ids_to_keep)))

        self._filter_agencies(agency_ids_to_keep)

        self._process_common_files(service_ids_to_keep=service_ids_to_keep, trip_ids_to_keep=trip_ids)

        return []
