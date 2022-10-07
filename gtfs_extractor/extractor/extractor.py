import csv
import errno
import os

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


class Extractor:
    def __init__(self, input_folder: Path, output_folder: Path):
        if not output_folder.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), output_folder)
        self._gtfs_object: GTFS = GTFS(input_folder=input_folder)
        self._input_folder: Path = input_folder
        self._output_folder: Path = output_folder

    @staticmethod
    @dask.delayed
    def __delayed_filter(rows: pd.DataFrame, ids: Set, column: Union[str, int]):
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
    ) -> Union[Tuple, None]:
        if not file_path.exists():
            raise GtfsFileNotFound(file_path=file_path.__str__())
        csv_chunks: ddf.DataFrame = ddf.read_csv(file_path, usecols=usecols, dtype=dtype, low_memory=False)
        results = list(
            dask.compute(*[self.__delayed_filter(d, ids, column) for d in csv_chunks.to_delayed()], scheduler=scheduler)
        )
        results = pd.concat(results, axis=0)
        if write_out:
            output_path = self._output_folder.joinpath(file_path.name)
            results.to_csv(output_path, index=False, doublequote=True, quoting=csv.QUOTE_ALL)
        if isinstance(return_columns, List) and len(return_columns) > 0:
            return tuple(set(results[return_column].tolist()) for return_column in return_columns)

    def _get_stops_in_bbox(self, bbox: Bbox) -> Set:
        csv_chunks: ddf.DataFrame = ddf.read_csv(
            self._gtfs_object.files.stops,
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

    def _get_trips_of_stops(self, stops_to_keep) -> Set:
        return self.__filter_using_custom_column(
            self._gtfs_object.files.stop_times,
            stops_to_keep,
            usecols=["stop_id", "trip_id"],
            column="stop_id",
            return_columns=["trip_id"],
            write_out=False,
        )[0]

    def _extract_common_files(self, service_ids_to_keep: List, trip_ids: List):
        ...

    def extract_by_agency(self, agencies: List[str]):
        ...

    def extract_by_bbox(self, bbox: Bbox) -> List[str]:
        logger.info("Get stops within bbox")
        stop_ids_in_bbox = self._get_stops_in_bbox(bbox)
        logger.info("Found {} stops in bbox".format(len(stop_ids_in_bbox)))

        logger.info("Get trips from selected stops")
        trip_ids: Set = self._get_trips_of_stops(stop_ids_in_bbox)
        logger.info("Found {} trips in bbox".format(len(trip_ids)))
