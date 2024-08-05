"""
Program Name: Class IngestManager
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The IngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of file names. It uses the collection and the cluster
objects that are passed from the run_ingest_threads (VXIngest class).
It finishes when the file_name_queue is empty.

It gets file names serially from a queue that is shared by a
thread pool of data_type_manager's and processes them one at a a_time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the file.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life. For IngestManager it is likely that
each file will require only one builder type to be instantiated.
When IngestManager finishes a document specification  it  writes the document to the output directory,
if an output directory was specified.

        Attributes:
            name -a threadName for logging and debugging purposes.
            credentials, first and last epoch,
            file_name_queue a shared queue of filenames.
            output_dir where the output documents will be written
            collection couchbase collection object for data service access
            cluster couchbase cluster object for query service access
            number_stations=sys.maxsize (you can limit how many stations will be processed - for debugging)

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

import contextlib
import json
import logging
import pathlib
import re
import sys
import time
from pathlib import Path

import mysql.connector
from tabulate import tabulate
from vxingest.builder_common.ingest_manager import CommonVxIngestManager
from vxingest.prepbufr_to_cb import prepbufr_builder as my_builder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class VxIngestManager(CommonVxIngestManager):
    """
    IngestManager is a Process Thread that manages an object pool of
    builders to ingest data from GSD grib2 files or netcdf files into documents that can be
    inserted into couchbase or written to json files in the specified output directory.

    This class will process data by retrieving an ingest_document specified
    by an ingest_document_id and instantiating a builder class of the type specified in the
    ingest_document.
    The ingest document specifies the builder class, and a template that defines
    how to place the variable values into a couchbase document and how to
    construct the couchbase data document id.

    It will then read file_names, one by one,
    from the file_name_queue.  The builders use the template to create documents for
    each filename and put them into the document map.

    When all of the result set entries for a file are processed, the IngestManager upsert
    the document(s) to couchbase, or writes to an output directory and retrieves a new filename from
    the queue and starts over.

    Each builder is kept in an object pool so that they do not need to be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """

    def __init__(
        self,
        name,
        load_spec,
        element_queue,
        output_dir,
        logging_queue,
        logging_configurer,
        write_data_for_station_list=None,  # used for debugging
        write_data_for_levels=None,  # used for debugging
    ):
        """constructor for VxIngestManager
        Args:
            name (string): the thread name for this IngestManager
            load_spec (Object): contains Couchbase credentials
            element_queue (Queue): reference to the element Queue
            output_dir (string): output directory path
        """
        # The Constructor for the RunCB class.
        self.thread_name = name
        self.load_spec = load_spec
        self.cb_credentials = self.load_spec["cb_connection"]
        self.ingest_document_ids = self.load_spec["ingest_document_ids"]
        # use the first one, there aren't multiples anyway
        self.ingest_document = self.load_spec["ingest_documents"][
            self.ingest_document_ids[0]
        ]
        self.ingest_type_builder_name = None
        self.queue = element_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir
        self.write_data_for_debug_station_list = write_data_for_station_list
        self.write_data_for_debug_levels = write_data_for_levels

        super().__init__(
            self.thread_name,
            self.load_spec,
            self.queue,
            self.output_dir,
            logging_queue,
            logging_configurer,
        )

        if self.write_data_for_debug_station_list:
            self.debug_station_file = None
            self.debug_station_file_name = (
                f"/tmp/debug_data_for_stations_{str(int(time.time()))}.txt"
            )
            with contextlib.suppress(OSError):
                Path(self.debug_station_file_name).unlink()
            print(
                f"debug data for stations {str(self.write_data_for_debug_station_list)}  is in {self.debug_station_file_name}"
            )

    def get_my_result_final(self, my_sql, i0, i1):
        """get the result from the object, returning None if it isn't there"""
        try:
            return str(my_sql[i0][i1])
        except Exception as _ignore:
            return None

    def write_data_for_debug(self, builder, document_map):
        """
        write the raw data and interpolated for a specific set of stations for debugging purposes
        """
        try:
            if self.debug_station_file is None:
                with pathlib.Path(self.debug_station_file_name).open(
                    "a"
                ) as self.debug_station_file:
                    self.debug_station_file.write("------\n")

                    for station in self.write_data_for_debug_station_list:
                        try:
                            self.debug_station_file.write(
                                f""" station: {station}\n\n"""
                            )

                            pb_raw_obs_data_120 = builder.raw_obs_data[station][120][
                                "obs_data"
                            ]
                            pb_raw_obs_data_220 = builder.raw_obs_data[station][220][
                                "obs_data"
                            ]
                            pb_interpolated_120 = builder.interpolated_data[station][
                                120
                            ]["data"]
                            pb_interpolated_220 = builder.interpolated_data[station][
                                220
                            ]["data"]

                            for level in self.write_data_for_debug_levels:
                                # MASS report type 120 raw_obs_data
                                self.debug_station_file.write(
                                    f"MASS report type 120 raw_obs_data for station:{station} and level:{level}\n"
                                )
                                if level in pb_raw_obs_data_120["pressure"]:
                                    raw_level_index_120 = pb_raw_obs_data_120[
                                        "pressure"
                                    ].index(level)
                                    for variable in sorted(
                                        list(pb_raw_obs_data_120.keys())
                                    ):
                                        self.debug_station_file.write(
                                            f"level:{level} {variable}: {pb_raw_obs_data_120[variable][raw_level_index_120] if pb_raw_obs_data_120[variable] is not None else None}\n"
                                        )

                                # WIND report type 220 raw_obs_data
                                self.debug_station_file.write(
                                    f"\nWIND report type 220 raw_obs_data for station:{station} and level:{level}\n"
                                )
                                if level in pb_raw_obs_data_220["pressure"]:
                                    raw_level_index_220 = pb_raw_obs_data_220[
                                        "pressure"
                                    ].index(level)
                                    for variable in sorted(
                                        list(pb_raw_obs_data_220.keys())
                                    ):
                                        self.debug_station_file.write(
                                            f"level:{level} {variable}: {pb_raw_obs_data_220[variable][raw_level_index_220] if pb_raw_obs_data_220[variable] is not None else None}\n"
                                        )

                                # interpolated data
                                # MASS report type 120 interpolated data
                                self.debug_station_file.write(
                                    f"\nMASS report type 120 interpolated data station:{station} level:{level}\n"
                                )
                                if level in pb_interpolated_120["pressure"]:
                                    for variable in sorted(
                                        list(pb_interpolated_120.keys())
                                    ):
                                        self.debug_station_file.write(
                                            f"level:{level} {variable}: {pb_interpolated_120[variable].get(level, None) if pb_interpolated_120[variable] is not None else None}\n"
                                        )

                                # interpolated data
                                # WIND report type 220 interpolated data
                                self.debug_station_file.write(
                                    f"\nWIND report type 220 interpolated data station:{station} level:{level}\n"
                                )
                                if level in pb_interpolated_220["pressure"]:
                                    for variable in sorted(
                                        list(pb_interpolated_220.keys())
                                    ):
                                        self.debug_station_file.write(
                                            f"level:{level} {variable}: {builder.interpolated_data[station][220]["data"][variable].get(level, None)}\n"
                                        )

                                # write station data
                                self.debug_station_file.write(
                                    f"\ndocument_map data for station:{station} level:{level}\n"
                                )
                                r = re.compile(f"DD:V01:RAOB:obs:prepbufr:{level}:.*")
                                key = list(filter(r.match, document_map.keys()))[0]
                                pb_final = document_map[key]["data"][station]
                                self.debug_station_file.write("\n")
                                self.debug_station_file.write(
                                    json.dumps(
                                        pb_final,
                                        indent=2,
                                    )
                                )
                                date = document_map[key]["fcstValidISO"].split("T")[0]
                                stmnt_mysql = f'select wmoid,press,z,t,dp,rh,wd,ws from ruc_ua_pb.RAOB where date = "{date}"  and  press = {level} and wmoid = "{station}";'
                                _mysql_db = mysql.connector.connect(
                                    host=self.load_spec["_mysql_host"],
                                    user=self.load_spec["_mysql_user"],
                                    password=self.load_spec["_mysql_pwd"],
                                )
                                my_cursor = _mysql_db.cursor()
                                my_cursor.execute(stmnt_mysql)
                                my_result_final = my_cursor.fetchall()

                                table = [
                                    [
                                        "source",
                                        "press",
                                        "temperature",
                                        "dewpoint",
                                        "relative_humidity",
                                        "specific_humidity",
                                        "height",
                                        "wind speed",
                                        "wind direction,",
                                        "U-Wind",
                                        "V-Wind",
                                    ],
                                    [
                                        "pb_raw_obs",
                                        pb_raw_obs_data_120["pressure"][
                                            raw_level_index_120
                                        ]
                                        if pb_raw_obs_data_120["pressure"] is not None
                                        else None,
                                        pb_raw_obs_data_120["temperature"][
                                            raw_level_index_120
                                        ]
                                        if pb_raw_obs_data_120["temperature"]
                                        is not None
                                        else None,
                                        pb_raw_obs_data_120["dewpoint"][
                                            raw_level_index_120
                                        ],
                                        pb_raw_obs_data_120["relative_humidity"][
                                            raw_level_index_120
                                        ]
                                        if pb_raw_obs_data_120["relative_humidity"]
                                        is not None
                                        else None,
                                        pb_raw_obs_data_120["specific_humidity"][
                                            raw_level_index_120
                                        ],
                                        pb_raw_obs_data_120["height"][
                                            raw_level_index_120
                                        ]
                                        if pb_raw_obs_data_120["height"] is not None
                                        else None,
                                        pb_raw_obs_data_220["wind_speed"][
                                            raw_level_index_220
                                        ]
                                        if pb_raw_obs_data_220["wind_speed"] is not None
                                        else None,
                                        pb_raw_obs_data_220["wind_direction"][
                                            raw_level_index_220
                                        ]
                                        if pb_raw_obs_data_220["wind_direction"]
                                        is not None
                                        else None,
                                        pb_raw_obs_data_220["U-Wind"][
                                            raw_level_index_220
                                        ]
                                        if pb_raw_obs_data_220["U-Wind"] is not None
                                        else None,
                                        pb_raw_obs_data_220["V-Wind"][
                                            raw_level_index_220
                                        ]
                                        if pb_raw_obs_data_220["V-Wind"] is not None
                                        else None,
                                    ],
                                    [
                                        "pb_interpolated",
                                        pb_interpolated_120["pressure"].get(level, None)
                                        if pb_interpolated_120["pressure"] is not None
                                        else None,
                                        pb_interpolated_120["temperature"].get(
                                            level, None
                                        )
                                        if pb_interpolated_120["temperature"]
                                        is not None
                                        else None,
                                        pb_interpolated_120["dewpoint"].get(level, None)
                                        if pb_interpolated_120["dewpoint"] is not None
                                        else None,
                                        pb_interpolated_120["relative_humidity"].get(
                                            level, None
                                        )
                                        if pb_interpolated_120["relative_humidity"]
                                        is not None
                                        else None,
                                        pb_interpolated_120["specific_humidity"].get(
                                            level, None
                                        ),
                                        pb_interpolated_120["height"].get(level, None)
                                        if pb_interpolated_120["height"] is not None
                                        else None,
                                        pb_interpolated_220["wind_speed"].get(
                                            level, None
                                        )
                                        if pb_interpolated_220["wind_speed"] is not None
                                        else None,
                                        pb_interpolated_220["wind_direction"].get(
                                            level, None
                                        )
                                        if pb_interpolated_220["wind_direction"]
                                        is not None
                                        else None,
                                        pb_interpolated_220["U-Wind"].get(level, None)
                                        if pb_interpolated_220["U-Wind"] is not None
                                        else None,
                                        pb_interpolated_220["V-Wind"].get(level, None)
                                        if pb_interpolated_220["V-Wind"] is not None
                                        else None,
                                    ],
                                    [
                                        "pb_final",
                                        pb_final["pressure"],
                                        pb_final["temperature"],
                                        pb_final["dewpoint"],
                                        pb_final["relative_humidity"],
                                        pb_final["specific_humidity"],
                                        pb_final["height"],
                                        pb_final["wind_speed"],
                                        pb_final["wind_direction"],
                                        pb_final["U-Wind"],
                                        pb_final["V-Wind"],
                                    ],
                                    [
                                        # wmoid,press,z,t,dp,rh,wd,ws
                                        "mysql_final",
                                        self.get_my_result_final(my_result_final, 0, 1),
                                        self.get_my_result_final(my_result_final, 0, 3),
                                        self.get_my_result_final(my_result_final, 0, 4),
                                        self.get_my_result_final(my_result_final, 0, 5),
                                        "--",
                                        self.get_my_result_final(my_result_final, 0, 2),
                                        self.get_my_result_final(my_result_final, 0, 7),
                                        self.get_my_result_final(my_result_final, 0, 6),
                                        "--",
                                        "--",
                                    ],
                                ]

                                self.debug_station_file.write("\nCOMPARE THE DATA\n")
                                self.debug_station_file.write(
                                    tabulate(
                                        table, headers="firstrow", tablefmt="fancy_grid"
                                    )
                                )
                                self.debug_station_file.write("\n------\n")
                                self.debug_station_file.write("Sql Statements\n")
                                self.debug_station_file.write(f"{stmnt_mysql}\n")
                                self.debug_station_file.write("------\n")
                        except Exception as _e:
                            logger.exception(
                                "%s: *** Error in IngestManager station %s not found ***",
                                self.thread_name,
                                station,
                            )
                            continue
                    self.debug_station_file.flush()
        except Exception as _e:
            logger.exception(
                "%s: *** Error in IngestManager run write_data_for_debug ***",
                self.thread_name,
            )

    def set_builder_name(self, queue_element):
        """
        get the builder name from the ingest document
        """
        if self.ingest_type_builder_name is None:
            try:
                self.ingest_type_builder_name = self.ingest_document["builder_type"]
            except Exception as _e:
                logger.exception(
                    "%s: process_element: Exception getting ingest document for %s ",
                    self.thread_name,
                    queue_element,
                )
                raise _e

    def process_queue_element(self, queue_element):
        """Process this queue_element
        Args:
            queue_element (string): queue_element
        Raises:
            _e: exception
        """
        # get or instantiate the builder

        start_process_time = int(time.time())
        document_map = {}

        try:
            logger.info("process_element - : start time: %s", str(start_process_time))
            try:
                self.set_builder_name(queue_element)
            except Exception as _e:
                logger.exception(
                    "%s: *** Error in IngestManager run getting builder name ***",
                    self.thread_name,
                )
                sys.exit("*** Error getting builder name: ")

            if self.ingest_type_builder_name in self.builder_map:
                builder = self.builder_map[self.ingest_type_builder_name]
            else:
                builder_class = getattr(my_builder, self.ingest_type_builder_name)
                builder = builder_class(self.load_spec, self.ingest_document)
                builder.set_write_data_for_debug_station_list(
                    self.write_data_for_debug_station_list
                )
                self.builder_map[self.ingest_type_builder_name] = builder
            document_map = builder.build_document(queue_element)
            if self.write_data_for_debug_station_list:
                self.write_data_for_debug(builder, document_map)

            if self.output_dir:
                self.write_document_to_files(queue_element, document_map)
            else:
                self.write_document_to_cb(queue_element, document_map)
        except Exception as _e:
            logger.exception(
                "%s: Exception in builder: %s",
                self.thread_name,
                str(self.ingest_type_builder_name),
            )
            raise _e
        finally:
            # reset the document map and record stop time
            stop_process_time = int(time.time())
            document_map = {}
            logger.info(
                "IngestManager.process_element: elapsed time: %s",
                str(stop_process_time - start_process_time),
            )
