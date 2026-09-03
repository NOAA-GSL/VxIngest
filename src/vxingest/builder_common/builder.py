"""
Builder - Parent class for all Builders
"""

import logging
import time
from pathlib import Path

from couchbase.exceptions import DocumentNotFoundException

logger = logging.getLogger(__name__)

# Shared "is VxImporter running" lock document. VxImporter (a separate process, possibly
# in another container) is expected to upsert this document with status="running" before it
# starts writing model/obs data and set status="idle" (or remove the document) when it is done.
# Builders that read data VxImporter writes (e.g. CTC, PARTIAL_SUMS) wait on this lock so they
# never read a partially-imported dataset.
IMPORT_LOCK_DOC_ID = "MD:import_lock:COMMON:V01"
IMPORT_LOCK_POLL_SECONDS = 10
IMPORT_LOCK_MAX_WAIT_SECONDS = 1800  # give up and proceed anyway after this long
IMPORT_LOCK_STALE_SECONDS = (
    1800  # ignore a lock that hasn't been refreshed this recently
)


class Builder:
    """
    Parent class for all Builders
    """

    def __init__(self, load_spec, ingest_document):
        self.ingest_document = ingest_document
        # CTC builders cannot init the ingest_document or the template, they get set in the build_document
        self.template = None if ingest_document is None else ingest_document["template"]
        self.load_spec = load_spec
        self.an_id = None
        self.document_map = {}
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False

    def wait_for_import_lock(self):
        """
        Block (with periodic polling) while VxImporter's import lock document indicates
        an import is in progress, so this builder doesn't read a partially-imported dataset.
        Safe no-op if the lock document is absent, stale, or the common_collection isn't
        available. Never raises - a failure to check the lock just means we proceed.
        """
        common_collection = self.load_spec.get("common_collection")
        if common_collection is None:
            return
        waited_seconds = 0
        while waited_seconds < IMPORT_LOCK_MAX_WAIT_SECONDS:
            try:
                lock_doc = common_collection.get(IMPORT_LOCK_DOC_ID).content_as[dict]
            except DocumentNotFoundException:
                return
            except Exception as _e:
                logger.warning(
                    "%s.wait_for_import_lock: could not read import lock, proceeding: %s",
                    self.__class__.__name__,
                    str(_e),
                )
                return
            if lock_doc.get("status") != "running":
                return
            lock_age_seconds = time.time() - lock_doc.get("updated", 0)
            if lock_age_seconds > IMPORT_LOCK_STALE_SECONDS:
                logger.warning(
                    "%s.wait_for_import_lock: import lock is stale (%.0fs old), proceeding",
                    self.__class__.__name__,
                    lock_age_seconds,
                )
                return
            logger.info(
                "%s.wait_for_import_lock: VxImporter job %s is running, waiting %ss",
                self.__class__.__name__,
                lock_doc.get("job_id", "unknown"),
                IMPORT_LOCK_POLL_SECONDS,
            )
            time.sleep(IMPORT_LOCK_POLL_SECONDS)
            waited_seconds += IMPORT_LOCK_POLL_SECONDS
        logger.warning(
            "%s.wait_for_import_lock: gave up waiting for import lock after %ss, proceeding",
            self.__class__.__name__,
            IMPORT_LOCK_MAX_WAIT_SECONDS,
        )

    def initialize_document_map(self):
        pass

    def get_document_map(self):
        pass

    def handle_data(self, **kwargs):
        pass

    def derive_id(self, **kwargs):
        pass

    def load_data(self, doc, element):
        pass

    def handle_document(self):
        pass

    def build_document(self, queue_element):
        pass

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        pass

    def create_data_file_id(self, subset, file_type, origin_type, file_name):
        """
        This method creates a metar grib_to_cb datafile id from the parameters
        """
        try:
            base_name = Path(file_name).name
            an_id = f"DF:{subset}:{file_type}:{origin_type}:{base_name}"
            return an_id
        except Exception as _e:
            logger.exception("%s create_data_file_id", self.__class__.__name__)
            return None
