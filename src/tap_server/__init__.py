"""
TAP Server Prototype

A prototype implementation of a TAP (Table Access Protocol) server
that implements a small subset of the IVOA TAP specification.
It is used to provide experimental access to some LSDB catalog
data, and is an incremental work in progress.  Not all ADQL
queries can be translated into the LSDB equivalent.

This server uses the adql_to_lsdb module to convert ADQL queries
__version__ = "0.2.0"

from .tap_schema_db import TAPSchemaDatabase
from .import_tap_schema import TAPSchemaImporter

__all__ = ["TAPSchemaDatabase", "TAPSchemaImporter"]

"""

__version__ = "0.2.0"
