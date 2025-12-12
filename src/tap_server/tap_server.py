"""
TAP (Table Access Protocol) Server Prototype

This is a prototype implementation of a TAP server following the IVOA TAP specification:
https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html

The server accepts ADQL queries and returns results in VOTable format.
This prototype returns sample data instead of executing actual queries against a database.
"""

import argparse
import datetime
import logging
import os
import re
import xml.etree.ElementTree as ET  # noqa: N817
from xml.dom import minidom

import lsdb
from adql_to_lsdb import parse_adql_entities
from flask import Flask, Response, request

# Import TAP schema database module
from .tap_schema_db import TAPSchemaDatabase

app = Flask(__name__)


# Log what the client is actually sending
@app.before_request
def log_request_info():
    """Help with debugging exactly what the client sent."""
    app.logger.debug("Request URL: %s", request.url)
    app.logger.debug("Request Method: %s", request.method)
    app.logger.debug("Request Headers: %s", request.headers)


# Initialize TAP schema database
# The database will be created if it doesn't exist when the server starts
TAP_SCHEMA_DB_PATH = os.path.join(os.path.dirname(__file__), "tap_schema.db")
tap_schema_db = TAPSchemaDatabase(TAP_SCHEMA_DB_PATH, qualified="tap_schema")


def is_tap_schema_query(query_str: str):
    """Check if the query is for a TAP_SCHEMA table."""
    if not query_str:
        return False
    return "tap_schema." in query_str.lower()


def query_tap_schema(query_str: str):
    """
    Query TAP_SCHEMA metadata tables using SQLite.

    Returns:
        Tuple of (data, columns) where data is list of dicts and columns is list of column names
    """
    try:
        data, result_columns = tap_schema_db.query_with_columns(query_str, None)
        return data, result_columns
    except Exception as e:
        # If query fails, return empty result
        app.logger.error("Error querying TAP_SCHEMA: %s. Query: %s", str(e), query_str, exc_info=True)
        return [], []


def get_column_metadata(table_name: str):
    """
    Get column metadata from tap_schema.db for a given table.

    Args:
        table_name: The table name to fetch metadata for (e.g., 'ztf_dr14' or 'public.ztf_dr14')

    Returns:
        Dictionary mapping column names to metadata dicts with keys: datatype, unit, ucd, description
    """
    if not table_name:
        return {}

    # Try the table name as-is first
    query = "SELECT column_name, datatype, unit, ucd, description FROM columns WHERE table_name = ?"
    try:
        tap_schema_db.connect()
        results = tap_schema_db.query(query, (table_name,))

        # If no results and table_name doesn't have schema prefix, try with 'public.' prefix
        if not results and "." not in table_name:
            results = tap_schema_db.query(query, (f"public.{table_name}",))

        # Build metadata dictionary
        metadata = {}
        for row in results:
            col_name = row["column_name"]
            metadata[col_name] = {
                "datatype": row.get("datatype", "char"),
                "unit": row.get("unit", ""),
                "ucd": row.get("ucd", ""),
                "description": row.get("description", ""),
            }

        return metadata
    except Exception as e:
        app.logger.error("Error fetching column metadata for table %s: %s", table_name, str(e), exc_info=True)
        return {}


def format_xml_with_indentation(element):
    """
    Format an XML element with proper indentation.

    Args:
        element: An ElementTree Element to format

    Returns:
        String containing formatted XML with proper indentation
    """
    # Convert to string
    xml_str = ET.tostring(element, encoding="unicode", method="xml")

    # Parse and format with minidom for indentation
    dom = minidom.parseString(xml_str)
    pretty_xml = dom.toprettyxml(indent="  ", encoding=None)

    # Remove the extra XML declaration that minidom adds (we'll add our own)
    lines = pretty_xml.split("\n")
    if lines[0].startswith("<?xml"):
        lines = lines[1:]

    # Remove empty lines at the end
    while lines and not lines[-1].strip():
        lines.pop()

    # Add XML declaration and return
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + "\n".join(lines)


def create_votable_response(data, columns, query_info, column_metadata=None):
    """
    Create a VOTable XML response.

    Args:
        data: List of dictionaries containing row data
        columns: List of column names
        query_info: Dictionary with query metadata
        column_metadata: Optional dictionary mapping column names to metadata dicts
                        (with keys: datatype, unit, ucd, description)

    Returns:
        String containing VOTable XML
    """
    # Create VOTable root element
    votable = ET.Element(
        "VOTABLE",
        {
            "version": "1.4",
            "xmlns": "http://www.ivoa.net/xml/VOTable/v1.4",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        },
    )

    # Add RESOURCE element
    resource = ET.SubElement(votable, "RESOURCE", {"type": "results"})

    # Add INFO elements for query metadata
    ET.SubElement(resource, "INFO", {"name": "QUERY_STATUS", "value": "OK"})

    ET.SubElement(resource, "INFO", {"name": "QUERY", "value": query_info.get("query", "")})

    ET.SubElement(
        resource, "INFO", {"name": "TIMESTAMP", "value": datetime.datetime.now(datetime.UTC).isoformat()}
    )

    # Add TABLE element
    table = ET.SubElement(resource, "TABLE", {"name": query_info.get("table", "results")})

    # Add FIELD elements for each column
    for col in columns:
        field_attrs = {
            "name": col,
            "datatype": "double",  # Default datatype
            "unit": "",
        }

        # Use column metadata from tap_schema.db if available
        if column_metadata and col in column_metadata:
            meta = column_metadata[col]
            if meta.get("datatype"):
                field_attrs["datatype"] = meta["datatype"]
            if meta.get("unit"):
                field_attrs["unit"] = meta["unit"]
            if meta.get("ucd"):
                field_attrs["ucd"] = meta["ucd"]
        else:
            # Warn about missing metadata
            table_name = query_info.get("table", "unknown")
            app.logger.warning(
                "Missing metadata for column '%s' in table '%s'. Using fallback values.",
                col,
                table_name,
            )

            # Fallback: Handle special astronomical columns with hard-coded values
            if col.lower() in ["ra", "ra_deg"]:
                field_attrs["unit"] = "deg"
                field_attrs["ucd"] = "pos.eq.ra;meta.main"
            elif col.lower() in ["dec", "dec_deg"]:
                field_attrs["unit"] = "deg"
                field_attrs["ucd"] = "pos.eq.dec;meta.main"
            elif col.lower() in ["mag", "magnitude"]:
                field_attrs["unit"] = "mag"
                field_attrs["ucd"] = "phot.mag"

        ET.SubElement(table, "FIELD", field_attrs)

    # Add DATA element with TABLEDATA
    data_elem = ET.SubElement(table, "DATA")
    tabledata = ET.SubElement(data_elem, "TABLEDATA")

    # Add rows
    for row_data in data:
        tr = ET.SubElement(tabledata, "TR")
        for col in columns:
            td = ET.SubElement(tr, "TD")
            value = row_data.get(col, "")
            td.text = str(value) if value is not None else ""

    # Format and return the XML with proper indentation
    return format_xml_with_indentation(votable)


def create_error_votable(error_message, query=""):
    """
    Create a VOTable error response.

    Args:
        error_message: Error message string
        query: The original query

    Returns:
        String containing VOTable XML with error
    """
    votable = ET.Element("VOTABLE", {"version": "1.4", "xmlns": "http://www.ivoa.net/xml/VOTable/v1.4"})

    resource = ET.SubElement(votable, "RESOURCE", {"type": "results"})

    ET.SubElement(resource, "INFO", {"name": "QUERY_STATUS", "value": "ERROR"})

    ET.SubElement(resource, "INFO", {"name": "ERROR", "value": error_message})

    if query:
        ET.SubElement(resource, "INFO", {"name": "QUERY", "value": query})

    # Format and return the XML with proper indentation
    return format_xml_with_indentation(votable)


def dataframe_to_votable_data(df):
    """
    Convert a pandas DataFrame to VOTable data format.

    Args:
        df: pandas DataFrame

    Returns:
        Tuple of (data_list, columns_list) where data_list is a list of dicts
    """
    # Get column names
    columns = df.columns.tolist()

    # Convert DataFrame to list of dictionaries
    data = df.to_dict("records")

    return data, columns


@app.route("/")
def index():
    """Root endpoint with server information."""
    html = """
    <html>
    <head><title>TAP Server Prototype</title></head>
    <body>
        <h1>TAP Server Prototype</h1>
        <p>This is a prototype implementation of a TAP (Table Access Protocol) server.</p>

        <h2>Endpoints</h2>
        <ul>
            <li><strong>GET/POST /sync</strong> - Synchronous query endpoint</li>
            <li><strong>GET /capabilities</strong> - Service capabilities</li>
            <li><strong>GET /tables</strong> - Available tables</li>
        </ul>

        <h2>Example Query</h2>
        <p>Submit an ADQL query to the /sync endpoint:</p>
        <pre>
curl -X POST http://localhost:5000/sync \\
  -d "REQUEST=doQuery" \\
  -d "LANG=ADQL" \\
  -d "QUERY=SELECT TOP 10 ra, dec, mag FROM ztf_dr14 WHERE mag < 20"
        </pre>

        <h2>Supported ADQL Features</h2>
        <ul>
            <li>SELECT with column list or *</li>
            <li>FROM with table name</li>
            <li>WHERE clause with comparison operators</li>
            <li>CONTAINS with POINT and CIRCLE for cone searches</li>
            <li>TOP/LIMIT clause</li>
        </ul>

        <p><em>Note: This server uses the adql_to_lsdb module to
               convert ADQL to LSDB operations.</em></p>
    </body>
    </html>
    """
    return html


@app.route("/sync", methods=["GET", "POST"])
def sync_query():
    """
    Synchronous query endpoint following TAP specification.

    Accepts parameters:
        REQUEST: Must be 'doQuery'
        LANG: Query language (ADQL)
        QUERY: The ADQL query string
        FORMAT: Output format (default: votable)
    """
    # Get parameters from either GET or POST
    params = request.form if request.method == "POST" else request.args

    # Validate REQUEST parameter
    request_type = params.get("REQUEST", "")
    if request_type != "doQuery":
        error_msg = f"Invalid REQUEST parameter: {request_type}. Must be 'doQuery'."
        app.logger.warning("Invalid REQUEST parameter: '%s' (expected 'doQuery')", request_type)
        return Response(create_error_votable(error_msg), mimetype="application/xml", status=400)

    # Validate LANG parameter
    lang = params.get("LANG", "ADQL")
    if not re.match(r"^adql(-\d+\(\.\d+\)?)?", lang, re.IGNORECASE):
        error_msg = f"Unsupported query language: {lang}. Only ADQL is supported."
        app.logger.warning("Unsupported query language requested: %s", lang)
        return Response(create_error_votable(error_msg), mimetype="application/xml", status=400)

    # Get query
    query = params.get("QUERY", "")
    if not query:
        error_msg = "Missing required parameter: QUERY"
        app.logger.warning("Request is missing the required QUERY parameter")
        return Response(create_error_votable(error_msg), mimetype="application/xml", status=400)

    # Get format (default to votable)
    output_format = params.get("FORMAT", "votable").lower()

    try:
        # Check if this is a TAP_SCHEMA query
        if is_tap_schema_query(query):
            # Query the TAP_SCHEMA metadata
            data, result_columns = query_tap_schema(query)
            table_name = "tap_schema"
        else:
            # Parse the ADQL query to get entities
            entities = parse_adql_entities(query)

            # Get the table name from the query
            assert entities["tables"]
            table = entities["tables"][0]

            # Handle regular catalog query
            # Convert table name like 'gaiadr3.gaia' to URL format.
            # Use one of several catalog prefixes, depending on how
            # this service is deployed:
            #
            # 1. If we take it right from public HTTP:
            #    catalog_prefix = "https://data.lsdb.io/hats"
            # 2. If we use the LSDB backend that does server-side filtering,
            #    to save bandwidth:
            #    catalog_prefix = "http://epyc.astro.washington.edu:43210/hats"
            # 3. If we're running on epyc, this is the direct path to the data,
            #    for best performance:
            #    catalog_prefix = "/var/www/data.lsdb.io/html/hats"
            catalog_prefix = "/var/www/data.lsdb.io/html/hats"
            if "." in table:
                parts = table.split(".")
                catalog_url = f"{catalog_prefix}/{parts[0]}/{parts[1]}/"
            else:
                catalog_url = f"{catalog_prefix}/{table}/"

            filters = entities["conditions"]
            search_filter = None
            if entities.get("spatial_search"):
                spatial = entities["spatial_search"]
                if spatial["type"] == "ConeSearch":
                    search_filter = lsdb.ConeSearch(
                        ra=spatial["ra"],
                        dec=spatial["dec"],
                        radius_arcsec=spatial["radius"] * 3600,
                    )
                # TODO: Other supported filters should be constructed here
                # TODO: such as PolygonSearch

            cat = lsdb.open_catalog(
                catalog_url,
                columns=entities["columns"],
                search_filter=search_filter,
                # NOTE: these filters sort of work, but fail with string values like "VARIABLE"
                filters=filters,
            )
            result_df = cat.head(entities["limits"])

            # Convert DataFrame to VOTable data format
            data, result_columns = dataframe_to_votable_data(result_df)

            # Extract table name from query for metadata using regex
            # Good examples that match: "FROM ztf_dr14", "FROM gaia_dr3.gaia", "from MyTable WHERE"
            # Bad examples that don't match (return 'results'): "FROMAGE", "FROM WHERE", "FROM LIMIT"
            table_name = "results"

            # SQL keywords that should not be considered table names
            sql_keywords = {
                "WHERE",
                "SELECT",
                "ORDER",
                "GROUP",
                "HAVING",
                "LIMIT",
                "OFFSET",
                "UNION",
                "INTERSECT",
                "EXCEPT",
                "JOIN",
                "INNER",
                "LEFT",
                "RIGHT",
                "OUTER",
                "CROSS",
                "ON",
                "AS",
                "AND",
                "OR",
                "NOT",
                "IN",
                "EXISTS",
                "BETWEEN",
                "LIKE",
                "IS",
                "NULL",
                "DISTINCT",
                "ALL",
                "ANY",
                "SOME",
            }

            # Match FROM keyword followed by table name (optionally schema.table)
            # Pattern: \bFROM\s+ matches FROM with word boundary followed by whitespace
            # ([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?) captures table or schema.table
            match = re.search(
                r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\b", query, re.IGNORECASE
            )

            if match:
                candidate = match.group(1)
                # Validate the candidate is not a SQL keyword (check table part for schema.table)
                table_part = candidate.split(".")[-1]
                if table_part.upper() not in sql_keywords:
                    table_name = candidate

        # Get column metadata from tap_schema.db
        column_metadata = get_column_metadata(table_name)

        # Create query info
        query_info = {"query": query, "table": table_name}

        # Generate response based on format
        if output_format in ["votable", "votable/td"]:
            xml_response = create_votable_response(data, result_columns, query_info, column_metadata)
            return Response(xml_response, mimetype="application/xml")
        else:
            error_msg = f"Unsupported format: {output_format}"
            app.logger.warning("Unsupported output format requested: %s", output_format)
            return Response(create_error_votable(error_msg, query), mimetype="application/xml", status=400)

    except Exception as e:
        error_msg = f"Error processing query: {str(e)}"
        app.logger.error("Error processing query: %s. Query: %s", str(e), query, exc_info=True)
        return Response(create_error_votable(error_msg, query), mimetype="application/xml", status=500)


@app.route("/capabilities")
def capabilities():
    """Return service capabilities in VOSI format."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<capabilities xmlns="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
              xmlns:vr="http://www.ivoa.net/xml/VOResource/v1.0"
              xmlns:tr="http://www.ivoa.net/xml/TAPRegExt/v1.0"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <capability standardID="ivo://ivoa.net/std/TAP">
        <interface xsi:type="tr:ParamHTTP" role="std">
            <accessURL use="base">http://localhost:5000/</accessURL>
        </interface>
        <language>
            <name>ADQL</name>
            <version ivo-id="ivo://ivoa.net/std/ADQL#v2.0">2.0</version>
            <languageFeatures type="ivo://ivoa.net/std/TAPRegExt#features-adqlgeo">
                <feature>
                    <form>CIRCLE</form>
                </feature>
                <feature>
                    <form>POINT</form>
                </feature>
                <feature>
                    <form>CONTAINS</form>
                </feature>
                <feature>
                    <form>POLYGON</form>
                </feature>
            </languageFeatures>
        </language>
        <outputFormat>
            <mime>application/xml</mime>
            <alias>votable</alias>
        </outputFormat>
    </capability>
</capabilities>
"""
    return Response(xml, mimetype="application/xml")


def generate_tables_xml():
    """
    Generate tables metadata XML from tap_schema.db.

    Returns:
        String containing tableset XML with schemas, tables, and columns
    """
    # Create tableset root element
    tableset = ET.Element("tableset", {"xmlns": "http://www.ivoa.net/xml/VODataService/v1.1"})

    try:
        # Query schemas from database
        tap_schema_db.connect()
        schemas = tap_schema_db.query("SELECT schema_name, description FROM schemas ORDER BY schema_name")

        for schema_row in schemas:
            schema_name = schema_row["schema_name"]

            # Create schema element
            schema_elem = ET.SubElement(tableset, "schema")
            name_elem = ET.SubElement(schema_elem, "name")
            name_elem.text = schema_name

            if schema_row.get("description"):
                desc_elem = ET.SubElement(schema_elem, "description")
                desc_elem.text = schema_row["description"]

            # Query tables for this schema
            tables = tap_schema_db.query(
                "SELECT table_name, description FROM tables WHERE schema_name = ? ORDER BY table_name",
                (schema_name,),
            )

            for table_row in tables:
                table_name = table_row["table_name"]

                # Create table element
                table_elem = ET.SubElement(schema_elem, "table")
                table_name_elem = ET.SubElement(table_elem, "name")
                table_name_elem.text = table_name

                if table_row.get("description"):
                    table_desc_elem = ET.SubElement(table_elem, "description")
                    table_desc_elem.text = table_row["description"]

                # Query columns for this table
                # Table names in columns table are fully qualified (schema.table)
                full_table_name = f"{schema_name}.{table_name}"
                columns = tap_schema_db.query(
                    "SELECT column_name, datatype, unit, ucd, description "
                    "FROM columns WHERE table_name = ? ORDER BY column_name",
                    (full_table_name,),
                )

                for column_row in columns:
                    # Create column element
                    column_elem = ET.SubElement(table_elem, "column")

                    col_name_elem = ET.SubElement(column_elem, "name")
                    col_name_elem.text = column_row["column_name"]

                    if column_row.get("datatype"):
                        datatype_elem = ET.SubElement(column_elem, "dataType")
                        datatype_elem.text = column_row["datatype"]

                    if column_row.get("unit"):
                        unit_elem = ET.SubElement(column_elem, "unit")
                        unit_elem.text = column_row["unit"]

                    if column_row.get("ucd"):
                        ucd_elem = ET.SubElement(column_elem, "ucd")
                        ucd_elem.text = column_row["ucd"]

                    if column_row.get("description"):
                        col_desc_elem = ET.SubElement(column_elem, "description")
                        col_desc_elem.text = column_row["description"]

        # Format and return the XML with proper indentation
        return format_xml_with_indentation(tableset)

    except Exception as e:
        app.logger.error("Failed to generate tables XML from tap_schema.db: %s", str(e), exc_info=True)
        # Return minimal valid XML on error
        return '<?xml version="1.0" encoding="UTF-8"?>\n<tableset xmlns="http://www.ivoa.net/xml/VODataService/v1.1"/>'


@app.route("/tables")
def tables():
    """Return available tables metadata from tap_schema.db."""
    xml = generate_tables_xml()
    return Response(xml, mimetype="application/xml")


def main():
    """Main entry point for the TAP server."""
    parser = argparse.ArgumentParser(description="TAP Server Prototype")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode with verbose logging")
    args = parser.parse_args()

    # Configure logging level based on debug flag
    if args.debug:
        app.logger.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)

    port = 43213
    app.logger.info("Starting TAP Server Prototype...")
    app.logger.info("Server will be available at http://localhost:%d", port)
    if args.debug:
        app.logger.info("Debug mode enabled")
    app.logger.info("Press Ctrl+C to stop the server")
    app.run(host="0.0.0.0", port=port, debug=args.debug)


if __name__ == "__main__":
    main()
