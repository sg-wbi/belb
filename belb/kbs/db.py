#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to database
"""

from typing import Iterator, Optional

import pandas as pd
from loguru import logger
from omegaconf import OmegaConf
from sqlalchemy.engine import Connection, Engine, Row, create_engine
from sqlalchemy.pool import PoolProxiedConnection
from sqlalchemy.schema import CreateSchema, Table
from sqlalchemy.sql.expression import Selectable

from belb.utils import StrEnum


class SqlDialects(StrEnum):
    """
    Availabel SQL dialaects
    """

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class SqlDrivers(StrEnum):
    """
    Availabel sqlalchemy drivers
    """

    PSYCOPG = "psycopg"
    PYSQLITE = "pysqlite"


class DatabaseConnection:
    """
    Base interface to the database
    """

    def __init__(
        self,
        config: OmegaConf,
        file: Optional[str] = None,
        debug: bool = False,
    ):
        self.db_config = config.db

        assert (
            self.db_config.dialect in SqlDialects
        ), f"Support only the following SQL dialects: {list(SqlDialects)}"

        self.file = file
        self.debug = debug
        self._connection: Optional[Connection] = None
        self._engine: Optional[Engine] = None
        self._raw_connection: Optional[PoolProxiedConnection] = None

    def open(self):
        """
        Open connection to database
        """

        database = f"{self.db_config.dialect}+{self.db_config.driver}://"

        if self.db_config.dialect == SqlDialects.SQLITE:
            if self.file is not None:
                database += f"/{self.file}"
            else:
                logger.warning(
                    "Using sqlite database w/o file: DATA WILL NOT BE SAVED (in-memory)..."
                )
                database += "/"
        else:
            database += f"{self.db_config.user}:{self.db_config.pwd}@{self.db_config.host}/{self.db_config.db_name}"

        logger.debug("CONNECTION: {}", database)

        self.engine = create_engine(database, echo=self.debug)

        self.connection = self.engine.connect()

    def close(self):
        """
        Close connection to database
        """

        if self.connection is not None:

            self.connection.close()

    def __enter__(self):
        """
        Use as context manager
        """

        self.open()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Use as context manager
        """

        self.close()

    @property
    def name(self) -> str:
        """
        Shortcut to class name
        """
        return self.__class__.__name__

    @property
    def connection(self) -> Connection:
        """
        Connection property: ensure object is called as context manager
        """

        assert (
            self._connection is not None
        ), f"`connection` was not set: {self.name} is a context manager!"

        return self._connection

    @connection.setter
    def connection(self, connection: Connection):
        """
        Set connection
        """

        self._connection = connection

    @property
    def engine(self) -> Engine:
        """
        Connection property: ensure object is called as context manager
        """

        assert (
            self._engine is not None
        ), f"`engine` was not set: {self.name} is a context manager!"

        return self._engine

    @engine.setter
    def engine(self, engine: Engine):
        """
        Set engine
        """

        self._engine = engine

    def row_to_dict(self, row: Row) -> dict:
        """
        Convert row to dictionary
        """

        return dict(row._mapping)  # pylint: disable=protected-access

    def get_raw_dbapi_connection(self):
        """
        Get raw DBI connection
        # how-do-i-get-at-the-raw-dbapi-connection-when-using-an-engine
        See: https://docs.sqlalchemy.org/en/20/faq/connections.html
        """
        # pep-249 style PoolProxiedConnection (historically called a "connection fairy")
        connection_fairy = self.connection.connection

        # to bypass "connection_fairy", such as to set attributes on the
        # unproxied pep-249 DBAPI connection, use .dbapi_connection
        raw_dbapi_connection = connection_fairy.dbapi_connection

        return raw_dbapi_connection

    def get_raw_connction_cursor(self):
        """
        Get raw connection cursor
        # how-do-i-get-at-the-raw-dbapi-connection-when-using-an-engine
        See: https://docs.sqlalchemy.org/en/20/faq/connections.html
        """

        # pep-249 style PoolProxiedConnection (historically called a "connection fairy")
        connection_fairy = self.connection.connection

        # typically to run statements one would get a cursor() from this
        # object
        cursor_obj = connection_fairy.cursor()
        # ... work with cursor_obj

        return cursor_obj

    def query(self, query: Selectable, fetch_size: int = 100000) -> Iterator[dict]:
        """
        Query database
        """

        proxy = self.connection.execution_options(stream_results=True).execute(query)

        while "batch not empty":  # equivalent of 'while True', but clearer
            batch = proxy.fetchmany(fetch_size)  # 100,000 rows at a time

            if not batch:
                break

            for row in batch:
                yield self.row_to_dict(row)

    def create_schema(self, name: Optional[str] = None):
        """
        Create schema
        """

        if self.db_config.dialect == SqlDialects.POSTGRESQL or name is not None:
            self.connection.execute(CreateSchema(name, if_not_exists=True))

    def populate_table(
        self,
        table: Table,
        df: pd.DataFrame,
        # debug: bool = False,
    ):
        """
        Populate table
        """
        if self.db_config.dialect == SqlDialects.POSTGRESQL:
            records = [
                tuple(None if pd.isna(x) else x for x in row) for row in df.values
            ]
            cursor = self.get_raw_connction_cursor()
            # https://www.psycopg.org/psycopg3/docs/basic/copy.html
            with cursor.copy(
                f"COPY {table.name} {tuple(df.columns)} FROM STDIN"
            ) as copy:
                copy.write(records)
        else:
            self.connection.execute(table.insert(), df.to_dict("records"))

        logger.debug("COPY: {}", len(df))

        self.connection.commit()


# result = await raw_connection.driver_connection.copy_records_to_table(
#     table_name=table.name,
#     records=records,
#     columns=columns,
#     schema_name=schema_name,
# )
# logger.debug("{}", result)
