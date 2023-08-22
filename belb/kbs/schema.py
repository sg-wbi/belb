#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BELB KB Schema
"""


from typing import Optional

from omegaconf import OmegaConf
from sqlalchemy import types as sql_types
from sqlalchemy.schema import Column, Index, MetaData, Table, UniqueConstraint

from belb.kbs.db import SqlDialects
from belb.kbs.parser import BaseKbConfig
from belb.utils import StrEnum


class Tables(StrEnum):
    """
    Table
    """

    KB = "kb"
    HISTORY = "history"
    IDENTIFIER_MAPPING = "identifier_mapping"
    CITATIONS = "citations"
    FOREIGN_IDENTIFIERS = "foreign_identifiers"
    IDENTIFIER_HOMONYMS = "identifier_homonyms"
    FOREIGN_NAME_HOMONYMS = "foreign_name_homonyms"
    NAME_HOMONYMS = "name_homonyms"


class BelbKbSchema:
    """
    Base BELB DB schema
    """

    def __init__(
        self,
        db_config: str,
        kb_config: BaseKbConfig,
    ):
        self.db_config = OmegaConf.load(db_config)
        # NOTE: sqlite has no schema
        if self.db_config.db.dialect == SqlDialects.SQLITE:
            self.db_config.schema.name = None
        self.schema_config = self.db_config.schema
        self.kb_config = kb_config
        self.metadata = MetaData()

    @property
    def schema_name(self) -> Optional[str]:
        """
        Schema name
        """

        return self.schema_config.get("name")

    @property
    def base_tables(self) -> list[Tables]:
        """
        Base KB tables
        """
        tables = [Tables.KB]
        if self.kb_config.string_identifier:
            tables.append(Tables.IDENTIFIER_MAPPING)
        if self.kb_config.history:
            tables.append(Tables.HISTORY)
        if self.kb_config.citations:
            tables.append(Tables.CITATIONS)
        if self.kb_config.foreign_identifier:
            tables.append(Tables.FOREIGN_IDENTIFIERS)

        return tables

    @property
    def dictionary_tables(self) -> list[Tables]:
        """
        Dictionary tables
        """

        dictionary_tables = [
            Tables.IDENTIFIER_HOMONYMS,
            Tables.NAME_HOMONYMS,
        ]

        if self.kb_config.foreign_identifier:
            dictionary_tables.extend([Tables.FOREIGN_NAME_HOMONYMS])

        return dictionary_tables

    def init_metadata(self, tables: Optional[list[Tables]] = None):
        """
        Create all tables in DB
        """

        tables = (
            tables if tables is not None else self.base_tables + self.dictionary_tables
        )

        for t in tables:
            self.get(t)

    def get_table_full_name(self, name: str) -> str:
        """
        Get table name
        """

        full_name = f"{self.kb_config.name}_{name}"

        schema_name = self.schema_config.get("name")

        if schema_name is not None:
            full_name = f"{schema_name}.{full_name}"

        return full_name

    def get(self, name: str, **kwargs) -> Table:
        """
        Call function according to name
        """

        assert name in Tables, f"Table `{name}` was not implemented in schema!"

        full_name = self.get_table_full_name(name)

        if full_name not in self.metadata.tables:
            do = f"get_{name}"
            if hasattr(self, do) and callable(func := getattr(self, do)):
                outputs = func(**kwargs)
            else:
                raise ValueError(f"{self.__class__.__name__} has not method `{do}`!")
        else:
            outputs = self.metadata.tables[full_name]

        return outputs

    def get_kb(self) -> Table:
        """
        Define `kb` table
        """

        name = f"{self.kb_config.name}_kb"

        unique_kb_entry = ["identifier", "name"]

        specs = [
            Column("uid", sql_types.Integer, primary_key=True),
            Column("identifier", sql_types.Integer, nullable=False),
            Column("description", sql_types.Integer, nullable=False),
            Column("name", sql_types.Text, nullable=False),
            Index(
                "index_identifier",
                "identifier",
                postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            ),
        ]

        if self.kb_config.foreign_identifier:
            specs.extend(
                [
                    Column("foreign_identifier", sql_types.Integer, nullable=False),
                    Index(
                        "index_foreign_identifier",
                        "foreign_identifier",
                        postgresql_tablespace=self.db_config.get(
                            "postgresql_tablespace"
                        ),
                    ),
                ]
            )
            unique_kb_entry.append("foreign_identifier")
            # unique_dictionary_entry.append("foreign_name")

        if self.kb_config.attribute:
            specs.extend(
                [
                    Column("attribute", sql_types.Text, nullable=False),
                    # Column("attribute_name", sql_types.Text, nullable=True),
                ]
            )
            # unique_dictionary_entry.append("attribute_name")

        specs.append(UniqueConstraint(*unique_kb_entry, name="unique_kb_entry"))

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_history(self) -> Table:
        """
        Define `history` table
        """

        name = f"{self.kb_config.name}_history"

        specs = [
            Column("old_identifier", sql_types.Text, primary_key=True),
            Column("new_identifier", sql_types.Text, nullable=False),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_identifier_mapping(self) -> Table:
        """
        Define `identifier mapping` table
        """

        name = f"{self.kb_config.name}_identifier_mapping"

        specs = [
            Column("original_identifier", sql_types.Text, primary_key=True),
            Column("internal_identifier", sql_types.Integer, nullable=False),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_citations(self) -> Table:
        """
        Define `citations` table
        """

        name = f"{self.kb_config.name}_citations"

        specs = [
            Column("pmid", sql_types.Integer, nullable=False),
            Column("identifier", sql_types.Text, nullable=False),
            Index(
                "index_citations_pmid",
                "pmid",
                postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            ),
            Index(
                "index_citations_identifier",
                "identifier",
                postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            ),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_foreign_identifiers(self):
        """
        Unique foreign identifiers
        """

        name = f"{self.kb_config.name}_foreign_identifiers"

        specs = [
            Column("identifier", sql_types.Integer, primary_key=True),
            Column("name", sql_types.Text, nullable=True),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_identifier_homonyms(self) -> Table:
        """
        Define `identifier homonyms` table
        """

        name = f"{self.kb_config.name}_identifier_homonyms"

        specs = [
            Column("homonym", sql_types.Text, primary_key=True),
            Column("identifier", sql_types.Integer, nullable=False),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_name_homonyms(self) -> Table:
        """
        Define `name homonyms` table
        """

        name = f"{self.kb_config.name}_name_homonyms"

        specs = [
            Column("uid", sql_types.Integer, primary_key=True),
            Column("name", sql_types.Text, nullable=False),
            Column("identifier", sql_types.Integer, nullable=False),
            Column("description", sql_types.Integer, nullable=False),
        ]

        if self.kb_config.foreign_identifier:
            specs.extend(
                [
                    Column("foreign_identifier", sql_types.Integer, primary_key=True),
                    # Index(
                    #     "index_foreign_identifier",
                    #     "foreign_identifier",
                    #     postgresql_tablespace=self.schema_config.get(
                    #         "postgresql_tablespace"
                    #     ),
                    # ),
                ]
            )

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table

    def get_foreign_name_homonyms(self) -> Table:
        """
        Define `name homonyms foreign identifier` table
        """

        name = f"{self.kb_config.name}_foreign_name_homonyms"

        specs = [
            Column("uid", sql_types.Integer, primary_key=True),
            Column("identifier", sql_types.Text, nullable=False),
            # Index(
            #     "index_identifier",
            #     "identifier",
            #     postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            # ),
        ]

        table = Table(
            name,
            self.metadata,
            schema=self.schema_config.get("name"),
            postgresql_tablespace=self.schema_config.get("postgresql_tablespace"),
            *specs,
        )

        return table
