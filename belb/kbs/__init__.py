#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Available knowledge bases with corresponding configurations and dictionary
"""
from ..preprocessing.data import Entities
from ..resources import Kbs
from .cellosaurus import cellosaurus
from .cellosaurus.cellosaurus import CellosaurusKbConfig, CellosaurusKbParser
from .ctd_chemicals import ctd_chemicals
from .ctd_chemicals.ctd_chemicals import (CtdChemicalsKbConfig,
                                          CtdChemicalsKbParser)
from .ctd_diseases import ctd_diseases
from .ctd_diseases.ctd_diseases import CtdDiseasesKbConfig, CtdDiseasesKbParser
from .dbsnp import dbsnp
from .dbsnp.dbsnp import DbSnpKbConfig, DbSnpKbParser
from .kb import BelbKb
from .ncbi_gene import ncbi_gene
from .ncbi_gene.ncbi_gene import NcbiGeneKbConfig, NcbiGeneKbParser
from .ncbi_taxonomy import ncbi_taxonomy
from .ncbi_taxonomy.ncbi_taxonomy import (NcbiTaxonomyKbConfig,
                                          NcbiTaxonomyKbParser)
from .parser import BaseKbConfig, BaseKbParser
from .schema import BelbKbSchema
from .umls import umls
from .umls.umls import UmlsKbConfig, UmlsKbParser

ENTITY_TO_KB_NAME: dict = {
    Entities.GENE: Kbs.NCBI_GENE.name,
    Entities.SPECIES: Kbs.NCBI_TAXONOMY.name,
    Entities.VARIANT: Kbs.DBSNP.name,
    Entities.CELL_LINE: Kbs.CELLOSAURUS.name,
    Entities.CHEMICAL: Kbs.CTD_CHEMICALS.name,
    Entities.DISEASE: Kbs.CTD_DISEASES.name,
    Entities.UMLS: Kbs.UMLS.name,
}

NAME_TO_KB_CONFIG = {
    Kbs.NCBI_TAXONOMY.name: NcbiTaxonomyKbConfig,
    Kbs.CTD_DISEASES.name: CtdDiseasesKbConfig,
    Kbs.CTD_CHEMICALS.name: CtdChemicalsKbConfig,
    Kbs.CELLOSAURUS.name: CellosaurusKbConfig,
    Kbs.UMLS.name: UmlsKbConfig,
    Kbs.NCBI_GENE.name: NcbiGeneKbConfig,
    Kbs.DBSNP.name: DbSnpKbConfig,
}

NAME_TO_KB_PARSER = {
    Kbs.NCBI_TAXONOMY.name: NcbiTaxonomyKbParser,
    Kbs.CTD_DISEASES.name: CtdDiseasesKbParser,
    Kbs.CTD_CHEMICALS.name: CtdChemicalsKbParser,
    Kbs.CELLOSAURUS.name: CellosaurusKbParser,
    Kbs.UMLS.name: UmlsKbParser,
    Kbs.NCBI_GENE.name: NcbiGeneKbParser,
    Kbs.DBSNP.name: DbSnpKbParser,
}

NAME_TO_KB_MODULE = {
    Kbs.NCBI_TAXONOMY.name: ncbi_taxonomy,
    Kbs.CTD_DISEASES.name: ctd_diseases,
    Kbs.CTD_CHEMICALS.name: ctd_chemicals,
    Kbs.CELLOSAURUS.name: cellosaurus,
    Kbs.UMLS.name: umls,
    Kbs.NCBI_GENE.name: ncbi_gene,
    Kbs.DBSNP.name: dbsnp,
}


class AutoKbParser:
    """
    Import util
    """

    @staticmethod
    def from_name(name: str, **kwargs) -> BaseKbParser:
        """
        Get kb parser from name
        """
        if name not in NAME_TO_KB_PARSER:
            raise ValueError(
                f"Unknown corpus `{name}`: choose one from {list(NAME_TO_KB_PARSER)}"
            )
        return NAME_TO_KB_PARSER[name](**kwargs)


class AutoBelbKbConfig:
    """
    Import util
    """

    @staticmethod
    def from_name(name: str, **kwargs) -> BaseKbConfig:
        """
        Get kb config from name
        """
        if name not in NAME_TO_KB_CONFIG:
            raise ValueError(
                f"Unknown corpus `{name}`: choose one from {list(NAME_TO_KB_CONFIG)}"
            )
        return NAME_TO_KB_CONFIG[name](**kwargs)


class AutoBelbKbSchema:
    """
    Import util
    """

    @staticmethod
    def from_name(db_config: str, **kwargs) -> BelbKbSchema:
        """
        Get kb schema from name
        """

        kb_config = AutoBelbKbConfig.from_name(**kwargs)

        return BelbKbSchema(db_config=db_config, kb_config=kb_config)


class AutoBelbKb:
    """
    Import util
    """

    @staticmethod
    def from_name(directory: str, debug: bool = False, **kwargs) -> BelbKb:
        """
        Get belb kb from name
        """

        schema = AutoBelbKbSchema.from_name(**kwargs)

        return BelbKb(directory=directory, schema=schema, debug=debug)
