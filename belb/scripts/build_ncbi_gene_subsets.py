#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate file with mapping from gene corpus name to species (NCBI Taxonomy IDs)
This is to speed up queries in NCBI Gene (restricting queries to species).
"""

import argparse

from belb import corpora
from belb.kbs import NcbiGeneKbConfig
# from belb.corpora.bc2 import Bc2CorpusConfig, Bc2CorpusParser, Bc2CorpusSubsets
# from belb.corpora.bioid import BioIdCorpusConfig, BioIdCorpusParser
# from belb.corpora.corpus import CorpusConverter, Entities
# from belb.corpora.nlm_gene import NlmGeneCorpusConfig, NLMGeneCorpusParser
# from belb.corpora.osiris import OsirisCorpusConfig, OsirisCorpusParser
# from belb.corpora.tmvar import TmVarCorpusConfig, TmVarCorpusParser
from belb.kbs.kb import BelbKb
from belb.utils import METADATA

# def main():
#     """
#     Run script
#     """
#
#     parser = argparse.ArgumentParser(
#         description="Generate meta-data file `corpus2species.json`"
#     )
#     parser.add_argument(
#         "--dir", type=str, required=True, help="Directory where BELB data is stored"
#     )
#     parser.add_argument(
#         "--pubtator",
#         type=str,
#         default=None,
#         help="Path to local PubTator sqlite DB (built from `bioconcepts2pubtator.offsets`)",
#     )
#     args = parser.parse_args()
#
#     kb = BelbKb(directory=args.dir, config=NcbiGeneKbConfig())
#
#     parsers = [
#         Bc2CorpusParser(config=Bc2CorpusConfig(subset=Bc2CorpusSubsets.FULL)),
#         NLMGeneCorpusParser(config=NlmGeneCorpusConfig()),
#         BioIdCorpusParser(config=BioIdCorpusConfig(entity_type=Entities.GENE)),
#         OsirisCorpusParser(config=OsirisCorpusConfig(entity_type=Entities.GENE)),
#         TmVarCorpusParser(config=TmVarCorpusConfig(entity_type=Entities.GENE)),
#     ]
#
#     species_subsets: dict = {"human": [9606]}
#
#     with kb as handle:
#
#         for parser in parsers:
#
#             species_subsets[parser.config.name] = set()
#
#             converter = CorpusConverter(
#                 directory=args.dir, parser=parser, pubtator=args.pubtator
#             )
#             converter.parser.on_before_load(
#                 directory=converter.download_directory, kb=handle
#             )
#             converter.load_data()
#             converter.parser.on_after_load(data=converter.data, kb=kb)
#
#             identifiers = converter.get_corpus_identifiers()
#
#             for batch in chunkize(identifiers, chunksize=10000):
#
#                 batch = tuple(batch)
#                 placeholders = kb.query_generator.get_placeholders(batch)
#                 query = f"select foreign_identifier from kb where identifier in ({placeholders})"
#                 df = handle.query(query, params=batch)
#
#                 species_subsets[parser.config.name].update(
#                     list(df["foreign_identifier"])
#                 )
#
#     save_json(
#         path=str(METADATA / "species_subsets.json"),
#         item={k: list(v) for k, v in species_subsets.items()},
#         kwargs={"json": {"indent": 2}},
#     )
#
#
# if __name__ == "__main__":
#     main()
