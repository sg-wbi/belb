from dataclasses import dataclass

from datasets import DatasetDict

from .config import BelbConfig, BelbConfigError, load_config
from .corpus import CORPUS_FEATURES, load_corpus
from .kb import ENTITIES_FEATURES, HISTORY_FEATURES, load_kb
from .kb.ncbi_taxonomy import load_taxid_names
from .utils import NIL, EntityType


@dataclass
class BelbPair:
    corpus: DatasetDict
    kb: DatasetDict


def load_pair(
    name: str,
    config: BelbConfig,
    names: bool = False,
    names_kwargs: dict | None = None,
    num_proc: int | None = None,
) -> BelbPair:
    """Load a corpus-kb pair by name."""

    try:
        pair = config.name_to_pair[name]
    except KeyError:
        raise BelbConfigError(f"Pairing '{name}' not found in configuration.")

    kb = load_kb(
        name=pair.kb,
        config=config,
        names=names,
        kb_kwargs=pair.kb_args,
        names_kwargs=names_kwargs,
        num_proc=num_proc,
    )

    corpus = load_corpus(
        name=pair.corpus,
        config=config,
        corpus_kwargs=pair.corpus_args,
        num_proc=num_proc,
    )

    return BelbPair(corpus=corpus, kb=kb)
