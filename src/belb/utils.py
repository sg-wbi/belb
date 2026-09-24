from enum import StrEnum
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import cast

from datasets import Dataset, config
from datasets.fingerprint import Hasher
from omegaconf import OmegaConf

NIL = "NIL"
ID_JOIN = ","
HF_USER = "bel-bench-local"


class EntityType(StrEnum):
    GENE = "GENE"
    DISEASE = "DISEASE"
    CHEMICAL = "CHEMICAL"
    SPECIES = "SPECIES"
    VARIANT = "VARIANT"
    CELL_LINE = "CELL-LINE"


def load_yaml_asset(path: Path | Traversable) -> dict:

    out = cast(dict, OmegaConf.to_container(OmegaConf.load(str(path))))

    return out


def get_fingerprint(params: dict):
    hasher = Hasher()
    hasher.update(params)
    return hasher.hexdigest()


def get_cache_dir(ds: Dataset):

    if ds.cache_files and len(ds.cache_files) > 0 and "filename" in ds.cache_files[0]:
        cache_dir = Path(ds.cache_files[0]["filename"]).parent
    else:
        raise RuntimeError(
            f"Could not determine cached from `ds._fingerprint={ds._fingerprint}`"
        )

    return cache_dir


def get_cache_path(fingerprint: str, ds: Dataset | None = None) -> str:
    """
    Get a stable cache path for a dataset operation.
    If base_ds is provided and has a cache file, use its directory.
    """
    cache_dir = config.HF_DATASETS_CACHE

    if ds is not None:
        cache_dir = get_cache_dir(ds=ds)

    return str(Path(cache_dir) / f"cache-{fingerprint}.arrow")
