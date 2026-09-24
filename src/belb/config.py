from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

from loguru import logger
from omegaconf import DictConfig, ListConfig, OmegaConf


class BelbConfigError(Exception):
    pass


@dataclass
class DataFile:
    name: str
    uri: str


@dataclass
class ResourceConfig:
    hf_repo_id: str | None = None
    data_dir: str | None = None
    data_files: list[DataFile] | None = None

    def __post_init__(self):
        if self.hf_repo_id is None and self.data_files is None:
            raise ValueError("`hf_repo_id` and `data_files` cannot be both `None`.")


@dataclass
class TaskConfig:
    endtoend: bool = False
    inkb: bool = True
    composite_mentions: bool | str = "AND"
    document_level: bool = False
    boundary_tolerance: int | None = None

    def __post_init__(self):
        is_string = isinstance(self.composite_mentions, str)

        invalid_string = self.composite_mentions not in ["AND", "OR"]

        is_bool = isinstance(self.composite_mentions, bool)

        if (is_string and invalid_string) or (not is_string and not is_bool):
            raise ValueError(
                "`composite` accepts only the values: (`AND`,`OR`, true, false)"
            )


@dataclass
class PairConfig:
    name: str
    kb: str
    corpus: str
    kb_args: dict[str, Any] = field(default_factory=dict)
    corpus_args: dict[str, Any] = field(default_factory=dict)


@dataclass
class BelbConfig:
    task: TaskConfig
    corpora: dict[str, ResourceConfig]
    kbs: dict[str, ResourceConfig]
    pairs: list[PairConfig]

    def __post_init__(self):
        self.name_to_pair = {}
        for p in self.pairs:
            if p.name not in self.name_to_pair:
                self.name_to_pair[p.name] = p
            else:
                raise BelbConfigError("Names in `pairs.yaml` must be unique")


def log_overrides(cfg: Any, overrides: Any, prefix: str = "") -> list[str]:
    """Recursively collect overrides only if values differ. For kbs/corpora, top-level key replacement logs only the key."""
    overridden_keys = []
    # Determine if this is a top-level kbs/corpora call
    is_top_kb_or_corpora = prefix in ("kbs", "corpora")
    if isinstance(overrides, DictConfig):
        for key, value in overrides.items():
            full_key = f"{prefix}.{str(key)}" if prefix else key
            if key in cfg:
                if is_top_kb_or_corpora:
                    # Log only the top-level key when replaced fully
                    val_old = OmegaConf.to_container(cfg[key])
                    val_new = OmegaConf.to_container(value)
                    if val_old != val_new:
                        overridden_keys.append(f"`{prefix}.{key}`")
                elif isinstance(value, DictConfig) and isinstance(cfg[key], DictConfig):
                    overridden_keys.extend(
                        log_overrides(cfg[key], value, str(full_key))
                    )
                else:
                    # Only log if the value actually changes
                    val_old = (
                        OmegaConf.to_container(cfg[key])
                        if isinstance(cfg[key], (DictConfig, ListConfig))
                        else cfg[key]
                    )
                    val_new = (
                        OmegaConf.to_container(value)
                        if isinstance(value, (DictConfig, ListConfig))
                        else value
                    )
                    if val_old != val_new:
                        overridden_keys.append(f"`{str(full_key)}`")
    elif isinstance(overrides, ListConfig):
        if prefix == "pairs":
            base_names = {p.name for p in cfg if hasattr(p, "name")}
            for over in overrides:
                name = over.get("name")
                if name and name in base_names:
                    overridden_keys.append(f"`{prefix}` '{name}'")
        elif cfg != overrides:
            overridden_keys.append(f"`{prefix}`")
    return overridden_keys


def merge_pairs(base: ListConfig, overrides: ListConfig) -> ListConfig:
    """Merge pair lists based on name identity."""
    # Index base pairs by name
    base_indexed = {p.name: p for p in base}

    for over in overrides:
        if over.name in base_indexed:
            # Merge args if present
            existing = base_indexed[over.name]
            if "kb" in over:
                existing.kb = over.kb
            if "corpus" in over:
                existing.corpus = over.corpus
            if "kb_args" in over:
                existing.kb_args = OmegaConf.merge(existing.kb_args, over.kb_args)
            if "corpus_args" in over:
                existing.corpus_args = OmegaConf.merge(
                    existing.corpus_args, over.corpus_args
                )
        else:
            base.append(over)
    return base


def load_config(
    config_dir: str | Path | None = None,
    default: bool = True,
) -> BelbConfig:
    """
    Load the benchmark configuration.

    1. Load default configuration from package assets if `use_defaults` is True.
    2. If `config_dir` is provided, merge with local YAML files:
       - corpora.yaml, kbs.yaml, task.yaml, pairs.yaml
    4. Validate against BenchmarkConfig schema.
    """

    cfg = OmegaConf.create()

    # 1. Load defaults
    if default:
        asset_root = Path(str(files("belb.assets") / "config"))
        for component in ["task", "corpora", "kbs", "pairs"]:
            default_file = asset_root / f"{component}.yaml"
            if default_file.exists():
                cfg[component] = OmegaConf.load(default_file)

    # 2. Merge with local configuration if provided
    overridden_keys = []
    if config_dir is not None:
        config_dir = Path(config_dir)
        if not config_dir.is_dir():
            raise NotADirectoryError(f"Config path is not a directory: {config_dir}")

        for component in ["task", "corpora", "kbs", "pairs"]:
            local_file = config_dir / f"{component}.yaml"
            if local_file.exists():
                local_cfg = cast(ListConfig, OmegaConf.load(local_file))
                if component in cfg:
                    overridden_keys.extend(
                        log_overrides(cfg[component], local_cfg, component)
                    )
                    if component in ["kbs", "corpora"]:
                        # Replace (not merge) individual entries present in local config
                        for k, v in local_cfg.items():
                            cfg[component][k] = v
                    elif component == "pairs":
                        cfg[component] = merge_pairs(cfg[component], local_cfg)
                    else:
                        cfg[component] = OmegaConf.merge(cfg[component], local_cfg)
                else:
                    cfg[component] = local_cfg

    if overridden_keys:
        logger.debug(f"BELB config: overriding {', '.join(overridden_keys)}")

    # 4. Validate against the structured schema
    schema = OmegaConf.structured(BelbConfig)
    merged_cfg = OmegaConf.merge(schema, cfg)

    result = OmegaConf.to_object(merged_cfg)
    assert isinstance(result, BelbConfig)

    return result
