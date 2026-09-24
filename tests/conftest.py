import pytest

from belb.config import BelbConfig, load_config


def pytest_addoption(parser):
    parser.addoption(
        "--config-dir",
        action="store",
        default=None,
        help="Path to directory with custom configs (corpora.yaml, kbs.yaml, ...)",
    )
    parser.addoption(
        "--default",
        action="store_true",
        default=True,
        help="Use default configuration (`--config-dir` gets merged and overwrites)",
    )


@pytest.fixture(scope="session")
def benchmark_config(request) -> BelbConfig:
    """Provides the benchmark configuration based on CLI flags."""
    default = request.config.getoption("--default")
    config_dir = request.config.getoption("--config-dir")

    # Load config with the provided path if any
    config = load_config(default=default, config_dir=config_dir)

    return config
