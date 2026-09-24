import argparse

from loguru import logger

from belb import load_config, load_corpus, load_kb


def main():
    parser = argparse.ArgumentParser(
        description="Push a BELB dataset (corpus, kb) to the HuggingFace Hub."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Config directory",
    )
    parser.add_argument(
        "--type",
        type=str,
        required=True,
        choices=["corpus", "kb"],
        help="Type of resource to push: corpus or kb.",
    )
    parser.add_argument(
        "--name", type=str, required=True, help="Name of the corpus or kb"
    )
    parser.add_argument(
        "--repo",
        type=str,
        required=True,
        help="Target HuggingFace Hub repo (username/repo_name)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Set uploaded dataset as private (forced to true for `type=corpus`).",
    )

    args = parser.parse_args()
    config = load_config(config_dir=args.config)

    if args.type == "corpus":
        args.private = True

    logger.info(
        "Pushing {} '{}' to {} (private={})",
        args.type,
        args.name,
        args.repo,
        args.private,
    )

    config.task.inkb = False
    config.task.endtoend = True
    config.task.composite_mentions = True

    # Resource loading: dataset should be a DatasetDict (HuggingFace-style)
    if args.type == "corpus":
        ds = load_corpus(
            name=args.name, config=config, annotation_patches=False, id_mapping=False
        )
        ds.push_to_hub(args.repo, private=args.private)

    elif args.type == "kb":
        ds = load_kb(name=args.name, config=config)
        for table in ["entities", "history"]:
            if table not in ds:
                continue
            ds[table].push_to_hub(args.repo, config_name=table, private=args.private)


if __name__ == "__main__":
    main()
