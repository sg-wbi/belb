#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Basic download and extract utilities
"""

import ftplib
import gzip
import os
import shutil
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from socket import gaierror
from typing import Callable

import requests  # type: ignore
from loguru import logger
from tqdm import tqdm


def ftp_download(
    name: str, directory: str, file_name: str, out_file: str, overwrite: bool = False
):
    """
    Use ftplib to download FTP file
    """

    if os.path.exists(out_file) and not overwrite:

        logger.debug(
            "Skip download `{}/{}/{}`: file already exists. Use `overwrite=True` to download anyway",
            name,
            directory,
            file_name,
        )

    else:

        try:
            ftp = ftplib.FTP(name)
        except gaierror as error:
            raise ValueError(f"FTP base name `{name}` is invalid!") from error

        ftp.login()

        try:
            ftp.cwd(directory)
        except ftplib.error_perm as error:
            raise ValueError(
                f"FTP `{name}` does not have folder `{directory}`!"
            ) from error

        logger.debug(
            "Download `{}/{}/{}`: save at `{}`", name, directory, file_name, out_file
        )
        with open(out_file, "wb") as out:
            total = ftp.size(file_name)
            with tqdm(
                total=total,
                unit_scale=True,
                desc=file_name,
                miniters=1,
                file=sys.stdout,
                leave=False,
            ) as pbar:

                def update_context(data):
                    """
                    update
                    """
                    pbar.update(len(data))
                    out.write(data)

                ftp.retrbinary(f"RETR {file_name}", update_context)


def std_download(url: str, path: str, overwrite: bool = False):
    """
    Standard download
    """

    if os.path.exists(path) and not overwrite:

        logger.debug(
            "Skip download `{}`: file already exists. Use `overwrite=True` to download anyway",
            url,
        )

    else:

        logger.debug("Download `{}`: save at `{}`", url, path)

        try:

            response = requests.get(url, stream=True)

            total_size = int(response.headers.get("content-length", 0))
            block_size = 1024  # 1 Kibibyte

            pbar = tqdm(total=total_size, unit="iB", unit_scale=True)

            with open(path, "wb") as outfile:
                for data in response.iter_content(block_size):
                    pbar.update(len(data))
                    outfile.write(data)
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error : {e.errno} - {e}") from e


def download(
    base_url: str,
    file: str,
    directory: str,
    extract: bool = False,
    overwrite: bool = False,
):
    """
    Download file from given url

    Parameters
    ----------
    base_url : str
        Base url: `base_url/file` = full url
    file : str
        File: `base_url/file` = full url
    directory : str
        Where to store download
    extract : bool
        Extract archive
    overwrite : bool
        Overwrite download
    """

    os.makedirs(directory, exist_ok=True)

    file_path = os.path.join(directory, file)

    if base_url.startswith("ftp"):

        base_url = base_url[:-1] if base_url.endswith("/") else base_url

        ftp_folder_path = os.path.basename(base_url)
        ftp_name = base_url.replace("ftp://", "").split("/")[0]

        ftp_download(
            name=ftp_name,
            directory=ftp_folder_path,
            file_name=file,
            out_file=file_path,
            overwrite=overwrite,
        )

    else:

        base_url = f"{base_url}/" if not base_url.endswith("/") else base_url

        std_download(path=file_path, url=f"{base_url}{file}", overwrite=overwrite)

    if extract:
        extract_archive(file_path, directory)


@dataclass
class ArchiveHandler:
    """
    Wrapper for archive: define opening function
    """

    name: str
    open_fn: Callable
    get_name_fn: Callable


def get_archive_handler(infile: str):
    """
    Given a path determine opening function
    """

    suffixes = "".join(Path(infile).suffixes)
    suffix = "".join(x for x in suffixes if (x.isalpha() or x == "."))

    if zipfile.is_zipfile(infile):
        handler_name = "zip"
        open_fn = zipfile.ZipFile

        def get_name_fn(x):
            return next(iter(x.namelist())).split(os.sep)[0]

    elif tarfile.is_tarfile(infile):
        handler_name = "tar"

        def open_fn(x):  # type: ignore
            return tarfile.open(x, "r:*")

        def get_name_fn(x):
            return x.getnames()[0]

    elif suffix.endswith(".gz"):
        handler_name = "gunzip"
        open_fn = gzip.open  # type: ignore

        def get_name_fn(x):
            return os.path.basename(x.name).replace(".gz", "")

    else:
        raise ValueError(f"File : `{infile}` has unsupported format : `{suffix}`")

    return ArchiveHandler(name=handler_name, open_fn=open_fn, get_name_fn=get_name_fn)


def gunzip_shutil(source_filepath: str, dest_filepath: str, block_size: int = 65536):
    """
    Extract guzipped file
    """
    with gzip.open(source_filepath, "rb") as s_file, open(
        dest_filepath, "wb"
    ) as d_file:
        shutil.copyfileobj(s_file, d_file, block_size)


def extract_archive(in_file: str, directory: str):
    """
    Extract archive
    """

    handler = get_archive_handler(in_file)

    archive = handler.open_fn(in_file)
    out_name = handler.get_name_fn(archive)

    out_file = os.path.join(directory, out_name)

    if not os.path.exists(out_file):
        logger.debug("Extracting `{}` into - {}", in_file, out_name)

        if handler.name == "gunzip":
            gunzip_shutil(source_filepath=in_file, dest_filepath=out_file)
        else:
            archive.extractall(directory)
