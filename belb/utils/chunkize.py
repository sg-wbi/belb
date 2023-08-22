# /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Input-output utils

This is mainly to have access to `gensim` `chunkize` without the need to install gensim all the time.
"""

import itertools
import multiprocessing
import os
import sys
import warnings
from typing import Iterable

import numpy as np
from loguru import logger

# Multiprocessing on Windows (and on OSX with python3.8+) uses "spawn" mode, which
# causes issues with pickling.
# So for these two platforms, use simpler serial processing in `chunkize`.
# See https://github.com/RaRe-Technologies/gensim/pull/2800#discussion_r410890171
if os.name == "nt" or (sys.platform == "darwin" and sys.version_info >= (3, 8)):

    def chunkize(
        corpus: Iterable, chunksize: int, maxsize: int = 0, as_numpy: bool = False
    ):
        """Split `corpus` into fixed-sized chunks, using :func:`~gensim.utils.chunkize_serial`.
        Parameters
        ----------
        corpus : iterable of object
            An iterable.
        chunksize : int
            Split `corpus` into chunks of this size.
        maxsize : int, optional
            Ignored. For interface compatibility only.
        as_numpy : bool, optional
            Yield chunks as `np.ndarray`s instead of lists?
        Yields
        ------
        list OR np.ndarray
            "chunksize"-ed chunks of elements from `corpus`.
        """
        if maxsize > 0:
            entity = "Windows" if os.name == "nt" else "OSX with python3.8+"
            warnings.warn(f"detected {entity}; aliasing chunkize to chunkize_serial")
        for chunk in chunkize_serial(corpus, chunksize, as_numpy=as_numpy):
            yield chunk

else:

    def chunkize(
        corpus: Iterable, chunksize: int, maxsize: int = 0, as_numpy: bool = False
    ):
        """Split `corpus` into fixed-sized chunks, using :func:`~gensim.utils.chunkize_serial`.
        Parameters
        ----------
        corpus : iterable of object
            An iterable.
        chunksize : int
            Split `corpus` into chunks of this size.
        maxsize : int, optional
            If > 0, prepare chunks in a background process, filling a chunk queue of size at most `maxsize`.
        as_numpy : bool, optional
            Yield chunks as `np.ndarray` instead of lists?
        Yields
        ------
        list OR np.ndarray
            "chunksize"-ed chunks of elements from `corpus`.
        Notes
        -----
        Each chunk is of length `chunksize`, except the last one which may be smaller.
        A once-only input stream (`corpus` from a generator) is ok, chunking is done efficiently via itertools.
        If `maxsize > 0`, don't wait idly in between successive chunk `yields`, but rather keep filling a short queue
        (of size at most `maxsize`) with forthcoming chunks in advance. This is realized by starting a separate process,
        and is meant to reduce I/O delays, which can be significant when `corpus` comes from a slow medium
        like HDD, database or network.
        If `maxsize == 0`, don't fool around with parallelism and simply yield the chunksize
        via :func:`~gensim.utils.chunkize_serial` (no I/O optimizations).
        Yields
        ------
        list of object OR np.ndarray
            Groups based on `iterable`
        """
        assert chunksize > 0

        if maxsize > 0:
            q: multiprocessing.Queue = multiprocessing.Queue(maxsize=maxsize)
            worker = InputQueue(
                q, corpus, chunksize, maxsize=maxsize, as_numpy=as_numpy
            )
            worker.daemon = True
            worker.start()
            while True:
                chunk = [q.get(block=True)]
                if chunk[0] is None:
                    break
                yield chunk.pop()
        else:
            for chunk in chunkize_serial(corpus, chunksize, as_numpy=as_numpy):
                yield chunk


def chunkize_serial(
    iterable: Iterable,
    chunksize: int,
    as_numpy: bool = False,
    dtype: np.dtype = np.float32,
):
    """Yield elements from `iterable` in "chunksize"-ed groups.
    The last returned element may be smaller if the length of collection is not divisible by `chunksize`.
    Parameters
    ----------
    iterable : iterable of object
        An iterable.
    chunksize : int
        Split iterable into chunks of this size.
    as_numpy : bool, optional
        Yield chunks as `np.ndarray` instead of lists.
    Yields
    ------
    list OR np.ndarray
        "chunksize"-ed chunks of elements from `iterable`.
    Examples
    --------
    .. sourcecode:: pycon
        >>> print(list(grouper(range(10), 3)))
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    it = iter(iterable)
    while True:
        if as_numpy:
            # convert each document to a 2d numpy array (~6x faster when transmitting
            # chunk data over the wire, in Pyro)
            wrapped_chunk = [
                [
                    np.array(doc, dtype=dtype)
                    for doc in itertools.islice(it, int(chunksize))
                ]
            ]
        else:
            wrapped_chunk = [list(itertools.islice(it, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        # memory opt: wrap the chunk and then pop(), to avoid leaving behind a dangling reference
        yield wrapped_chunk.pop()


class InputQueue(multiprocessing.Process):
    """Populate a queue of input chunks from a streamed corpus.
    Useful for reading and chunking corpora in the background, in a separate process,
    so that workers that use the queue are not starved for input chunks.
    """

    def __init__(
        self,
        q: multiprocessing.Queue,
        corpus: Iterable,
        chunksize: int,
        maxsize: int,
        as_numpy: bool,
    ):
        """
        Parameters
        ----------
        q : multiprocessing.Queue
            Enqueue chunks into this queue.
        corpus : iterable of iterable of (int, numeric)
            Corpus to read and split into "chunksize"-ed groups
        chunksize : int
            Split `corpus` into chunks of this size.
        as_numpy : bool, optional
            Enqueue chunks as `numpy.ndarray` instead of lists.
        """
        super(InputQueue, self).__init__()
        self.q = q
        self.maxsize = maxsize
        self.corpus = corpus
        self.chunksize = chunksize
        self.as_numpy = as_numpy

    def run(self):
        it = iter(self.corpus)
        while True:
            chunk = itertools.islice(it, self.chunksize)
            if self.as_numpy:
                # HACK XXX convert documents to numpy arrays, to save memory.
                # This also gives a scipy warning at runtime:
                # "UserWarning: indices array has non-integer dtype (float64)"
                wrapped_chunk = [[np.asarray(doc) for doc in chunk]]
            else:
                wrapped_chunk = [list(chunk)]

            if not wrapped_chunk[0]:
                self.q.put(None, block=True)
                break

            try:
                qsize = self.q.qsize()
            except NotImplementedError:
                qsize = "?"
            logger.debug(
                "prepared another chunk of %i documents (qsize=%s)",
                len(wrapped_chunk[0]),
                qsize,
            )
            self.q.put(wrapped_chunk.pop(), block=True)
