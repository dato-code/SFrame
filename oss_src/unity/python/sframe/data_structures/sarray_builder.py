"""
An interface for creating an SArray over time.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

from ..connect import main as glconnect
from ..cython.cy_sarray_builder import UnitySArrayBuilderProxy
from .sarray import SArray

class SArrayBuilder(object):
    """
    An interface to incrementally build an SArray element by element.

    Once closed, the SArray cannot be "reopened" using this interface.

    Parameters
    ----------
    num_segments : int, optional
        Number of segments that can be written in parallel.

    history_size : int, optional
        The number of elements to be cached as history. Caches the last
        `history_size` elements added with `append` or `append_multiple`.

    dtype : type, optional
        The type the resulting SArray will be. If None, the resulting SArray
        will take on the type of the first non-None value it receives.
        
    Returns
    -------
    out : SArrayBuilder

    Examples
    --------
    >>> from graphlab import SArrayBuilder

    >>> sb = SArrayBuilder()

    >>> sb.append(1)

    >>> sb.append([2,3])

    >>> sb.close()
    dtype: int
    Rows: 3
    [1, 2, 3]

    """
    def __init__(self, num_segments=1, history_size=10, dtype=None):
        self._builder = UnitySArrayBuilderProxy(glconnect.get_client())
        if dtype is None:
            dtype = type(None)
        self._builder.init(num_segments, history_size, dtype)
        self._block_size = 1024

    def append(self, data, segment=0):
        """
        Append a single element to an SArray.

        Throws a RuntimeError if the type of `data` is incompatible with
        the type of the SArray. 

        Parameters
        ----------
        data  : any SArray-supported type
            A data element to add to the SArray.

        segment : int
            The segment to write this element. Each segment is numbered
            sequentially, starting with 0. Any value in segment 1 will be after
            any value in segment 0, and the order of elements in each segment is
            preserved as they are added.
        """
        self._builder.append(data, segment)
        
    def append_multiple(self, data, segment=0):
        """
        Append multiple elements to an SArray.

        Throws a RuntimeError if the type of `data` is incompatible with
        the type of the SArray. 

        Parameters
        ----------
        data  : any SArray-supported type
            A data element to add to the SArray.

        segment : int
            The segment to write this element. Each segment is numbered
            sequentially, starting with 0. Any value in segment 1 will be after
            any value in segment 0, and the order of elements in each segment is
            preserved as they are added.
        """
        if not hasattr(data, '__iter__'):
            raise TypeError("append_multiple must be passed an iterable object")
        tmp_list = []
        block_pos = 0
        first = True
        while block_pos == self._block_size or first:
            first = False
            for i in data:
                tmp_list.append(i)
                ++block_pos
                if block_pos == self._block_size:
                    break
            self._builder.append_multiple(tmp_list, segment)
            tmp_list = []

    def get_type(self):
        """
        The type the result SArray will be if `close` is called.
        """
        return self._builder.get_type()

    def read_history(self, num=10):
        """
        Outputs the last `num` elements that were appended either by `append` or
        `append_multiple`.

        Returns
        -------
        out : list

        """
        if num < 0:
          num = 0
        return self._builder.read_history(num)

    def close(self):
        """
        Creates an SArray from all values that were appended to the
        SArrayBuilder. No function that appends data may be called after this
        is called.

        Returns
        -------
        out : SArray

        """
        return SArray(_proxy=self._builder.close())
