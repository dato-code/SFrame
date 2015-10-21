"""
This module defines the SArray class which provides the
ability to create, access and manipulate a remote scalable array object.

SArray acts similarly to pandas.Series but without indexing.
The data is immutable, homogeneous, and is stored on the GraphLab Server side.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from .. import connect as _mt
from ..connect import main as glconnect
from ..cython.cy_type_utils import pytype_from_dtype, infer_type_of_list, is_numeric_type
from ..cython.cy_sarray import UnitySArrayProxy
from ..cython.context import debug_trace as cython_context
from ..util import _make_internal_url
from .image import Image as _Image
from .. import aggregate as _aggregate
from ..deps import numpy, HAS_NUMPY
from ..deps import pandas, HAS_PANDAS

import time
import array
import datetime
import warnings
import numbers

__all__ = ['SArray']

def _create_sequential_sarray(size, start=0, reverse=False):
    if type(size) is not int:
        raise TypeError("size must be int")

    if type(start) is not int:
        raise TypeError("size must be int")

    if type(reverse) is not bool:
        raise TypeError("reverse must me bool")

    with cython_context():
        return SArray(_proxy=glconnect.get_unity().create_sequential_sarray(size, start, reverse))

class SArray(object):
    """
    An immutable, homogeneously typed array object backed by persistent storage.

    SArray is scaled to hold data that are much larger than the machine's main
    memory. It fully supports missing values and random access. The
    data backing an SArray is located on the same machine as the GraphLab
    Server process. Each column in an :py:class:`~graphlab.SFrame` is an
    SArray.

    Parameters
    ----------
    data : list | numpy.ndarray | pandas.Series | string
        The input data. If this is a list, numpy.ndarray, or pandas.Series,
        the data in the list is converted and stored in an SArray.
        Alternatively if this is a string, it is interpreted as a path (or
        url) to a text file. Each line of the text file is loaded as a
        separate row. If ``data`` is a directory where an SArray was previously
        saved, this is loaded as an SArray read directly out of that
        directory.

    dtype : {None, int, float, str, list, array.array, dict, datetime.datetime, graphlab.Image}, optional
        The data type of the SArray. If not specified (None), we attempt to
        infer it from the input. If it is a numpy array or a Pandas series, the
        dtype of the array/series is used. If it is a list, the dtype is
        inferred from the inner list. If it is a URL or path to a text file, we
        default the dtype to str.

    ignore_cast_failure : bool, optional
        If True, ignores casting failures but warns when elements cannot be
        casted into the specified dtype.

    Notes
    -----
    - If ``data`` is pandas.Series, the index will be ignored.
    - The datetime is based on the Boost datetime format (see http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html
      for details)

    Examples
    --------
    SArray can be constructed in various ways:

    Construct an SArray from list.

    >>> from graphlab import SArray
    >>> sa = SArray(data=[1,2,3,4,5], dtype=int)

    Construct an SArray from numpy.ndarray.

    >>> sa = SArray(data=numpy.asarray([1,2,3,4,5]), dtype=int)
    or:
    >>> sa = SArray(numpy.asarray([1,2,3,4,5]), int)

    Construct an SArray from pandas.Series.

    >>> sa = SArray(data=pd.Series([1,2,3,4,5]), dtype=int)
    or:
    >>> sa = SArray(pd.Series([1,2,3,4,5]), int)

    If the type is not specified, automatic inference is attempted:

    >>> SArray(data=[1,2,3,4,5]).dtype()
    int
    >>> SArray(data=[1,2,3,4,5.0]).dtype()
    float

    The SArray supports standard datatypes such as: integer, float and string.
    It also supports three higher level datatypes: float arrays, dict
    and list (array of arbitrary types).

    Create an SArray from a list of strings:

    >>> sa = SArray(data=['a','b'])

    Create an SArray from a list of float arrays;

    >>> sa = SArray([[1,2,3], [3,4,5]])

    Create an SArray from a list of lists:

    >>> sa = SArray(data=[['a', 1, {'work': 3}], [2, 2.0]])

    Create an SArray from a list of dictionaries:

    >>> sa = SArray(data=[{'a':1, 'b': 2}, {'b':2, 'c': 1}])

    Create an SArray from a list of datetime objects:

    >>> sa = SArray(data=[datetime.datetime(2011, 10, 20, 9, 30, 10)])

    Construct an SArray from local text file. (Only works for local server).

    >>> sa = SArray('/tmp/a_to_z.txt.gz')

    Construct an SArray from a text file downloaded from a URL.

    >>> sa = SArray('http://s3-us-west-2.amazonaws.com/testdatasets/a_to_z.txt.gz')

    **Numeric Operators**

    SArrays support a large number of vectorized operations on numeric types.
    For instance:

    >>> sa = SArray([1,1,1,1,1])
    >>> sb = SArray([2,2,2,2,2])
    >>> sc = sa + sb
    >>> sc
    dtype: int
    Rows: 5
    [3, 3, 3, 3, 3]
    >>> sc + 2
    dtype: int
    Rows: 5
    [5, 5, 5, 5, 5]

    Operators which are supported include all numeric operators (+,-,*,/), as
    well as comparison operators (>, >=, <, <=), and logical operators (&, | ).

    For instance:

    >>> sa = SArray([1,2,3,4,5])
    >>> (sa >= 2) & (sa <= 4)
    dtype: int
    Rows: 5
    [0, 1, 1, 1, 0]

    The numeric operators (+,-,*,/) also work on array types:

    >>> sa = SArray(data=[[1.0,1.0], [2.0,2.0]])
    >>> sa + 1
    dtype: list
    Rows: 2
    [array('f', [2.0, 2.0]), array('f', [3.0, 3.0])]
    >>> sa + sa
    dtype: list
    Rows: 2
    [array('f', [2.0, 2.0]), array('f', [4.0, 4.0])]

    The addition operator (+) can also be used for string concatenation:

    >>> sa = SArray(data=['a','b'])
    >>> sa + "x"
    dtype: str
    Rows: 2
    ['ax', 'bx']

    This can be useful for performing type interpretation of lists or
    dictionaries stored as strings:

    >>> sa = SArray(data=['a,b','c,d'])
    >>> ("[" + sa + "]").astype(list) # adding brackets make it look like a list
    dtype: list
    Rows: 2
    [['a', 'b'], ['c', 'd']]

    All comparison operations and boolean operators are supported and emit
    binary SArrays.

    >>> sa = SArray([1,2,3,4,5])
    >>> sa >= 2
    dtype: int
    Rows: 3
    [0, 1, 1, 1, 1]
    >>> (sa >= 2) & (sa <= 4)
    dtype: int
    Rows: 3
    [0, 1, 1, 1, 0]


    **Element Access and Slicing**
    SArrays can be accessed by integer keys just like a regular python list.
    Such operations may not be fast on large datasets so looping over an SArray
    should be avoided.

    >>> sa = SArray([1,2,3,4,5])
    >>> sa[0]
    1
    >>> sa[2]
    3
    >>> sa[5]
    IndexError: SFrame index out of range

    Negative indices can be used to access elements from the tail of the array

    >>> sa[-1] # returns the last element
    5
    >>> sa[-2] # returns the second to last element
    4

    The SArray also supports the full range of python slicing operators:

    >>> sa[1000:] # Returns an SArray containing rows 1000 to the end
    >>> sa[:1000] # Returns an SArray containing rows 0 to row 999 inclusive
    >>> sa[0:1000:2] # Returns an SArray containing rows 0 to row 1000 in steps of 2
    >>> sa[-100:] # Returns an SArray containing last 100 rows
    >>> sa[-100:len(sa):2] # Returns an SArray containing last 100 rows in steps of 2

    **Logical Filter**

    An SArray can be filtered using

    >>> array[binary_filter]

    where array and binary_filter are SArrays of the same length. The result is
    a new SArray which contains only elements of 'array' where its matching row
    in the binary_filter is non zero.

    This permits the use of boolean operators that can be used to perform
    logical filtering operations.  For instance:

    >>> sa = SArray([1,2,3,4,5])
    >>> sa[(sa >= 2) & (sa <= 4)]
    dtype: int
    Rows: 3
    [2, 3, 4]

    This can also be used more generally to provide filtering capability which
    is otherwise not expressible with simple boolean functions. For instance:

    >>> sa = SArray([1,2,3,4,5])
    >>> sa[sa.apply(lambda x: math.log(x) <= 1)]
    dtype: int
    Rows: 3
    [1, 2]

    This is equivalent to

    >>> sa.filter(lambda x: math.log(x) <= 1)
    dtype: int
    Rows: 3
    [1, 2]

    **Iteration**

    The SArray is also iterable, but not efficiently since this involves a
    streaming transmission of data from the server to the client. This should
    not be used for large data.

    >>> sa = SArray([1,2,3,4,5])
    >>> [i + 1 for i in sa]
    [2, 3, 4, 5, 6]

    This can be used to convert an SArray to a list:

    >>> sa = SArray([1,2,3,4,5])
    >>> l = list(sa)
    >>> l
    [1, 2, 3, 4, 5]
    """

    __slots__ = ["__proxy__", "_getitem_cache"]

    def __init__(self, data=[], dtype=None, ignore_cast_failure=False, _proxy=None):
        """
        __init__(data=list(), dtype=None, ignore_cast_failure=False)

        Construct a new SArray. The source of data includes: list,
        numpy.ndarray, pandas.Series, and urls.
        """
        _mt._get_metric_tracker().track('sarray.init')
        if dtype is not None and type(dtype) != type:
            raise TypeError('dtype must be a type, e.g. use int rather than \'int\'')

        if (_proxy):
            self.__proxy__ = _proxy
        elif type(data) == SArray:
            self.__proxy__ = data.__proxy__
        else:
            self.__proxy__ = UnitySArrayProxy(glconnect.get_client())
            # we need to perform type inference
            if dtype is None:
                if HAS_PANDAS and isinstance(data, pandas.Series):
                    # if it is a pandas series get the dtype of the series
                    dtype = pytype_from_dtype(data.dtype)
                    if dtype == object:
                        # we need to get a bit more fine grained than that
                        dtype = infer_type_of_list(data)

                elif HAS_NUMPY and isinstance(data, numpy.ndarray):
                    # first try the fast inproc method
                    try:
                        from .. import numpy_loader
                        if numpy_loader.numpy_activation_successful():
                            from ..numpy import _fast_numpy_to_sarray
                            ret = _fast_numpy_to_sarray(data)
                            # conversion is good!
                            # swap the proxy.
                            self.__proxy__, ret.__proxy__ = ret.__proxy__, self.__proxy__
                            return
                    except:
                        pass

                    # if it is a numpy array, get the dtype of the array
                    dtype = pytype_from_dtype(data.dtype)
                    if dtype == object:
                        # we need to get a bit more fine grained than that
                        dtype = infer_type_of_list(data)
                    if len(data.shape) == 2:
                        # we need to make it an array or a list
                        if dtype == float or dtype == int:
                            dtype = array.array
                        else:
                            dtype = list
                    elif len(data.shape) > 2:
                        raise TypeError("Cannot convert Numpy arrays of greater than 2 dimensions")

                elif (isinstance(data, str) or isinstance(data, unicode)):
                    # if it is a file, we default to string
                    dtype = str
                elif hasattr(data, '__iter__') and not isinstance(data, dict):
                    # if it is a list, Get the first type and make sure
                    # the remaining items are all of the same type
                    #
                    # While dicts are iterable, we disable constructing SArrays
                    # in this way since it is really strange that
                    # SArray({1:2,3:4}) --> SArray of [1,3]
                    dtype = infer_type_of_list(data)

            if HAS_PANDAS and isinstance(data, pandas.Series):
                with cython_context():
                    self.__proxy__.load_from_iterable(data.values, dtype, ignore_cast_failure)
            elif (HAS_NUMPY and isinstance(data, numpy.ndarray)) or (hasattr(data, '__iter__') and not isinstance(data, dict)):
                with cython_context():
                    self.__proxy__.load_from_iterable(data, dtype, ignore_cast_failure)
            elif (isinstance(data, str) or isinstance(data, unicode)):
                internal_url = _make_internal_url(data)
                with cython_context():
                    self.__proxy__.load_autodetect(internal_url, dtype)
            else:
                raise TypeError("Unexpected data source. " \
                                "Possible data source types are: list, " \
                                "numpy.ndarray, pandas.Series, and string(url)")

    @classmethod
    def date_range(cls,start_time,end_time,freq):
        '''
        Returns a new SArray that represents a fixed frequency datetime index.

        Parameters
        ----------
        start_time : datetime.datetime
          Left bound for generating dates.

        end_time : datetime.datetime
          Right bound for generating dates.

        freq : datetime.timedelta
          Fixed frequency between two consecutive data points.

        Returns
        -------
        out : SArray

        Examples
        --------
        >>> import datetime as dt
        >>> start = dt.datetime(2013, 5, 7, 10, 4, 10)
        >>> end = dt.datetime(2013, 5, 10, 10, 4, 10)
        >>> sa = gl.SArray.date_range(start,end,dt.timedelta(1))
        >>> print sa
        dtype: datetime
        Rows: 4
        [datetime.datetime(2013, 5, 7, 10, 4, 10),
         datetime.datetime(2013, 5, 8, 10, 4, 10),
         datetime.datetime(2013, 5, 9, 10, 4, 10),
         datetime.datetime(2013, 5, 10, 10, 4, 10)]
       '''

        if not isinstance(start_time,datetime.datetime):
            raise TypeError("The ``start_time`` argument must be from type datetime.datetime.")

        if not isinstance(end_time,datetime.datetime):
            raise TypeError("The ``end_time`` argument must be from type datetime.datetime.")

        if not isinstance(freq,datetime.timedelta):
            raise TypeError("The ``freq`` argument must be from type datetime.timedelta.")

        from .. import extensions
        return extensions.date_range(start_time,end_time,freq.total_seconds())

    @classmethod
    def from_const(cls, value, size):
        """
        Constructs an SArray of size with a const value.

        Parameters
        ----------
        value : [int | float | str | array.array | list | dict | datetime]
          The value to fill the SArray
        size : int
          The size of the SArray

        Examples
        --------
        Construct an SArray consisting of 10 zeroes:

        >>> graphlab.SArray.from_const(0, 10)
        """
        assert isinstance(size, (int, long)) and size >= 0, "size must be a positive int"
        if not isinstance(value, (type(None), int, float, str, array.array, list, dict, datetime.datetime)):
            raise TypeError('Cannot create sarray of value type %s' % str(type(value)))
        proxy = UnitySArrayProxy(glconnect.get_client())
        proxy.load_from_const(value, size)
        return cls(_proxy=proxy)

    @classmethod
    def from_sequence(cls, *args):
        """
        from_sequence(start=0, stop)

        Create an SArray from sequence

        .. sourcecode:: python

            Construct an SArray of integer values from 0 to 999

            >>> gl.SArray.from_sequence(1000)

            This is equivalent, but more efficient than:

            >>> gl.SArray(range(1000))

            Construct an SArray of integer values from 10 to 999

            >>> gl.SArray.from_sequence(10, 1000)

            This is equivalent, but more efficient than:

            >>> gl.SArray(range(10, 1000))

        Parameters
        ----------
        start : int, optional
            The start of the sequence. The sequence will contain this value.

        stop : int
          The end of the sequence. The sequence will not contain this value.

        """
        start = None
        stop = None
        # fill with args. This checks for from_sequence(100), from_sequence(10,100)
        if len(args) == 1:
            stop = args[0]
        elif len(args) == 2:
            start = args[0]
            stop = args[1]

        if stop is None and start is None:
            raise TypeError("from_sequence expects at least 1 argument. got 0")
        elif start is None:
            return _create_sequential_sarray(stop)
        else:
            size = stop - start
            # this matches the behavior of range
            # i.e. range(100,10) just returns an empty array
            if (size < 0):
                size = 0
            return _create_sequential_sarray(size, start)

    @classmethod
    def from_avro(cls, filename):
        """
        Construct an SArray from an Avro file. The SArray type is determined by
        the schema of the Avro file.

        Parameters
        ----------
        filename : str
          The Avro file to load into an SArray.

        Examples
        --------
        Construct an SArray from a local Avro file named 'data.avro':

        >>> graphlab.SArray.from_avro('/data/data.avro')

        Notes
        -----
        Currently only supports direct loading of files on the local filesystem.

        References
        ----------
        - `Avro Specification <http://avro.apache.org/docs/1.7.7/spec.html>`_
        """
        _mt._get_metric_tracker().track('sarray.from_avro')
        proxy = UnitySArrayProxy(glconnect.get_client())
        proxy.load_from_avro(filename)
        return cls(_proxy = proxy)

    def __get_content_identifier__(self):
        """
        Returns the unique identifier of the content that backs the SArray

        Notes
        -----
        Meant for internal use only.
        """
        with cython_context():
            return self.__proxy__.get_content_identifier()

    def save(self, filename, format=None):
        """
        Saves the SArray to file.

        The saved SArray will be in a directory named with the `targetfile`
        parameter.

        Parameters
        ----------
        filename : string
            A local path or a remote URL.  If format is 'text', it will be
            saved as a text file. If format is 'binary', a directory will be
            created at the location which will contain the SArray.

        format : {'binary', 'text', 'csv'}, optional
            Format in which to save the SFrame. Binary saved SArrays can be
            loaded much faster and without any format conversion losses.
            'text' and 'csv' are synonymous: Each SArray row will be written
            as a single line in an output text file. If not
            given, will try to infer the format from filename given. If file
            name ends with 'csv', 'txt' or '.csv.gz', then save as 'csv' format,
            otherwise save as 'binary' format.
        """
        from .sframe import SFrame as _SFrame

        if format == None:
            if filename.endswith(('.csv', '.csv.gz', 'txt')):
                format = 'text'
            else:
                format = 'binary'
        if format == 'binary':
            with cython_context():
                self.__proxy__.save(_make_internal_url(filename))
        elif format == 'text' or format == 'csv':
            sf = _SFrame({'X1':self})
            with cython_context():
                sf.__proxy__.save_as_csv(_make_internal_url(filename), {'header':False})
        else:
            raise ValueError("Unsupported format: {}".format(format))

    def _escape_space(self,s):
            return "".join([ch.encode('string_escape') if ch.isspace() else ch for ch in s])

    def __repr__(self):
        """
        Returns a string description of the SArray.
        """
        data_str = self.__str__()
        ret = "dtype: " + str(self.dtype().__name__) + "\n"
        if (self.__proxy__.has_size()):
            ret = ret + "Rows: " + str(self.size()) + "\n"
        else:
            ret = ret + "Rows: ?\n"
        ret = ret + data_str
        return ret

    def __str__(self):
        """
        Returns a string containing the first 100 elements of the array.
        """

        # If sarray is image, take head of elements casted to string.
        if self.dtype() == _Image:
            headln = str(list(self.astype(str).head(100)))
        else:
            headln = self._escape_space(str(list(self.head(100))))
            headln = unicode(headln.decode('string_escape'),'utf-8',errors='replace').encode('utf-8')
        if (self.__proxy__.has_size() == False or self.size() > 100):
            # cut the last close bracket
            # and replace it with ...
            headln = headln[0:-1] + ", ... ]"
        return headln

    def __nonzero__(self):
        """
        Returns true if the array is not empty.
        """
        return self.size() != 0

    def __len__(self):
        """
        Returns the length of the array
        """
        return self.size()

    def __iter__(self):
        """
        Provides an iterator to the contents of the array.
        """
        def generator():
            elems_at_a_time = 262144
            self.__proxy__.begin_iterator()
            ret = self.__proxy__.iterator_get_next(elems_at_a_time)
            while(True):
                for j in ret:
                    yield j

                if len(ret) == elems_at_a_time:
                    ret = self.__proxy__.iterator_get_next(elems_at_a_time)
                else:
                    break

        return generator()

    def __contains__(self, item):
        """
        Returns true if any element in this SArray is identically equal to item.

        Following are equivalent:

        >>> element in sa
        >>> sa.__contains__(element)

        For an element-wise contains see ``SArray.contains``

        """
        return (self == item).any()

    def contains(self, item):
        """
        Performs an element-wise substring search of "item". The current
        array must contains strings and item must be a string. Produces a 1
        for each row if item is a substring of the row and 0 otherwise.

        Conceptually equivalent to:

        >>> sa.apply(lambda x: item in x)

        Parameters
        ----------
        item : str
            The item to search for.

        Returns
        -------
        out : SArray
            A binary SArray where a non-zero value denotes that the string
            was found in the SArray. And 0 if it is not found.

        Examples
        --------
        >>> SArray(["abc","def","ghi"]).contains("a")
        dtype: int
        Rows: 3
        [1, 0, 0]
        """
        return SArray(_proxy = self.__proxy__.left_scalar_operator(item, 'in'))

    def __add__(self, other):
        """
        If other is a scalar value, adds it to the current array, returning
        the new result. If other is an SArray, performs an element-wise
        addition of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '+'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '+'))

    def __sub__(self, other):
        """
        If other is a scalar value, subtracts it from the current array, returning
        the new result. If other is an SArray, performs an element-wise
        subtraction of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '-'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '-'))

    def __mul__(self, other):
        """
        If other is a scalar value, multiplies it to the current array, returning
        the new result. If other is an SArray, performs an element-wise
        multiplication of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '*'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '*'))

    def __div__(self, other):
        """
        If other is a scalar value, divides each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise division of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '/'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '/'))

    def __truediv__(self, other):
        """
        If other is a scalar value, divides each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise division of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '/'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '/'))


    def __floordiv__(self, other):
        """
        If other is a scalar value, divides each element of the current array
        by the value, returning floor of the result. If other is an SArray, performs
        an element-wise division of the two arrays returning the floor of the result.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '/')).astype(int)
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '/')).astype(int)

    def __pow__(self, other):
        """
        If other is a scalar value, raises each element of the current array to
        the power of that value, returning floor of the result. If other
        is an SArray, performs an element-wise power of the two
        arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '**'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '**'))

    def __neg__(self):
        """
        Returns the negative of each element.
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(0, '-'))

    def __pos__(self):
        if self.dtype() not in [int, long, float, array.array]:
            raise RuntimeError("Runtime Exception. Unsupported type operation. "
                               "cannot perform operation + on type %s" % str(self.dtype()))

        with cython_context():
            return SArray(_proxy = self.__proxy__)

    def __abs__(self):
        """
        Returns the absolute value of each element.
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.left_scalar_operator(0, 'left_abs'))

    def __mod__(self, other):
        """
        Other must be a scalar value. Performs an element wise division remainder.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '%'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '%'))


    def __lt__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '<'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '<'))

    def __gt__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '>'))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '>'))


    def __le__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '<='))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '<='))


    def __ge__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '>='))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '>='))


    def __radd__(self, other):
        """
        Adds a scalar value to the current array.
        Returned array has the same type as the array on the right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '+'))


    def __rsub__(self, other):
        """
        Subtracts a scalar value from the current array.
        Returned array has the same type as the array on the right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '-'))


    def __rmul__(self, other):
        """
        Multiplies a scalar value to the current array.
        Returned array has the same type as the array on the right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '*'))


    def __rdiv__(self, other):
        """
        Divides a scalar value by each element in the array
        Returned array has the same type as the array on the right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '/'))

    def __rtruediv__(self, other):
        """
        Divides a scalar value by each element in the array
        Returned array has the same type as the array on the right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '/'))

    def __rfloordiv__(self, other):
        """
        Divides a scalar value by each element in the array returning the
        floored result.  Returned array has the same type as the array on the
        right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '/')).astype(int)


    def __rmod__(self, other):
        """
        Divides a scalar value by each element in the array returning the
        floored result.  Returned array has the same type as the array on the
        right hand side
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '%'))

    def __rpow__(self, other):
        """
        Raises each element of the current array to the power of that
        value, returning floor of the result.
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.right_scalar_operator(other, '**'))

    def __eq__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the new result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '=='))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '=='))


    def __ne__(self, other):
        """
        If other is a scalar value, compares each element of the current array
        by the value, returning the new result. If other is an SArray, performs
        an element-wise comparison of the two arrays.
        """
        with cython_context():
            if type(other) is SArray:
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '!='))
            else:
                return SArray(_proxy = self.__proxy__.left_scalar_operator(other, '!='))


    def __and__(self, other):
        """
        Perform a logical element-wise 'and' against another SArray.
        """
        if type(other) is SArray:
            with cython_context():
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '&'))
        else:
            raise TypeError("SArray can only perform logical and against another SArray")


    def __or__(self, other):
        """
        Perform a logical element-wise 'or' against another SArray.
        """
        if type(other) is SArray:
            with cython_context():
                return SArray(_proxy = self.__proxy__.vector_operator(other.__proxy__, '|'))
        else:
            raise TypeError("SArray can only perform logical or against another SArray")


    def __getitem__(self, other):
        """
        If the key is an SArray of identical length, this function performs a
        logical filter: i.e. it subselects all the elements in this array
        where the corresponding value in the other array evaluates to true.
        If the key is an integer this returns a single row of
        the SArray. If the key is a slice, this returns an SArray with the
        sliced rows. See the GraphLab Create User Guide for usage examples.
        """
        sa_len = len(self)
        if isinstance(other, numbers.Integral):
            if other < 0:
                other += sa_len
            if other >= sa_len:
                raise IndexError("SArray index out of range")

            try:
                lb, ub, value_list = self._getitem_cache
                if lb <= other < ub:
                    return value_list[other - lb]

            except AttributeError:
                pass

            # Not in cache, need to grab it
            block_size = 1024 * (32 if self.dtype() in [int, long, float] else 4)

            block_num = int(other // block_size)

            lb = block_num * block_size
            ub = min(sa_len, lb + block_size)

            val_list = list(SArray(_proxy = self.__proxy__.copy_range(lb, 1, ub)))
            self._getitem_cache = (lb, ub, val_list)
            return val_list[other - lb]

        elif type(other) is SArray:
            if len(other) != sa_len:
                raise IndexError("Cannot perform logical indexing on arrays of different length.")
            with cython_context():
                return SArray(_proxy = self.__proxy__.logical_filter(other.__proxy__))

        elif type(other) is slice:
            start = other.start
            stop = other.stop
            step = other.step
            if start is None:
                start = 0
            if stop is None:
                stop = sa_len
            if step is None:
                step = 1
            # handle negative indices
            if start < 0:
                start = sa_len + start
            if stop < 0:
                stop = sa_len + stop

            return SArray(_proxy = self.__proxy__.copy_range(start, step, stop))
        else:
            raise IndexError("Invalid type to use for indexing")

    def __materialize__(self):
        """
        For a SArray that is lazily evaluated, force persist this sarray
        to disk, committing all lazy evaluated operations.
        """
        with cython_context():
            self.__proxy__.materialize()

    def __is_materialized__(self):
        """
        Returns whether or not the sarray has been materialized.
        """
        return self.__proxy__.is_materialized()

    def size(self):
        """
        The size of the SArray.
        """
        return self.__proxy__.size()

    def dtype(self):
        """
        The data type of the SArray.

        Returns
        -------
        out : type
            The type of the SArray.

        Examples
        --------
        >>> sa = gl.SArray(["The quick brown fox jumps over the lazy dog."])
        >>> sa.dtype()
        str
        >>> sa = gl.SArray(range(10))
        >>> sa.dtype()
        int
        """
        return self.__proxy__.dtype()

    def head(self, n=10):
        """
        Returns an SArray which contains the first n rows of this SArray.

        Parameters
        ----------
        n : int
            The number of rows to fetch.

        Returns
        -------
        out : SArray
            A new SArray which contains the first n rows of the current SArray.

        Examples
        --------
        >>> gl.SArray(range(10)).head(5)
        dtype: int
        Rows: 5
        [0, 1, 2, 3, 4]
        """
        return SArray(_proxy=self.__proxy__.head(n))

    def vector_slice(self, start, end=None):
        """
        If this SArray contains vectors or lists, this returns a new SArray
        containing each individual element sliced, between start and
        end (exclusive).

        Parameters
        ----------
        start : int
            The start position of the slice.

        end : int, optional.
            The end position of the slice. Note that the end position
            is NOT included in the slice. Thus a g.vector_slice(1,3) will extract
            entries in position 1 and 2. If end is not specified, the return
            array will contain only one element, the element at the start
            position.

        Returns
        -------
        out : SArray
            Each individual vector sliced according to the arguments.

        Examples
        --------

        If g is a vector of floats:

        >>> g = SArray([[1,2,3],[2,3,4]])
        >>> g
        dtype: array
        Rows: 2
        [array('d', [1.0, 2.0, 3.0]), array('d', [2.0, 3.0, 4.0])]

        >>> g.vector_slice(0) # extracts the first element of each vector
        dtype: float
        Rows: 2
        [1.0, 2.0]

        >>> g.vector_slice(0, 2) # extracts the first two elements of each vector
        dtype: array.array
        Rows: 2
        [array('d', [1.0, 2.0]), array('d', [2.0, 3.0])]

        If a vector cannot be sliced, the result will be None:

        >>> g = SArray([[1],[1,2],[1,2,3]])
        >>> g
        dtype: array.array
        Rows: 3
        [array('d', [1.0]), array('d', [1.0, 2.0]), array('d', [1.0, 2.0, 3.0])]

        >>> g.vector_slice(2)
        dtype: float
        Rows: 3
        [None, None, 3.0]

        >>> g.vector_slice(0,2)
        dtype: list
        Rows: 3
        [None, array('d', [1.0, 2.0]), array('d', [1.0, 2.0])]

        If g is a vector of mixed types (float, int, str, array, list, etc.):

        >>> g = SArray([['a',1,1.0],['b',2,2.0]])
        >>> g
        dtype: list
        Rows: 2
        [['a', 1, 1.0], ['b', 2, 2.0]]

        >>> g.vector_slice(0) # extracts the first element of each vector
        dtype: list
        Rows: 2
        [['a'], ['b']]
        """
        if (self.dtype() != array.array) and (self.dtype() != list):
            raise RuntimeError("Only Vector type can be sliced")
        if end == None:
            end = start + 1

        with cython_context():
            return SArray(_proxy=self.__proxy__.vector_slice(start, end))

    def subslice(self, start=None, stop=None, step=None):
        """
        This returns an SArray with each element sliced accordingly to the
        slice specified. This is conceptually equivalent to:

        >>> g.apply(lambda x: x[start:step:stop])

        The SArray must be of type list, vector, or string.

        For instance:

        >>> g = SArray(["abcdef","qwerty"])
        >>> g.subslice(start=0, stop=2)
        dtype: str
        Rows: 2
        ["ab", "qw"]
        >>> g.subslice(3,-1)
        dtype: str
        Rows: 2
        ["de", "rt"]
        >>> g.subslice(3)
        dtype: str
        Rows: 2
        ["def", "rty"]

        >>> g = SArray([[1,2,3], [4,5,6]])
        >>> g.subslice(0, 1)
        dtype: str
        Rows: 2
        [[1], [4]]

        Parameters
        ----------
        start : int or None (default)
            The start position of the slice

        stop: int or None (default)
            The stop position of the slice

        step: int or None (default)
            The step size of the slice

        Returns
        -------
        out : SArray
            Each individual vector/string/list sliced according to the arguments.

        """
        if self.dtype() not in [str, array.array, list]:
            raise TypeError("SArray must contain strings, arrays or lists")
        from .. import extensions
        return extensions.sarray_subslice(self, start, step, stop)

    def _count_words(self, to_lower=True, delimiters=["\r", "\v", "\n", "\f", "\t", " "]):
        """
        This returns an SArray with, for each input string, a dict from the unique,
        delimited substrings to their number of occurrences within the original
        string.

        The SArray must be of type string.

        ..WARNING:: This function is deprecated, and will be removed in future
        versions of GraphLab Create. Please use the `text_analytics.count_words`
        function instead.

        Parameters
        ----------
        to_lower : bool, optional
            "to_lower" indicates whether to map the input strings to lower case
            before counts

        delimiters: list[string], optional
            "delimiters" is a list of which characters to delimit on to find tokens

        Returns
        -------
        out : SArray
            for each input string, a dict from the unique, delimited substrings
            to their number of occurrences within the original string.

        Examples
        --------
        >>> sa = graphlab.SArray(["The quick brown fox jumps.",
                                 "Word word WORD, word!!!word"])
        >>> sa._count_words()
        dtype: dict
        Rows: 2
        [{'quick': 1, 'brown': 1, 'jumps': 1, 'fox': 1, 'the': 1},
         {'word': 2, 'word,': 1, 'word!!!word': 1}]
            """

        if (self.dtype() != str):
            raise TypeError("Only SArray of string type is supported for counting bag of words")

        if (not all([len(delim) == 1 for delim in delimiters])):
            raise ValueError("Delimiters must be single-character strings")

        _mt._get_metric_tracker().track('sarray.count_words')

        # construct options, will extend over time
        options = dict()
        options["to_lower"] = to_lower == True
        # defaults to std::isspace whitespace delimiters if no others passed in
    	options["delimiters"] = delimiters

        with cython_context():
            return SArray(_proxy=self.__proxy__.count_bag_of_words(options))

    def _count_ngrams(self, n=2, method="word", to_lower=True, ignore_space=True):
        """
        For documentation, see graphlab.text_analytics.count_ngrams().

        ..WARNING:: This function is deprecated, and will be removed in future
        versions of GraphLab Create. Please use the `text_analytics.count_words`
        function instead.
        """
        if (self.dtype() != str):
            raise TypeError("Only SArray of string type is supported for counting n-grams")

        if (type(n) != int):
            raise TypeError("Input 'n' must be of type int")

        if (n < 1):
            raise ValueError("Input 'n' must be greater than 0")

        if (n > 5):
            warnings.warn("It is unusual for n-grams to be of size larger than 5.")

        _mt._get_metric_tracker().track('sarray.count_ngrams', properties={'n':n, 'method':method})

        # construct options, will extend over time
        options = dict()
        options["to_lower"] = to_lower == True
        options["ignore_space"] = ignore_space == True


        if method == "word":
            with cython_context():
                return SArray(_proxy=self.__proxy__.count_ngrams(n, options ))
        elif method == "character" :
            with cython_context():
                return SArray(_proxy=self.__proxy__.count_character_ngrams(n, options ))
        else:
            raise ValueError("Invalid 'method' input  value. Please input either 'word' or 'character' ")

    def dict_trim_by_keys(self, keys, exclude=True):
        """
        Filter an SArray of dictionary type by the given keys. By default, all
        keys that are in the provided list in ``keys`` are *excluded* from the
        returned SArray.

        Parameters
        ----------
        keys : list
            A collection of keys to trim down the elements in the SArray.

        exclude : bool, optional
            If True, all keys that are in the input key list are removed. If
            False, only keys that are in the input key list are retained.

        Returns
        -------
        out : SArray
            A SArray of dictionary type, with each dictionary element trimmed
            according to the input criteria.

        See Also
        --------
        dict_trim_by_values

        Examples
        --------
        >>> sa = graphlab.SArray([{"this":1, "is":1, "dog":2},
                                  {"this": 2, "are": 2, "cat": 1}])
        >>> sa.dict_trim_by_keys(["this", "is", "and", "are"], exclude=True)
        dtype: dict
        Rows: 2
        [{'dog': 2}, {'cat': 1}]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        _mt._get_metric_tracker().track('sarray.dict_trim_by_keys')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_trim_by_keys(keys, exclude))

    def dict_trim_by_values(self, lower=None, upper=None):
        """
        Filter dictionary values to a given range (inclusive). Trimming is only
        performed on values which can be compared to the bound values. Fails on
        SArrays whose data type is not ``dict``.

        Parameters
        ----------
        lower : int or long or float, optional
            The lowest dictionary value that would be retained in the result. If
            not given, lower bound is not applied.

        upper : int or long or float, optional
            The highest dictionary value that would be retained in the result.
            If not given, upper bound is not applied.

        Returns
        -------
        out : SArray
            An SArray of dictionary type, with each dict element trimmed
            according to the input criteria.

        See Also
        --------
        dict_trim_by_keys

        Examples
        --------
        >>> sa = graphlab.SArray([{"this":1, "is":5, "dog":7},
                                  {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_trim_by_values(2,5)
        dtype: dict
        Rows: 2
        [{'is': 5}, {'this': 2, 'cat': 5}]

        >>> sa.dict_trim_by_values(upper=5)
        dtype: dict
        Rows: 2
        [{'this': 1, 'is': 5}, {'this': 2, 'are': 1, 'cat': 5}]
        """

        if None != lower and (not is_numeric_type(type(lower))):
            raise TypeError("lower bound has to be a numeric value")

        if None != upper and (not is_numeric_type(type(upper))):
            raise TypeError("upper bound has to be a numeric value")

        _mt._get_metric_tracker().track('sarray.dict_trim_by_values')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_trim_by_values(lower, upper))

    def dict_keys(self):
        """
        Create an SArray that contains all the keys from each dictionary
        element as a list. Fails on SArrays whose data type is not ``dict``.

        Returns
        -------
        out : SArray
            A SArray of list type, where each element is a list of keys
            from the input SArray element.

        See Also
        --------
        dict_values

        Examples
        ---------
        >>> sa = graphlab.SArray([{"this":1, "is":5, "dog":7},
                                  {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_keys()
        dtype: list
        Rows: 2
        [['this', 'is', 'dog'], ['this', 'are', 'cat']]
        """
        _mt._get_metric_tracker().track('sarray.dict_keys')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_keys())

    def dict_values(self):
        """
        Create an SArray that contains all the values from each dictionary
        element as a list. Fails on SArrays whose data type is not ``dict``.

        Returns
        -------
        out : SArray
            A SArray of list type, where each element is a list of values
            from the input SArray element.

        See Also
        --------
        dict_keys

        Examples
        --------
        >>> sa = graphlab.SArray([{"this":1, "is":5, "dog":7},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_values()
        dtype: list
        Rows: 2
        [[1, 5, 7], [2, 1, 5]]

        """
        _mt._get_metric_tracker().track('sarray.dict_values')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_values())

    def dict_has_any_keys(self, keys):
        """
        Create a boolean SArray by checking the keys of an SArray of
        dictionaries. An element of the output SArray is True if the
        corresponding input element's dictionary has any of the given keys.
        Fails on SArrays whose data type is not ``dict``.

        Parameters
        ----------
        keys : list
            A list of key values to check each dictionary against.

        Returns
        -------
        out : SArray
            A SArray of int type, where each element indicates whether the
            input SArray element contains any key in the input list.

        See Also
        --------
        dict_has_all_keys

        Examples
        --------
        >>> sa = graphlab.SArray([{"this":1, "is":5, "dog":7}, {"animal":1},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_has_any_keys(["is", "this", "are"])
        dtype: int
        Rows: 3
        [1, 1, 0]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        _mt._get_metric_tracker().track('sarray.dict_has_any_keys')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_has_any_keys(keys))

    def dict_has_all_keys(self, keys):
        """
        Create a boolean SArray by checking the keys of an SArray of
        dictionaries. An element of the output SArray is True if the
        corresponding input element's dictionary has all of the given keys.
        Fails on SArrays whose data type is not ``dict``.

        Parameters
        ----------
        keys : list
            A list of key values to check each dictionary against.

        Returns
        -------
        out : SArray
            A SArray of int type, where each element indicates whether the
            input SArray element contains all keys in the input list.

        See Also
        --------
        dict_has_any_keys

        Examples
        --------
        >>> sa = graphlab.SArray([{"this":1, "is":5, "dog":7},
                                 {"this": 2, "are": 1, "cat": 5}])
        >>> sa.dict_has_all_keys(["is", "this"])
        dtype: int
        Rows: 2
        [1, 0]
        """
        if isinstance(keys, str) or (not hasattr(keys, "__iter__")):
            keys = [keys]

        _mt._get_metric_tracker().track('sarray.dict_has_all_keys')

        with cython_context():
            return SArray(_proxy=self.__proxy__.dict_has_all_keys(keys))

    def apply(self, fn, dtype=None, skip_undefined=True, seed=None):
        """
        apply(fn, dtype=None, skip_undefined=True, seed=None)

        Transform each element of the SArray by a given function. The result
        SArray is of type ``dtype``. ``fn`` should be a function that returns
        exactly one value which can be cast into the type specified by
        ``dtype``. If ``dtype`` is not specified, the first 100 elements of the
        SArray are used to make a guess about the data type.

        Parameters
        ----------
        fn : function
            The function to transform each element. Must return exactly one
            value which can be cast into the type specified by ``dtype``.
            This can also be a toolkit extension function which is compiled
            as a native shared library using SDK.


        dtype : {None, int, float, str, list, array.array, dict, graphlab.Image}, optional
            The data type of the new SArray. If ``None``, the first 100 elements
            of the array are used to guess the target data type.

        skip_undefined : bool, optional
            If True, will not apply ``fn`` to any undefined values.

        seed : int, optional
            Used as the seed if a random number generator is included in ``fn``.

        Returns
        -------
        out : SArray
            The SArray transformed by ``fn``. Each element of the SArray is of
            type ``dtype``.

        See Also
        --------
        SFrame.apply

        Examples
        --------
        >>> sa = graphlab.SArray([1,2,3])
        >>> sa.apply(lambda x: x*2)
        dtype: int
        Rows: 3
        [2, 4, 6]

        Using native toolkit extension function:

        .. code-block:: c++

            #include <graphlab/sdk/toolkit_function_macros.hpp>
            #include <cmath>

            using namespace graphlab;
            double logx(const flexible_type& x, double base) {
              return log((double)(x)) / log(base);
            }

            BEGIN_FUNCTION_REGISTRATION
            REGISTER_FUNCTION(logx, "x", "base");
            END_FUNCTION_REGISTRATION

        compiled into example.so

        >>> import example

        >>> sa = graphlab.SArray([1,2,4])
        >>> sa.apply(lambda x: example.logx(x, 2))
        dtype: float
        Rows: 3
        [0.0, 1.0, 2.0]
        """
        assert callable(fn), "Input function must be callable."

        dryrun = [fn(i) for i in self.head(100) if i is not None]
        if dtype == None:
            dtype = infer_type_of_list(dryrun)
        if not seed:
            seed = time.time()

        # log metric
        _mt._get_metric_tracker().track('sarray.apply')

        # First phase test if it is a toolkit function
        nativefn = None
        try:
            from .. import extensions
            nativefn = extensions._build_native_function_call(fn)
        except:
            # failure are fine. we just fall out into the next few phases
            pass

        if nativefn is not None:
            # this is a toolkit lambda. We can do something about it
            with cython_context():
                return SArray(_proxy=self.__proxy__.transform_native(nativefn, dtype, skip_undefined, seed))

        with cython_context():
            return SArray(_proxy=self.__proxy__.transform(fn, dtype, skip_undefined, seed))


    def filter(self, fn, skip_undefined=True, seed=None):
        """
        Filter this SArray by a function.

        Returns a new SArray filtered by this SArray.  If `fn` evaluates an
        element to true, this element is copied to the new SArray. If not, it
        isn't. Throws an exception if the return type of `fn` is not castable
        to a boolean value.

        Parameters
        ----------
        fn : function
            Function that filters the SArray. Must evaluate to bool or int.

        skip_undefined : bool, optional
            If True, will not apply fn to any undefined values.

        seed : int, optional
            Used as the seed if a random number generator is included in fn.

        Returns
        -------
        out : SArray
            The SArray filtered by fn. Each element of the SArray is of
            type int.

        Examples
        --------
        >>> sa = graphlab.SArray([1,2,3])
        >>> sa.filter(lambda x: x < 3)
        dtype: int
        Rows: 2
        [1, 2]
        """
        assert callable(fn), "Input must be callable"
        if not seed:
            seed = time.time()

        _mt._get_metric_tracker().track('sarray.filter')

        with cython_context():
            return SArray(_proxy=self.__proxy__.filter(fn, skip_undefined, seed))


    def sample(self, fraction, seed=None):
        """
        Create an SArray which contains a subsample of the current SArray.

        Parameters
        ----------
        fraction : float
            The fraction of the rows to fetch. Must be between 0 and 1.

        seed : int
            The random seed for the random number generator.

        Returns
        -------
        out : SArray
            The new SArray which contains the subsampled rows.

        Examples
        --------
        >>> sa = graphlab.SArray(range(10))
        >>> sa.sample(.3)
        dtype: int
        Rows: 3
        [2, 6, 9]
        """
        if (fraction > 1 or fraction < 0):
            raise ValueError('Invalid sampling rate: ' + str(fraction))
        if (self.size() == 0):
            return SArray()
        if not seed:
            seed = time.time()

        _mt._get_metric_tracker().track('sarray.sample')

        with cython_context():
            return SArray(_proxy=self.__proxy__.sample(fraction, seed))

    def _save_as_text(self, url):
        """
        Save the SArray to disk as text file.
        """
        raise NotImplementedError


    def all(self):
        """
        Return True if every element of the SArray evaluates to True. For
        numeric SArrays zeros and missing values (``None``) evaluate to False,
        while all non-zero, non-missing values evaluate to True. For string,
        list, and dictionary SArrays, empty values (zero length strings, lists
        or dictionaries) or missing values (``None``) evaluate to False. All
        other values evaluate to True.

        Returns True on an empty SArray.

        Returns
        -------
        out : bool

        See Also
        --------
        any

        Examples
        --------
        >>> graphlab.SArray([1, None]).all()
        False
        >>> graphlab.SArray([1, 0]).all()
        False
        >>> graphlab.SArray([1, 2]).all()
        True
        >>> graphlab.SArray(["hello", "world"]).all()
        True
        >>> graphlab.SArray(["hello", ""]).all()
        False
        >>> graphlab.SArray([]).all()
        True
        """
        with cython_context():
            return self.__proxy__.all()


    def any(self):
        """
        Return True if any element of the SArray evaluates to True. For numeric
        SArrays any non-zero value evaluates to True. For string, list, and
        dictionary SArrays, any element of non-zero length evaluates to True.

        Returns False on an empty SArray.

        Returns
        -------
        out : bool

        See Also
        --------
        all

        Examples
        --------
        >>> graphlab.SArray([1, None]).any()
        True
        >>> graphlab.SArray([1, 0]).any()
        True
        >>> graphlab.SArray([0, 0]).any()
        False
        >>> graphlab.SArray(["hello", "world"]).any()
        True
        >>> graphlab.SArray(["hello", ""]).any()
        True
        >>> graphlab.SArray(["", ""]).any()
        False
        >>> graphlab.SArray([]).any()
        False
        """
        with cython_context():
            return self.__proxy__.any()


    def max(self):
        """
        Get maximum numeric value in SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type.

        Returns
        -------
        out : type of SArray
            Maximum value of SArray

        See Also
        --------
        min

        Examples
        --------
        >>> graphlab.SArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).max()
        96
        """
        with cython_context():
            return self.__proxy__.max()


    def min(self):
        """
        Get minimum numeric value in SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type.

        Returns
        -------
        out : type of SArray
            Minimum value of SArray

        See Also
        --------
        max

        Examples
        --------
        >>> graphlab.SArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).min()

        """
        with cython_context():
            return self.__proxy__.min()

    def argmax(self):
        """
        Get the index of the maximum numeric value in SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type.

        Returns
        -------
        out : int
            Index of the maximum value of SArray

        See Also
        --------
        argmin

        Examples
        --------
        >>> graphlab.SArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).argmax()

        """
        from .sframe import SFrame as _SFrame

        if len(self) == 0:
            return None
        if not any([isinstance(self[0], i) for i in [int,float,long]]):
            raise TypeError("SArray must be of type 'int', 'long', or 'float'.")

        sf = _SFrame(self).add_row_number()
        sf_out = sf.groupby(key_columns=[],operations={'maximum_x1': _aggregate.ARGMAX('X1','id')})
        return sf_out['maximum_x1'][0]

    def argmin(self):
        """
        Get the index of the minimum numeric value in SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type.

        Returns
        -------
        out : int
            index of the minimum value of SArray

        See Also
        --------
        argmax

        Examples
        --------
        >>> graphlab.SArray([14, 62, 83, 72, 77, 96, 5, 25, 69, 66]).argmin()

        """
        from .sframe import SFrame as _SFrame

        if len(self) == 0:
            return None
        if not any([isinstance(self[0], i) for i in [int,float,long]]):
            raise TypeError("SArray must be of type 'int', 'long', or 'float'.")

        sf = _SFrame(self).add_row_number()
        sf_out = sf.groupby(key_columns=[],operations={'minimum_x1': _aggregate.ARGMIN('X1','id')})
        return sf_out['minimum_x1'][0]


    def sum(self):
        """
        Sum of all values in this SArray.

        Raises an exception if called on an SArray of strings, lists, or
        dictionaries. If the SArray contains numeric arrays (array.array) and
        all the arrays are the same length, the sum over all the arrays will be
        returned. Returns None on an empty SArray. For large values, this may
        overflow without warning.

        Returns
        -------
        out : type of SArray
            Sum of all values in SArray
        """
        with cython_context():
            return self.__proxy__.sum()

    def mean(self):
        """
        Mean of all the values in the SArray, or mean image.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type or non-Image type.

        Returns
        -------
        out : float | graphlab.Image
            Mean of all values in SArray, or image holding per-pixel mean
            across the input SArray.
        """
        with cython_context():
            if self.dtype() == _Image:
                from  .. import extensions
                return extensions.generate_mean(self)
            else:
                return self.__proxy__.mean()


    def std(self, ddof=0):
        """
        Standard deviation of all the values in the SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type or if `ddof` >= length of SArray.

        Parameters
        ----------
        ddof : int, optional
            "delta degrees of freedom" in the variance calculation.

        Returns
        -------
        out : float
            The standard deviation of all the values.
        """
        with cython_context():
            return self.__proxy__.std(ddof)


    def var(self, ddof=0):
        """
        Variance of all the values in the SArray.

        Returns None on an empty SArray. Raises an exception if called on an
        SArray with non-numeric type or if `ddof` >= length of SArray.

        Parameters
        ----------
        ddof : int, optional
            "delta degrees of freedom" in the variance calculation.

        Returns
        -------
        out : float
            Variance of all values in SArray.
        """
        with cython_context():
            return self.__proxy__.var(ddof)

    def num_missing(self):
        """
        Number of missing elements in the SArray.

        Returns
        -------
        out : int
            Number of missing values.
        """
        with cython_context():
            return self.__proxy__.num_missing()

    def nnz(self):
        """
        Number of non-zero elements in the SArray.

        Returns
        -------
        out : int
            Number of non-zero elements.
        """
        with cython_context():
            return self.__proxy__.nnz()

    def datetime_to_str(self,str_format="%Y-%m-%dT%H:%M:%S%ZP"):
        """
        Create a new SArray with all the values cast to str. The string format is
        specified by the 'str_format' parameter.

        Parameters
        ----------
        str_format : str
            The format to output the string. Default format is "%Y-%m-%dT%H:%M:%S%ZP".

        Returns
        -------
        out : SArray[str]
            The SArray converted to the type 'str'.

        Examples
        --------
        >>> dt = datetime.datetime(2011, 10, 20, 9, 30, 10, tzinfo=GMT(-5))
        >>> sa = graphlab.SArray([dt])
        >>> sa.datetime_to_str("%e %b %Y %T %ZP")
        dtype: str
        Rows: 1
        [20 Oct 2011 09:30:10 GMT-05:00]

        See Also
        ----------
        str_to_datetime

        References
        ----------
        [1] Boost date time from string conversion guide (http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html)

        """
        if(self.dtype() != datetime.datetime):
            raise TypeError("datetime_to_str expects SArray of datetime as input SArray")

        _mt._get_metric_tracker().track('sarray.datetime_to_str')
        with cython_context():
            return SArray(_proxy=self.__proxy__.datetime_to_str(str_format))

    def str_to_datetime(self,str_format="%Y-%m-%dT%H:%M:%S%ZP"):
        """
        Create a new SArray with all the values cast to datetime. The string format is
        specified by the 'str_format' parameter.

        Parameters
        ----------
        str_format : str
            The string format of the input SArray. Default format is "%Y-%m-%dT%H:%M:%S%ZP".
            If str_format is "ISO", the the format is "%Y%m%dT%H%M%S%F%q"
        Returns
        -------
        out : SArray[datetime.datetime]
            The SArray converted to the type 'datetime'.

        Examples
        --------
        >>> sa = graphlab.SArray(["20-Oct-2011 09:30:10 GMT-05:30"])
        >>> sa.str_to_datetime("%d-%b-%Y %H:%M:%S %ZP")
        dtype: datetime
        Rows: 1
        datetime.datetime(2011, 10, 20, 9, 30, 10, tzinfo=GMT(-5.5))

        See Also
        ----------
        datetime_to_str

        References
        ----------
        [1] boost date time to string conversion guide (http://www.boost.org/doc/libs/1_48_0/doc/html/date_time/date_time_io.html)

        """
        if(self.dtype() != str):
            raise TypeError("str_to_datetime expects SArray of str as input SArray")

        _mt._get_metric_tracker().track('sarray.str_to_datetime')
        with cython_context():
            return SArray(_proxy=self.__proxy__.str_to_datetime(str_format))

    def pixel_array_to_image(self, width, height, channels, undefined_on_failure=True, allow_rounding=False):
        """
        Create a new SArray with all the values cast to :py:class:`graphlab.image.Image`
        of uniform size.

        Parameters
        ----------
        width: int
            The width of the new images.

        height: int
            The height of the new images.

        channels: int.
            Number of channels of the new images.

        undefined_on_failure: bool , optional , default True
            If True, return None type instead of Image type in failure instances.
            If False, raises error upon failure.

        allow_rounding: bool, optional , default False
            If True, rounds non-integer values when converting to Image type.
            If False, raises error upon rounding.

        Returns
        -------
        out : SArray[graphlab.Image]
            The SArray converted to the type 'graphlab.Image'.

        See Also
        --------
        astype, str_to_datetime, datetime_to_str

        Examples
        --------
        The MNIST data is scaled from 0 to 1, but our image type only loads integer  pixel values
        from 0 to 255. If we just convert without scaling, all values below one would be cast to
        0.

        >>> mnist_array = graphlab.SArray('http://s3.amazonaws.com/dato-datasets/mnist/mnist_vec_sarray')
        >>> scaled_mnist_array = mnist_array * 255
        >>> mnist_img_sarray = gl.SArray.pixel_array_to_image(scaled_mnist_array, 28, 28, 1, allow_rounding = True)

        """
        if(self.dtype() != array.array):
            raise TypeError("array_to_img expects SArray of arrays as input SArray")

        num_to_test = 10

        num_test = min(self.size(), num_to_test)

        mod_values = [val % 1 for x in range(num_test) for val in self[x]]

        out_of_range_values = [(val > 255 or val < 0) for x in range(num_test) for val in self[x]]

        if sum(mod_values) != 0.0 and not allow_rounding:
            raise ValueError("There are non-integer values in the array data. Images only support integer data values between 0 and 255. To permit rounding, set the 'allow_rounding' paramter to 1.")

        if sum(out_of_range_values) != 0:
            raise ValueError("There are values outside the range of 0 to 255. Images only support integer data values between 0 and 255.")

        _mt._get_metric_tracker().track('sarray.pixel_array_to_img')

        from .. import extensions
        return extensions.vector_sarray_to_image_sarray(self, width, height, channels, undefined_on_failure)

    def astype(self, dtype, undefined_on_failure=False):
        """
        Create a new SArray with all values cast to the given type. Throws an
        exception if the types are not castable to the given type.

        Parameters
        ----------
        dtype : {int, float, str, list, array.array, dict, datetime.datetime}
            The type to cast the elements to in SArray

        undefined_on_failure: bool, optional
            If set to True, runtime cast failures will be emitted as missing
            values rather than failing.

        Returns
        -------
        out : SArray [dtype]
            The SArray converted to the type ``dtype``.

        Notes
        -----
        - The string parsing techniques used to handle conversion to dictionary
          and list types are quite generic and permit a variety of interesting
          formats to be interpreted. For instance, a JSON string can usually be
          interpreted as a list or a dictionary type. See the examples below.
        - For datetime-to-string  and string-to-datetime conversions,
          use sa.datetime_to_str() and sa.str_to_datetime() functions.
        - For array.array to graphlab.Image conversions, use sa.pixel_array_to_image()

        Examples
        --------
        >>> sa = graphlab.SArray(['1','2','3','4'])
        >>> sa.astype(int)
        dtype: int
        Rows: 4
        [1, 2, 3, 4]

        Given an SArray of strings that look like dicts, convert to a dictionary
        type:

        >>> sa = graphlab.SArray(['{1:2 3:4}', '{a:b c:d}'])
        >>> sa.astype(dict)
        dtype: dict
        Rows: 2
        [{1: 2, 3: 4}, {'a': 'b', 'c': 'd'}]
        """

        _mt._get_metric_tracker().track('sarray.astype.%s' % str(dtype.__name__))

        if (dtype == _Image) and (self.dtype() == array.array):
            raise TypeError("Cannot cast from image type to array with sarray.astype(). Please use sarray.pixel_array_to_img() instead.")

        with cython_context():
            return SArray(_proxy=self.__proxy__.astype(dtype, undefined_on_failure))

    def clip(self, lower=float('nan'), upper=float('nan')):
        """
        Create a new SArray with each value clipped to be within the given
        bounds.

        In this case, "clipped" means that values below the lower bound will be
        set to the lower bound value. Values above the upper bound will be set
        to the upper bound value. This function can operate on SArrays of
        numeric type as well as array type, in which case each individual
        element in each array is clipped. By default ``lower`` and ``upper`` are
        set to ``float('nan')`` which indicates the respective bound should be
        ignored. The method fails if invoked on an SArray of non-numeric type.

        Parameters
        ----------
        lower : int, optional
            The lower bound used to clip. Ignored if equal to ``float('nan')``
            (the default).

        upper : int, optional
            The upper bound used to clip. Ignored if equal to ``float('nan')``
            (the default).

        Returns
        -------
        out : SArray

        See Also
        --------
        clip_lower, clip_upper

        Examples
        --------
        >>> sa = graphlab.SArray([1,2,3])
        >>> sa.clip(2,2)
        dtype: int
        Rows: 3
        [2, 2, 2]
        """
        with cython_context():
            return SArray(_proxy=self.__proxy__.clip(lower, upper))

    def clip_lower(self, threshold):
        """
        Create new SArray with all values clipped to the given lower bound. This
        function can operate on numeric arrays, as well as vector arrays, in
        which case each individual element in each vector is clipped. Throws an
        exception if the SArray is empty or the types are non-numeric.

        Parameters
        ----------
        threshold : float
            The lower bound used to clip values.

        Returns
        -------
        out : SArray

        See Also
        --------
        clip, clip_upper

        Examples
        --------
        >>> sa = graphlab.SArray([1,2,3])
        >>> sa.clip_lower(2)
        dtype: int
        Rows: 3
        [2, 2, 3]
        """
        with cython_context():
            return SArray(_proxy=self.__proxy__.clip(threshold, float('nan')))


    def clip_upper(self, threshold):
        """
        Create new SArray with all values clipped to the given upper bound. This
        function can operate on numeric arrays, as well as vector arrays, in
        which case each individual element in each vector is clipped.

        Parameters
        ----------
        threshold : float
            The upper bound used to clip values.

        Returns
        -------
        out : SArray

        See Also
        --------
        clip, clip_lower

        Examples
        --------
        >>> sa = graphlab.SArray([1,2,3])
        >>> sa.clip_upper(2)
        dtype: int
        Rows: 3
        [1, 2, 2]
        """
        with cython_context():
            return SArray(_proxy=self.__proxy__.clip(float('nan'), threshold))

    def tail(self, n=10):
        """
        Get an SArray that contains the last n elements in the SArray.

        Parameters
        ----------
        n : int
            The number of elements to fetch

        Returns
        -------
        out : SArray
            A new SArray which contains the last n rows of the current SArray.
        """
        with cython_context():
            return SArray(_proxy=self.__proxy__.tail(n))


    def dropna(self):
        """
        Create new SArray containing only the non-missing values of the
        SArray.

        A missing value shows up in an SArray as 'None'.  This will also drop
        float('nan').

        Returns
        -------
        out : SArray
            The new SArray with missing values removed.
        """

        _mt._get_metric_tracker().track('sarray.dropna')

        with cython_context():
            return SArray(_proxy = self.__proxy__.drop_missing_values())

    def fillna(self, value):
        """
        Create new SArray with all missing values (None or NaN) filled in
        with the given value.

        The size of the new SArray will be the same as the original SArray. If
        the given value is not the same type as the values in the SArray,
        `fillna` will attempt to convert the value to the original SArray's
        type. If this fails, an error will be raised.

        Parameters
        ----------
        value : type convertible to SArray's type
            The value used to replace all missing values

        Returns
        -------
        out : SArray
            A new SArray with all missing values filled
        """
        _mt._get_metric_tracker().track('sarray.fillna')

        with cython_context():
            return SArray(_proxy = self.__proxy__.fill_missing_values(value))

    def topk_index(self, topk=10, reverse=False):
        """
        Create an SArray indicating which elements are in the top k.

        Entries are '1' if the corresponding element in the current SArray is a
        part of the top k elements, and '0' if that corresponding element is
        not. Order is descending by default.

        Parameters
        ----------
        topk : int
            The number of elements to determine if 'top'

        reverse : bool
            If True, return the topk elements in ascending order

        Returns
        -------
        out : SArray (of type int)

        Notes
        -----
        This is used internally by SFrame's topk function.
        """
        with cython_context():
            return SArray(_proxy = self.__proxy__.topk_index(topk, reverse))

    def sketch_summary(self, background=False, sub_sketch_keys=None):
        """
        Summary statistics that can be calculated with one pass over the SArray.

        Returns a graphlab.Sketch object which can be further queried for many
        descriptive statistics over this SArray. Many of the statistics are
        approximate. See the :class:`~graphlab.Sketch` documentation for more
        detail.

        Parameters
        ----------
        background : boolean, optional
          If True, the sketch construction will return immediately and the
          sketch will be constructed in the background. While this is going on,
          the sketch can be queried incrementally, but at a performance penalty.
          Defaults to False.

        sub_sketch_keys : int | str | list of int | list of str, optional
            For SArray of dict type, also constructs sketches for a given set of keys,
            For SArray of array type, also constructs sketches for the given indexes.
            The sub sketches may be queried using: :py:func:`~graphlab.Sketch.element_sub_sketch()`.
            Defaults to None in which case no subsketches will be constructed.

        Returns
        -------
        out : Sketch
            Sketch object that contains descriptive statistics for this SArray.
            Many of the statistics are approximate.
        """
        from ..data_structures.sketch import Sketch
        if (self.dtype() == _Image):
            raise TypeError("sketch_summary() is not supported for arrays of image type")
        if (type(background) != bool):
            raise TypeError("'background' parameter has to be a boolean value")
        if (sub_sketch_keys != None):
            if (self.dtype() != dict and self.dtype() != array.array):
                raise TypeError("sub_sketch_keys is only supported for SArray of dictionary or array type")
            if not hasattr(sub_sketch_keys, "__iter__"):
                sub_sketch_keys = [sub_sketch_keys]
            value_types = set([type(i) for i in sub_sketch_keys])
            if (len(value_types) != 1):
                raise ValueError("sub_sketch_keys member values need to have the same type.")
            value_type = value_types.pop();
            if (self.dtype() == dict and value_type != str):
                raise TypeError("Only string value(s) can be passed to sub_sketch_keys for SArray of dictionary type. "+
                    "For dictionary types, sketch summary is computed by casting keys to string values.")
            if (self.dtype() == array.array and value_type != int):
                raise TypeError("Only int value(s) can be passed to sub_sketch_keys for SArray of array type")
        else:
            sub_sketch_keys = list()

        _mt._get_metric_tracker().track('sarray.sketch_summary')
        return Sketch(self, background, sub_sketch_keys = sub_sketch_keys)

    def append(self, other):
        """
        Append an SArray to the current SArray. Creates a new SArray with the
        rows from both SArrays. Both SArrays must be of the same type.

        Parameters
        ----------
        other : SArray
            Another SArray whose rows are appended to current SArray.

        Returns
        -------
        out : SArray
            A new SArray that contains rows from both SArrays, with rows from
            the ``other`` SArray coming after all rows from the current SArray.

        See Also
        --------
        SFrame.append

        Examples
        --------
        >>> sa = graphlab.SArray([1, 2, 3])
        >>> sa2 = graphlab.SArray([4, 5, 6])
        >>> sa.append(sa2)
        dtype: int
        Rows: 6
        [1, 2, 3, 4, 5, 6]
        """
        _mt._get_metric_tracker().track('sarray.append')
        if type(other) is not SArray:
            raise RuntimeError("SArray append can only work with SArray")

        if self.dtype() != other.dtype():
            raise RuntimeError("Data types in both SArrays have to be the same")

        with cython_context():
            return SArray(_proxy = self.__proxy__.append(other.__proxy__))

    def unique(self):
        """
        Get all unique values in the current SArray.

        Raises a TypeError if the SArray is of dictionary type. Will not
        necessarily preserve the order of the given SArray in the new SArray.


        Returns
        -------
        out : SArray
            A new SArray that contains the unique values of the current SArray.

        See Also
        --------
        SFrame.unique
        """
        from .sframe import SFrame as _SFrame

        _mt._get_metric_tracker().track('sarray.unique')
        tmp_sf = _SFrame()
        tmp_sf.add_column(self, 'X1')

        res = tmp_sf.groupby('X1',{})

        return SArray(_proxy=res['X1'].__proxy__)

    def show(self, view=None):
        """
        show(view=None)
        Visualize the SArray with GraphLab Create :mod:`~graphlab.canvas`. This function starts Canvas
        if it is not already running. If the SArray has already been plotted,
        this function will update the plot.

        Parameters
        ----------
        view : str, optional
            The name of the SFrame view to show. Can be one of:

            - None: Use the default (depends on the dtype of the SArray).
            - 'Categorical': Shows most frequent items in this SArray, sorted
              by frequency. Only valid for str, int, or float dtypes.
            - 'Numeric': Shows a histogram (distribution of values) for the
              SArray. Only valid for int or float dtypes.
            - 'Dictionary': Shows a cross filterable list of keys (categorical)
              and values (categorical or numeric). Only valid for dict dtype.
            - 'Array': Shows a Numeric view, filterable by sub-column (index).
              Only valid for array.array dtype.
            - 'List': Shows a Categorical view, aggregated across all sub-
              columns (indices). Only valid for list dtype.

        Returns
        -------
        view : graphlab.canvas.view.View
            An object representing the GraphLab Canvas view

        See Also
        --------
        canvas

        Examples
        --------
        Suppose 'sa' is an SArray, we can view it in GraphLab Canvas using:

        >>> sa.show()

        If 'sa' is a numeric (int or float) SArray, we can view it as
        a categorical variable using:

        >>> sa.show(view='Categorical')
        """
        from ..visualization.show import show
        show(self, view=view)

    def item_length(self):
        """
        Length of each element in the current SArray.

        Only works on SArrays of dict, array, or list type. If a given element
        is a missing value, then the output elements is also a missing value.
        This function is equivalent to the following but more performant:

            sa_item_len =  sa.apply(lambda x: len(x) if x is not None else None)

        Returns
        -------
        out_sf : SArray
            A new SArray, each element in the SArray is the len of the corresponding
            items in original SArray.

        Examples
        --------
        >>> sa = SArray([
        ...  {"is_restaurant": 1, "is_electronics": 0},
        ...  {"is_restaurant": 1, "is_retail": 1, "is_electronics": 0},
        ...  {"is_restaurant": 0, "is_retail": 1, "is_electronics": 0},
        ...  {"is_restaurant": 0},
        ...  {"is_restaurant": 1, "is_electronics": 1},
        ...  None])
        >>> sa.item_length()
        dtype: int
        Rows: 6
        [2, 3, 3, 1, 2, None]
        """
        if (self.dtype() not in [list, dict, array.array]):
            raise TypeError("item_length() is only applicable for SArray of type list, dict and array.")

        _mt._get_metric_tracker().track('sarray.item_length')

        with cython_context():
            return SArray(_proxy = self.__proxy__.item_length())

    def split_datetime(self, column_name_prefix = "X", limit=None, tzone=False):
        """
        Splits an SArray of datetime type to multiple columns, return a
        new SFrame that contains expanded columns. A SArray of datetime will be
        split by default into an SFrame of 6 columns, one for each
        year/month/day/hour/minute/second element.

        **Column Naming**

        When splitting a SArray of datetime type, new columns are named:
        prefix.year, prefix.month, etc. The prefix is set by the parameter
        "column_name_prefix" and defaults to 'X'. If column_name_prefix is
        None or empty, then no prefix is used.

        **Timezone Column**
        If tzone parameter is True, then timezone information is represented
        as one additional column which is a float shows the offset from
        GMT(0.0) or from UTC.


        Parameters
        ----------
        column_name_prefix: str, optional
            If provided, expanded column names would start with the given prefix.
            Defaults to "X".

        limit: list[str], optional
            Limits the set of datetime elements to expand.
            Possible values are 'year','month','day','hour','minute','second',
            'weekday', 'isoweekday', 'tmweekday', and 'us'.
            If not provided, only ['year','month','day','hour','minute','second']
            are expanded.

            - 'year': The year number
            - 'month': A value between 1 and 12 where 1 is January.
            - 'day': Day of the months. Begins at 1.
            - 'hour': Hours since midnight.
            - 'minute': Minutes after the hour.
            - 'second': Seconds after the minute.
            - 'us': Microseconds after the second. Between 0 and 999,999.
            - 'weekday': A value between 0 and 6 where 0 is Monday.
            - 'isoweekday': A value between 1 and 7 where 1 is Monday.
            - 'tmweekday': A value between 0 and 7 where 0 is Sunday

        tzone: bool, optional
            A boolean parameter that determines whether to show timezone column or not.
            Defaults to False.

        Returns
        -------
        out : SFrame
            A new SFrame that contains all expanded columns

        Examples
        --------
        To expand only day and year elements of a datetime SArray

         >>> sa = SArray(
            [datetime(2011, 1, 21, 7, 7, 21, tzinfo=GMT(0)),
             datetime(2010, 2, 5, 7, 8, 21, tzinfo=GMT(4.5)])

         >>> sa.split_datetime(column_name_prefix=None,limit=['day','year'])
            Columns:
                day   int
                year  int
            Rows: 2
            Data:
            +-------+--------+
            |  day  |  year  |
            +-------+--------+
            |   21  |  2011  |
            |   5   |  2010  |
            +-------+--------+
            [2 rows x 2 columns]


        To expand only year and tzone elements of a datetime SArray
        with tzone column represented as a string. Columns are named with prefix:
        'Y.column_name'.

        >>> sa.split_datetime(column_name_prefix="Y",limit=['year'],tzone=True)
            Columns:
                Y.year  int
                Y.tzone float
            Rows: 2
            Data:
            +----------+---------+
            |  Y.year  | Y.tzone |
            +----------+---------+
            |    2011  |  0.0    |
            |    2010  |  4.5    |
            +----------+---------+
            [2 rows x 2 columns]
        """
        from .sframe import SFrame as _SFrame

        if self.dtype() != datetime.datetime:
            raise TypeError("Only column of datetime type is supported.")

        if column_name_prefix == None:
            column_name_prefix = ""
        if type(column_name_prefix) != str:
            raise TypeError("'column_name_prefix' must be a string")

        # convert limit to column_keys
        if limit != None:
            if (not hasattr(limit, '__iter__')):
                raise TypeError("'limit' must be a list");

            name_types = set([type(i) for i in limit])
            if (len(name_types) != 1):
                raise TypeError("'limit' contains values that are different types")

            if (name_types.pop() != str):
                raise TypeError("'limit' must contain string values.")

            if len(set(limit)) != len(limit):
                raise ValueError("'limit' contains duplicate values")

        column_types = []

        if(limit == None):
            limit = ['year','month','day','hour','minute','second']

        column_types = [int] * len(limit)

        if(tzone == True):
            limit += ['tzone']
            column_types += [float]

        _mt._get_metric_tracker().track('sarray.split_datetime')

        with cython_context():
           return _SFrame(_proxy=self.__proxy__.expand(column_name_prefix, limit, column_types))

    def unpack(self, column_name_prefix = "X", column_types=None, na_value=None, limit=None):
        """
        Convert an SArray of list, array, or dict type to an SFrame with
        multiple columns.

        `unpack` expands an SArray using the values of each list/array/dict as
        elements in a new SFrame of multiple columns. For example, an SArray of
        lists each of length 4 will be expanded into an SFrame of 4 columns,
        one for each list element. An SArray of lists/arrays of varying size
        will be expand to a number of columns equal to the longest list/array.
        An SArray of dictionaries will be expanded into as many columns as
        there are keys.

        When unpacking an SArray of list or array type, new columns are named:
        `column_name_prefix`.0, `column_name_prefix`.1, etc. If unpacking a
        column of dict type, unpacked columns are named
        `column_name_prefix`.key1, `column_name_prefix`.key2, etc.

        When unpacking an SArray of list or dictionary types, missing values in
        the original element remain as missing values in the resultant columns.
        If the `na_value` parameter is specified, all values equal to this
        given value are also replaced with missing values. In an SArray of
        array.array type, NaN is interpreted as a missing value.

        :py:func:`graphlab.SFrame.pack_columns()` is the reverse effect of unpack

        Parameters
        ----------
        column_name_prefix: str, optional
            If provided, unpacked column names would start with the given prefix.

        column_types: list[type], optional
            Column types for the unpacked columns. If not provided, column
            types are automatically inferred from first 100 rows. Defaults to
            None.

        na_value: optional
            Convert all values that are equal to `na_value` to
            missing value if specified.

        limit: list, optional
            Limits the set of list/array/dict keys to unpack.
            For list/array SArrays, 'limit' must contain integer indices.
            For dict SArray, 'limit' must contain dictionary keys.

        Returns
        -------
        out : SFrame
            A new SFrame that contains all unpacked columns

        Examples
        --------
        To unpack a dict SArray

        >>> sa = SArray([{ 'word': 'a',     'count': 1},
        ...              { 'word': 'cat',   'count': 2},
        ...              { 'word': 'is',    'count': 3},
        ...              { 'word': 'coming','count': 4}])

        Normal case of unpacking SArray of type dict:

        >>> sa.unpack(column_name_prefix=None)
        Columns:
            count   int
            word    str
        <BLANKLINE>
        Rows: 4
        <BLANKLINE>
        Data:
        +-------+--------+
        | count |  word  |
        +-------+--------+
        |   1   |   a    |
        |   2   |  cat   |
        |   3   |   is   |
        |   4   | coming |
        +-------+--------+
        [4 rows x 2 columns]
        <BLANKLINE>

        Unpack only keys with 'word':

        >>> sa.unpack(limit=['word'])
        Columns:
            X.word  str
        <BLANKLINE>
        Rows: 4
        <BLANKLINE>
        Data:
        +--------+
        | X.word |
        +--------+
        |   a    |
        |  cat   |
        |   is   |
        | coming |
        +--------+
        [4 rows x 1 columns]
        <BLANKLINE>

        >>> sa2 = SArray([
        ...               [1, 0, 1],
        ...               [1, 1, 1],
        ...               [0, 1]])

        Convert all zeros to missing values:

        >>> sa2.unpack(column_types=[int, int, int], na_value=0)
        Columns:
            X.0     int
            X.1     int
            X.2     int
        <BLANKLINE>
        Rows: 3
        <BLANKLINE>
        Data:
        +------+------+------+
        | X.0  | X.1  | X.2  |
        +------+------+------+
        |  1   | None |  1   |
        |  1   |  1   |  1   |
        | None |  1   | None |
        +------+------+------+
        [3 rows x 3 columns]
        <BLANKLINE>
        """
        from .sframe import SFrame as _SFrame

        if self.dtype() not in [dict, array.array, list]:
            raise TypeError("Only SArray of dict/list/array type supports unpack")

        if column_name_prefix == None:
            column_name_prefix = ""
        if type(column_name_prefix) != str:
            raise TypeError("'column_name_prefix' must be a string")

        # validdate 'limit'
        if limit != None:
            if (not hasattr(limit, '__iter__')):
                raise TypeError("'limit' must be a list");

            name_types = set([type(i) for i in limit])
            if (len(name_types) != 1):
                raise TypeError("'limit' contains values that are different types")

            # limit value should be numeric if unpacking sarray.array value
            if (self.dtype() != dict) and (name_types.pop() != int):
                raise TypeError("'limit' must contain integer values.")

            if len(set(limit)) != len(limit):
                raise ValueError("'limit' contains duplicate values")

        if (column_types != None):
            if not hasattr(column_types, '__iter__'):
                raise TypeError("column_types must be a list");

            for column_type in column_types:
                if (column_type not in (int, float, str, list, dict, array.array)):
                    raise TypeError("column_types contains unsupported types. Supported types are ['float', 'int', 'list', 'dict', 'str', 'array.array']")

            if limit != None:
                if len(limit) != len(column_types):
                    raise ValueError("limit and column_types do not have the same length")
            elif self.dtype() == dict:
                raise ValueError("if 'column_types' is given, 'limit' has to be provided to unpack dict type.")
            else:
                limit = range(len(column_types))

        else:
            head_rows = self.head(100).dropna()
            lengths = [len(i) for i in head_rows]
            if len(lengths) == 0 or max(lengths) == 0:
                raise RuntimeError("Cannot infer number of items from the SArray, SArray may be empty. please explicitly provide column types")

            # infer column types for dict type at server side, for list and array, infer from client side
            if self.dtype() != dict:
                length = max(lengths)
                if limit == None:
                    limit = range(length)
                else:
                    # adjust the length
                    length = len(limit)

                if self.dtype() == array.array:
                    column_types = [float for i in range(length)]
                else:
                    column_types = list()
                    for i in limit:
                        t = [(x[i] if ((x is not None) and len(x) > i) else None) for x in head_rows]
                        column_types.append(infer_type_of_list(t))

        _mt._get_metric_tracker().track('sarray.unpack')

        with cython_context():
            if (self.dtype() == dict and column_types == None):
                limit = limit if limit != None else []
                return _SFrame(_proxy=self.__proxy__.unpack_dict(column_name_prefix, limit, na_value))
            else:
                return _SFrame(_proxy=self.__proxy__.unpack(column_name_prefix, limit, column_types, na_value))

    def sort(self, ascending=True):
        """
        Sort all values in this SArray.

        Sort only works for sarray of type str, int and float, otherwise TypeError
        will be raised. Creates a new, sorted SArray.

        Parameters
        ----------
        ascending: boolean, optional
           If true, the sarray values are sorted in ascending order, otherwise,
           descending order.

        Returns
        -------
        out: SArray

        Examples
        --------
        >>> sa = SArray([3,2,1])
        >>> sa.sort()
        dtype: int
        Rows: 3
        [1, 2, 3]
        """
        from .sframe import SFrame as _SFrame

        if self.dtype() not in (int, float, str, datetime.datetime):
            raise TypeError("Only sarray with type (int, float, str, datetime.datetime) can be sorted")
        sf = _SFrame()
        sf['a'] = self
        return sf.sort('a', ascending)['a']
