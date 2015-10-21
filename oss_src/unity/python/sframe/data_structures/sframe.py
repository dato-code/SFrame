"""
This module defines the SFrame class which provides the
ability to create, access and manipulate a remote scalable dataframe object.

SFrame acts similarly to pandas.DataFrame, but the data is completely immutable
and is stored column wise on the GraphLab Server side.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .. import connect as _mt
from ..connect import main as glconnect
from ..cython.cy_type_utils import infer_type_of_list
from ..cython.context import debug_trace as cython_context
from ..cython.cy_sframe import UnitySFrameProxy
from ..util import _make_internal_url
from .sarray import SArray, _create_sequential_sarray
from .. import aggregate
from .image import Image as _Image
from ..deps import pandas, HAS_PANDAS
from .grouped_sframe import GroupedSFrame
import array
from prettytable import PrettyTable
from textwrap import wrap
import datetime
import time
import itertools
import logging as _logging
import os
import subprocess
import uuid
import platform
import numbers
import sys
import csv

__all__ = ['SFrame']
__LOGGER__ = _logging.getLogger(__name__)

SFRAME_GARBAGE_COLLECTOR = []
SFRAME_GRAPHLABUTIL_REF = None

FOOTER_STRS = ['Note: Only the head of the SFrame is printed.',
               'You can use print_rows(num_rows=m, num_columns=n) to print more rows and columns.']

LAZY_FOOTER_STRS = ['Note: Only the head of the SFrame is printed. This SFrame is lazily evaluated.',
                    'You can use len(sf) to force materialization.']
root_package_name = __import__(__name__.split('.')[0]).__name__
SFRAME_ROOTS = [# Binary/lib location in production egg
                os.path.abspath(os.path.join(os.path.dirname(
                    os.path.realpath(__file__)), '..')),
                # Build tree location of SFrame binaries
                os.path.abspath(os.path.join(os.path.dirname(
                    os.path.realpath(__file__)),
                        '..', '..',  '..', '..','..','oss_src','sframe')),
                # Location of python sources
                os.path.abspath(os.path.join(os.path.dirname(
                    os.path.realpath(__file__)),
                        '..', '..',  '..', '..', 'unity', 'python', root_package_name)),
                # Build tree dependency location
                os.path.abspath(os.path.join(os.path.dirname(
                    os.path.realpath(__file__)),
                        '..', '..',  '..', '..', '..', '..', 'deps', 'local', 'lib'))
                ]

SPARK_UNITY = "spark_unity"
HDFS_LIB = "libhdfs.so"
RDD_JAR_FILE = "spark_unity.jar"
RDD_SUPPORT_INITED = False
BINARY_PATHS = {}
STAGING_DIR = None
RDD_SUPPORT = True
PRODUCTION_RUN = False
REMOTE_OS = None
SPARK_SUPPORT_NAMES = {'RDD_JAR_PATH': 'spark_unity.jar'}

first = True
for i in SFRAME_ROOTS:
    for key,val in SPARK_SUPPORT_NAMES.iteritems():
        tmp_path = os.path.join(i, val)
        if key not in BINARY_PATHS and os.path.isfile(tmp_path):
            BINARY_PATHS[key] = tmp_path
    if all(name in BINARY_PATHS for name in SPARK_SUPPORT_NAMES.keys()):
        if first:
            PRODUCTION_RUN = True
        break
    first = False

for name in SPARK_SUPPORT_NAMES.keys():
    if (name not in BINARY_PATHS):
        __LOGGER__.warn("GraphLab engine cannot find %s" % SPARK_SUPPORT_NAMES[name])

if not all(name in BINARY_PATHS for name in SPARK_SUPPORT_NAMES.keys()):
    RDD_SUPPORT = False

def get_spark_integration_jar_path():
    """
    The absolute path of the jar file required to enable GraphLab Create's
    integration with Apache Spark.
    """
    if 'RDD_JAR_PATH' not in BINARY_PATHS:
        raise RuntimeError("Could not find a spark integration jar.  "\
            "Does your version of GraphLab Create support Spark Integration (is it >= 1.0)?")
    return BINARY_PATHS['RDD_JAR_PATH']

def __rdd_support_init__(sprk_ctx,graphlab_util_ref):
    global REMOTE_OS
    global RDD_SUPPORT_INITED
    global STAGING_DIR
    global BINARY_PATHS

    if not RDD_SUPPORT or RDD_SUPPORT_INITED:
        return

    sprk_ctx._jsc.addJar(BINARY_PATHS['RDD_JAR_PATH'])

    # Make sure our GraphLabUtil scala functions are accessible from the driver
    try:
        graphlab_util_ref.getBinaryName()
    except:
        raise RuntimeError("Could not execute RDD translation functions. "\
                           "This means either SparkContext is not initialized correctly "\
                           "or %s is not accessible.\n "\
                           "jar file path: %s" % (RDD_JAR_FILE,BINARY_PATHS['RDD_JAR_PATH']))

    dummy_rdd = sprk_ctx.parallelize([1])

    if PRODUCTION_RUN and (sprk_ctx.master.startswith('yarn-client') or sprk_ctx.master.startswith('spark://')):
        # Get cluster operating system
        os_rdd = dummy_rdd.map(lambda x: platform.system())
        REMOTE_OS = os_rdd.collect()[0]

        # Set binary path
        for i in BINARY_PATHS.keys():
            s = BINARY_PATHS[i]
            if REMOTE_OS == 'Linux':
                BINARY_PATHS[i] = os.path.join(os.path.dirname(s),os.path.basename(s))
            elif REMOTE_OS == 'Darwin':
                BINARY_PATHS[i] = os.path.join(os.path.dirname(s),os.path.basename(s))
            else:
                raise RuntimeError("YARN cluster has unsupported operating system "\
                                    "(something other than Linux or Mac OS X). "\
                                    "Cannot convert RDDs on this cluster to SFrame.")

    # Create staging directory
    staging_dir = '.graphlabStaging'
    if sprk_ctx.master.startswith('yarn-client') or sprk_ctx.master.startswith('spark://'):
        # Get that staging directory's full name
        tmp_loc = graphlab_util_ref.getHadoopNameNode()

        STAGING_DIR = os.path.join(tmp_loc, "user", sprk_ctx.sparkUser(), staging_dir)
        if STAGING_DIR is None:
            raise RuntimeError("Failed to create a staging directory on HDFS. "\
                               "Do your cluster nodes have a working hdfs client?")

        # Actually create the staging dir
        unity = glconnect.get_unity()
        unity.__mkdir__(STAGING_DIR)
        unity.__chmod__(STAGING_DIR, 0777)
    elif sprk_ctx.master[0:5] == 'local':
        # Save the output sframes to the same temp workspace this engine is
        # using
        #TODO: Consider cases where server and client aren't on the same machine
        unity = glconnect.get_unity()
        STAGING_DIR = unity.get_current_cache_file_location()
        if STAGING_DIR is None:
            raise RuntimeError("Could not retrieve local staging directory! \
                    Please contact us on http://forum.dato.com.")
    else:
        raise RuntimeError("Your spark context's master is '" +
                str(sprk_ctx.master) +
                "'. Only 'local' and 'yarn-client' are supported.")

    RDD_SUPPORT_INITED = True

def load_sframe(filename):
    """
    Load an SFrame. The filename extension is used to determine the format
    automatically. This function is particularly useful for SFrames previously
    saved in binary format. For CSV imports the ``SFrame.read_csv`` function
    provides greater control. If the SFrame is in binary format, ``filename`` is
    actually a directory, created when the SFrame is saved.

    Parameters
    ----------
    filename : string
        Location of the file to load. Can be a local path or a remote URL.

    Returns
    -------
    out : SFrame

    See Also
    --------
    SFrame.save, SFrame.read_csv

    Examples
    --------
    >>> sf = graphlab.SFrame({'id':[1,2,3], 'val':['A','B','C']})
    >>> sf.save('my_sframe')        # 'my_sframe' is a directory
    >>> sf_loaded = graphlab.load_sframe('my_sframe')
    """
    sf = SFrame(data=filename)
    return sf


class SFrame(object):
    """
    A tabular, column-mutable dataframe object that can scale to big data. The
    data in SFrame is stored column-wise on the GraphLab Server side, and is
    stored on persistent storage (e.g. disk) to avoid being constrained by
    memory size.  Each column in an SFrame is a size-immutable
    :class:`~graphlab.SArray`, but SFrames are mutable in that columns can be
    added and subtracted with ease.  An SFrame essentially acts as an ordered
    dict of SArrays.

    Currently, we support constructing an SFrame from the following data
    formats:

    * csv file (comma separated value)
    * sframe directory archive (A directory where an sframe was saved
      previously)
    * general text file (with csv parsing options, See :py:meth:`read_csv()`)
    * a Python dictionary
    * pandas.DataFrame
    * JSON
    * Apache Avro
    * PySpark RDD

    and from the following sources:

    * your local file system
    * the GraphLab Server's file system
    * HDFS
    * Amazon S3
    * HTTP(S).

    Only basic examples of construction are covered here. For more information
    and examples, please see the `User Guide <https://dato.com/learn/user
    guide/index.html#Working_with_data_Tabular_data>`_, `API Translator
    <https://dato.com/learn/translator>`_, `How-Tos
    <https://dato.com/learn/how-to>`_, and data science `Gallery
    <https://dato.com/learn/gallery>`_.
    
    Parameters
    ----------
    data : array | pandas.DataFrame | string | dict, optional
        The actual interpretation of this field is dependent on the ``format``
        parameter. If ``data`` is an array or Pandas DataFrame, the contents are
        stored in the SFrame. If ``data`` is a string, it is interpreted as a
        file. Files can be read from local file system or urls (local://,
        hdfs://, s3://, http://).

    format : string, optional
        Format of the data. The default, "auto" will automatically infer the
        input data format. The inference rules are simple: If the data is an
        array or a dataframe, it is associated with 'array' and 'dataframe'
        respectively. If the data is a string, it is interpreted as a file, and
        the file extension is used to infer the file format. The explicit
        options are:

        - "auto"
        - "array"
        - "dict"
        - "sarray"
        - "dataframe"
        - "csv"
        - "tsv"
        - "sframe".

    See Also
    --------
    read_csv:
        Create a new SFrame from a csv file. Preferred for text and CSV formats,
        because it has a lot more options for controlling the parser.

    save : Save an SFrame for later use.

    Notes
    -----
    - When reading from HDFS on Linux we must guess the location of your java
      installation. By default, we will use the location pointed to by the
      JAVA_HOME environment variable.  If this is not set, we check many common
      installation paths. You may use two environment variables to override
      this behavior.  GRAPHLAB_JAVA_HOME allows you to specify a specific java
      installation and overrides JAVA_HOME.  GRAPHLAB_LIBJVM_DIRECTORY
      overrides all and expects the exact directory that your preferred
      libjvm.so file is located.  Use this ONLY if you'd like to use a
      non-standard JVM.

    Examples
    --------

    >>> import graphlab
    >>> from graphlab import SFrame

    **Construction**

    Construct an SFrame from a dataframe and transfers the dataframe object
    across the network.

    >>> df = pandas.DataFrame()
    >>> sf = SFrame(data=df)

    Construct an SFrame from a local csv file (only works for local server).

    >>> sf = SFrame(data='~/mydata/foo.csv')

    Construct an SFrame from a csv file on Amazon S3. This requires the
    environment variables: *AWS_ACCESS_KEY_ID* and *AWS_SECRET_ACCESS_KEY* to be
    set before the python session started. Alternatively, you can use
    :py:func:`graphlab.aws.set_credentials()` to set the credentials after
    python is started and :py:func:`graphlab.aws.get_credentials()` to verify
    these environment variables.

    >>> sf = SFrame(data='s3://mybucket/foo.csv')

    Read from HDFS using a specific java installation (environment variable
    only applies when using Linux)

    >>> import os
    >>> os.environ['GRAPHLAB_JAVA_HOME'] = '/my/path/to/java'
    >>> from graphlab import SFrame
    >>> sf = SFrame("hdfs://mycluster.example.com:8020/user/myname/coolfile.txt")

    An SFrame can be constructed from a dictionary of values or SArrays:

    >>> sf = gl.SFrame({'id':[1,2,3],'val':['A','B','C']})
    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C

    Or equivalently:

    >>> ids = SArray([1,2,3])
    >>> vals = SArray(['A','B','C'])
    >>> sf = SFrame({'id':ids,'val':vals})

    It can also be constructed from an array of SArrays in which case column
    names are automatically assigned.

    >>> ids = SArray([1,2,3])
    >>> vals = SArray(['A','B','C'])
    >>> sf = SFrame([ids, vals])
    >>> sf
    Columns:
        X1 int
        X2 str
    Rows: 3
    Data:
       X1  X2
    0  1   A
    1  2   B
    2  3   C

    If the SFrame is constructed from a list of values, an SFrame of a single
    column is constructed.

    >>> sf = SFrame([1,2,3])
    >>> sf
    Columns:
        X1 int
    Rows: 3
    Data:
       X1
    0  1
    1  2
    2  3

    **Parsing**

    The :py:func:`graphlab.SFrame.read_csv()` is quite powerful and, can be
    used to import a variety of row-based formats.

    First, some simple cases:

    >>> !cat ratings.csv
    user_id,movie_id,rating
    10210,1,1
    10213,2,5
    10217,2,2
    10102,1,3
    10109,3,4
    10117,5,2
    10122,2,4
    10114,1,5
    10125,1,1
    >>> gl.SFrame.read_csv('ratings.csv')
    Columns:
      user_id   int
      movie_id  int
      rating    int
    Rows: 9
    Data:
    +---------+----------+--------+
    | user_id | movie_id | rating |
    +---------+----------+--------+
    |  10210  |    1     |   1    |
    |  10213  |    2     |   5    |
    |  10217  |    2     |   2    |
    |  10102  |    1     |   3    |
    |  10109  |    3     |   4    |
    |  10117  |    5     |   2    |
    |  10122  |    2     |   4    |
    |  10114  |    1     |   5    |
    |  10125  |    1     |   1    |
    +---------+----------+--------+
    [9 rows x 3 columns]


    Delimiters can be specified, if "," is not the delimiter, for instance
    space ' ' in this case. Only single character delimiters are supported.

    >>> !cat ratings.csv
    user_id movie_id rating
    10210 1 1
    10213 2 5
    10217 2 2
    10102 1 3
    10109 3 4
    10117 5 2
    10122 2 4
    10114 1 5
    10125 1 1
    >>> gl.SFrame.read_csv('ratings.csv', delimiter=' ')

    By default, "NA" or a missing element are interpreted as missing values.

    >>> !cat ratings2.csv
    user,movie,rating
    "tom",,1
    harry,5,
    jack,2,2
    bill,,
    >>> gl.SFrame.read_csv('ratings2.csv')
    Columns:
      user  str
      movie int
      rating    int
    Rows: 4
    Data:
    +---------+-------+--------+
    |   user  | movie | rating |
    +---------+-------+--------+
    |   tom   |  None |   1    |
    |  harry  |   5   |  None  |
    |   jack  |   2   |   2    |
    | missing |  None |  None  |
    +---------+-------+--------+
    [4 rows x 3 columns]

    Furthermore due to the dictionary types and list types, can handle parsing
    of JSON-like formats.

    >>> !cat ratings3.csv
    business, categories, ratings
    "Restaurant 1", [1 4 9 10], {"funny":5, "cool":2}
    "Restaurant 2", [], {"happy":2, "sad":2}
    "Restaurant 3", [2, 11, 12], {}
    >>> gl.SFrame.read_csv('ratings3.csv')
    Columns:
    business    str
    categories  array
    ratings dict
    Rows: 3
    Data:
    +--------------+--------------------------------+-------------------------+
    |   business   |           categories           |         ratings         |
    +--------------+--------------------------------+-------------------------+
    | Restaurant 1 | array('d', [1.0, 4.0, 9.0, ... | {'funny': 5, 'cool': 2} |
    | Restaurant 2 |           array('d')           |  {'sad': 2, 'happy': 2} |
    | Restaurant 3 | array('d', [2.0, 11.0, 12.0])  |            {}           |
    +--------------+--------------------------------+-------------------------+
    [3 rows x 3 columns]

    The list and dictionary parsers are quite flexible and can absorb a
    variety of purely formatted inputs. Also, note that the list and dictionary
    types are recursive, allowing for arbitrary values to be contained.

    All these are valid lists:

    >>> !cat interesting_lists.csv
    list
    []
    [1,2,3]
    [1;2,3]
    [1 2 3]
    [{a:b}]
    ["c",d, e]
    [[a]]
    >>> gl.SFrame.read_csv('interesting_lists.csv')
    Columns:
      list  list
    Rows: 7
    Data:
    +-----------------+
    |       list      |
    +-----------------+
    |        []       |
    |    [1, 2, 3]    |
    |    [1, 2, 3]    |
    |    [1, 2, 3]    |
    |   [{'a': 'b'}]  |
    | ['c', 'd', 'e'] |
    |     [['a']]     |
    +-----------------+
    [7 rows x 1 columns]

    All these are valid dicts:

    >>> !cat interesting_dicts.csv
    dict
    {"classic":1,"dict":1}
    {space:1 seperated:1}
    {emptyvalue:}
    {}
    {:}
    {recursive1:[{a:b}]}
    {:[{:[a]}]}
    >>> gl.SFrame.read_csv('interesting_dicts.csv')
    Columns:
      dict  dict
    Rows: 7
    Data:
    +------------------------------+
    |             dict             |
    +------------------------------+
    |  {'dict': 1, 'classic': 1}   |
    | {'seperated': 1, 'space': 1} |
    |     {'emptyvalue': None}     |
    |              {}              |
    |         {None: None}         |
    | {'recursive1': [{'a': 'b'}]} |
    | {None: [{None: array('d')}]} |
    +------------------------------+
    [7 rows x 1 columns]

    **Saving**

    Save and load the sframe in native format.

    >>> sf.save('mysframedir')
    >>> sf2 = graphlab.load_sframe('mysframedir')

    **Column Manipulation**

    An SFrame is composed of a collection of columns of SArrays, and individual
    SArrays can be extracted easily. For instance given an SFrame:

    >>> sf = SFrame({'id':[1,2,3],'val':['A','B','C']})
    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C

    The "id" column can be extracted using:

    >>> sf["id"]
    dtype: int
    Rows: 3
    [1, 2, 3]

    And can be deleted using:

    >>> del sf["id"]

    Multiple columns can be selected by passing a list of column names:

    >>> sf = SFrame({'id':[1,2,3],'val':['A','B','C'],'val2':[5,6,7]})
    >>> sf
    Columns:
        id   int
        val  str
        val2 int
    Rows: 3
    Data:
       id  val val2
    0  1   A   5
    1  2   B   6
    2  3   C   7
    >>> sf2 = sf[['id','val']]
    >>> sf2
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C

    You can also select columns using types or a list of types:

    >>> sf2 = sf[int]
    >>> sf2
    Columns:
        id   int
        val2 int
    Rows: 3
    Data:
       id  val2
    0  1   5
    1  2   6
    2  3   7

    Or a mix of types and names:

    >>> sf2 = sf[['id', str]]
    >>> sf2
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C


    The same mechanism can be used to re-order columns:

    >>> sf = SFrame({'id':[1,2,3],'val':['A','B','C']})
    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C
    >>> sf[['val','id']]
    >>> sf
    Columns:
        val str
        id  int
    Rows: 3
    Data:
       val id
    0  A   1
    1  B   2
    2  C   3

    **Element Access and Slicing**

    SFrames can be accessed by integer keys just like a regular python list.
    Such operations may not be fast on large datasets so looping over an SFrame
    should be avoided.

    >>> sf = SFrame({'id':[1,2,3],'val':['A','B','C']})
    >>> sf[0]
    {'id': 1, 'val': 'A'}
    >>> sf[2]
    {'id': 3, 'val': 'C'}
    >>> sf[5]
    IndexError: SFrame index out of range

    Negative indices can be used to access elements from the tail of the array

    >>> sf[-1] # returns the last element
    {'id': 3, 'val': 'C'}
    >>> sf[-2] # returns the second to last element
    {'id': 2, 'val': 'B'}

    The SFrame also supports the full range of python slicing operators:

    >>> sf[1000:] # Returns an SFrame containing rows 1000 to the end
    >>> sf[:1000] # Returns an SFrame containing rows 0 to row 999 inclusive
    >>> sf[0:1000:2] # Returns an SFrame containing rows 0 to row 1000 in steps of 2
    >>> sf[-100:] # Returns an SFrame containing last 100 rows
    >>> sf[-100:len(sf):2] # Returns an SFrame containing last 100 rows in steps of 2

    **Logical Filter**

    An SFrame can be filtered using

    >>> sframe[binary_filter]

    where sframe is an SFrame and binary_filter is an SArray of the same length.
    The result is a new SFrame which contains only rows of the SFrame where its
    matching row in the binary_filter is non zero.

    This permits the use of boolean operators that can be used to perform
    logical filtering operations. For instance, given an SFrame

    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C

    >>> sf[(sf['id'] >= 1) & (sf['id'] <= 2)]
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B

    See :class:`~graphlab.SArray` for more details on the use of the logical
    filter.

    This can also be used more generally to provide filtering capability which
    is otherwise not expressible with simple boolean functions. For instance:

    >>> sf[sf['id'].apply(lambda x: math.log(x) <= 1)]
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B

    Or alternatively:

    >>> sf[sf.apply(lambda x: math.log(x['id']) <= 1)]

            Create an SFrame from a Python dictionary.

    >>> from graphlab import SFrame
    >>> sf = SFrame({'id':[1,2,3], 'val':['A','B','C']})
    >>> sf
    Columns:
        id  int
        val str
    Rows: 3
    Data:
       id  val
    0  1   A
    1  2   B
    2  3   C
    """

    __slots__ = ['shape', '__proxy__', '_proxy', '_cache']

    def __init__(self, data=None,
                 format='auto',
                 _proxy=None):
        """__init__(data=list(), format='auto')
        Construct a new SFrame from a url or a pandas.DataFrame.
        """
        # emit metrics for num_rows, num_columns, and type (local://, s3, hdfs, http)
        tracker = _mt._get_metric_tracker()
        if (_proxy):
            self.__proxy__ = _proxy
        else:
            self.__proxy__ = UnitySFrameProxy(glconnect.get_client())
            _format = None
            if (format == 'auto'):
                if (HAS_PANDAS and isinstance(data, pandas.DataFrame)):
                    _format = 'dataframe'
                    tracker.track('sframe.location.memory', value=1)
                elif (isinstance(data, str) or isinstance(data, unicode)):

                    if data.find('://') == -1:
                        suffix = 'local'
                    else:
                        suffix = data.split('://')[0]
                    tracker.track(('sframe.location.%s' % (suffix)), value=1)

                    if data.endswith(('.csv', '.csv.gz')):
                        _format = 'csv'
                    elif data.endswith(('.tsv', '.tsv.gz')):
                        _format = 'tsv'
                    elif data.endswith(('.txt', '.txt.gz')):
                        print "Assuming file is csv. For other delimiters, " + \
                            "please use `SFrame.read_csv`."
                        _format = 'csv'
                    else:
                        _format = 'sframe'
                elif type(data) == SArray:
                    _format = 'sarray'

                elif isinstance(data, SFrame):
                    _format = 'sframe_obj'

                elif (hasattr(data, 'iteritems')):
                    _format = 'dict'
                    tracker.track('sframe.location.memory', value=1)

                elif hasattr(data, '__iter__'):
                    _format = 'array'
                    tracker.track('sframe.location.memory', value=1)
                elif data is None:
                    _format = 'empty'
                else:
                    raise ValueError('Cannot infer input type for data ' + str(data))
            else:
                _format = format

            tracker.track(('sframe.format.%s' % _format), value=1)

            with cython_context():
                if (_format == 'dataframe'):
                    self.__proxy__.load_from_dataframe(data)
                elif (_format == 'sframe_obj'):
                    for col in data.column_names():
                        self.__proxy__.add_column(data[col].__proxy__, col)
                elif (_format == 'sarray'):
                    self.__proxy__.add_column(data.__proxy__, "")
                elif (_format == 'array'):
                    if len(data) > 0:
                        unique_types = set([type(x) for x in data if x is not None])
                        if len(unique_types) == 1 and SArray in unique_types:
                            for arr in data:
                                self.add_column(arr)
                        elif SArray in unique_types:
                            raise ValueError("Cannot create SFrame from mix of regular values and SArrays")
                        else:
                            self.__proxy__.add_column(SArray(data).__proxy__, "")
                elif (_format == 'dict'):
                    # Validate that every column is the same length.
                    if len(set(len(value) for value in data.values())) > 1:
                        # probably should be a value error. But we used to raise
                        # runtime error here...
                        raise RuntimeError("All column should be of the same length")
                    # split into SArray values and other iterable values.
                    # We convert the iterable values in bulk, and then add the sarray values as columns
                    sarray_keys = sorted(key for key,value in data.iteritems() if isinstance(value, SArray))
                    self.__proxy__.load_from_dataframe({key:value for key,value in data.iteritems() if not isinstance(value, SArray)})
                    for key in sarray_keys:
                        self.__proxy__.add_column(data[key].__proxy__, key)
                elif (_format == 'csv'):
                    url = data
                    tmpsf = SFrame.read_csv(url, delimiter=',', header=True)
                    self.__proxy__ = tmpsf.__proxy__
                elif (_format == 'tsv'):
                    url = data
                    tmpsf = SFrame.read_csv(url, delimiter='\t', header=True)
                    self.__proxy__ = tmpsf.__proxy__
                elif (_format == 'sframe'):
                    url = _make_internal_url(data)
                    self.__proxy__.load_from_sframe_index(url)
                elif (_format == 'empty'):
                    pass
                else:
                    raise ValueError('Unknown input type: ' + format)

        sframe_size = -1
        if self.__has_size__():
          sframe_size = self.num_rows()
        tracker.track('sframe.row.size', value=sframe_size)
        tracker.track('sframe.col.size', value=self.num_cols())

    @staticmethod
    def _infer_column_types_from_lines(first_rows):
        if (len(first_rows.column_names()) < 1):
          print "Insufficient number of columns to perform type inference"
          raise RuntimeError("Insufficient columns ")
        if len(first_rows) < 1:
          print "Insufficient number of rows to perform type inference"
          raise RuntimeError("Insufficient rows")
        # gets all the values column-wise
        all_column_values_transposed = [list(first_rows[col])
                for col in first_rows.column_names()]
        # transpose
        all_column_values = [list(x) for x in zip(*all_column_values_transposed)]
        all_column_type_hints = [[type(t) for t in vals] for vals in all_column_values]
        # collect the hints
        # if every line was inferred to have a different number of elements, die
        if len(set(len(x) for x in all_column_type_hints)) != 1:
            print "Unable to infer column types. Defaulting to str"
            return str

        import types

        column_type_hints = all_column_type_hints[0]
        # now perform type combining across rows
        for i in range(1, len(all_column_type_hints)):
          currow = all_column_type_hints[i]
          for j in range(len(column_type_hints)):
            # combine types
            d = set([currow[j], column_type_hints[j]])
            if (len(d) == 1):
              # easy case. both agree on the type
              continue
            if ((int in d) and (float in d)):
              # one is an int, one is a float. its a float
              column_type_hints[j] = float
            elif ((array.array in d) and (list in d)):
              # one is an array , one is a list. its a list
              column_type_hints[j] = list
            elif types.NoneType in d:
              # one is a NoneType. assign to other type
              if currow[j] != types.NoneType:
                  column_type_hints[j] = currow[j]
            else:
              column_type_hints[j] = str
        # final pass. everything whih is still NoneType is now a str
        for i in range(len(column_type_hints)):
          if column_type_hints[i] == types.NoneType:
            column_type_hints[i] = str

        return column_type_hints

    @classmethod
    def _read_csv_impl(cls,
                       url,
                       delimiter=',',
                       header=True,
                       error_bad_lines=False,
                       comment_char='',
                       escape_char='\\',
                       double_quote=True,
                       quote_char='\"',
                       skip_initial_space=True,
                       column_type_hints=None,
                       na_values=["NA"],
                       line_terminator="\n",
                       usecols=[],
                       nrows=None,
                       skiprows=0,
                       verbose=True,
                       store_errors=True,
                       **kwargs):
        """
        Constructs an SFrame from a CSV file or a path to multiple CSVs, and
        returns a pair containing the SFrame and optionally
        (if store_errors=True) a dict of filenames to SArrays
        indicating for each file, what are the incorrectly parsed lines
        encountered.

        Parameters
        ----------
        store_errors : bool
            If true, the output errors dict will be filled.

        See `read_csv` for the rest of the parameters.
        """
        # Pandas argument compatibility
        if "sep" in kwargs:
            delimiter = kwargs['sep']
            del kwargs['sep']
        if "quotechar" in kwargs:
            quote_char = kwargs['quotechar']
            del kwargs['quotechar']
        if "doublequote" in kwargs:
            double_quote = kwargs['doublequote']
            del kwargs['doublequote']
        if "comment" in kwargs:
            comment_char = kwargs['comment']
            del kwargs['comment']
            if comment_char is None:
                comment_char = ''
        if "lineterminator" in kwargs:
            line_terminator = kwargs['lineterminator']
            del kwargs['lineterminator']
        if len(kwargs) > 0:
            raise TypeError("Unexpected keyword arguments " + str(kwargs.keys()))

        parsing_config = dict()
        parsing_config["delimiter"] = delimiter
        parsing_config["use_header"] = header
        parsing_config["continue_on_failure"] = not error_bad_lines
        parsing_config["comment_char"] = comment_char
        parsing_config["escape_char"] = escape_char
        parsing_config["double_quote"] = double_quote
        parsing_config["quote_char"] = quote_char
        parsing_config["skip_initial_space"] = skip_initial_space
        parsing_config["store_errors"] = store_errors
        parsing_config["line_terminator"] = line_terminator
        parsing_config["output_columns"] = usecols
        parsing_config["skip_rows"] =skiprows

        if type(na_values) is str:
          na_values = [na_values]
        if na_values is not None and len(na_values) > 0:
            parsing_config["na_values"] = na_values

        if nrows != None:
          parsing_config["row_limit"] = nrows

        proxy = UnitySFrameProxy(glconnect.get_client())
        internal_url = _make_internal_url(url)

        # Attempt to automatically detect the column types. Either produce a
        # list of types; otherwise default to all str types.
        column_type_inference_was_used = False
        if column_type_hints is None:
            try:
                # Get the first 100 rows (using all the desired arguments).
                first_rows = SFrame.read_csv(url, nrows=100,
                                 column_type_hints=type(None),
                                 header=header,
                                 delimiter=delimiter,
                                 comment_char=comment_char,
                                 escape_char=escape_char,
                                 double_quote=double_quote,
                                 quote_char=quote_char,
                                 skip_initial_space=skip_initial_space,
                                 na_values=na_values,
                                 line_terminator=line_terminator,
                                 usecols=usecols,
                                 skiprows=skiprows,
                                 verbose=verbose)
                column_type_hints = SFrame._infer_column_types_from_lines(first_rows)
                typelist = '[' + ','.join(t.__name__ for t in column_type_hints) + ']'
                if verbose != False:
                    print "------------------------------------------------------"
                    print "Inferred types from first line of file as "
                    print "column_type_hints="+ typelist
                    print "If parsing fails due to incorrect types, you can correct"
                    print "the inferred type list above and pass it to read_csv in"
                    print "the column_type_hints argument"
                    print "------------------------------------------------------"
                column_type_inference_was_used = True
            except RuntimeError as e:
                if type(e) == RuntimeError and ("cancel" in e.message or "Cancel" in e.message):
                    raise e
                # If the above fails, default back to str for all columns.
                column_type_hints = str
                if verbose != False:
                    print 'Could not detect types. Using str for each column.'

        if type(column_type_hints) is type:
            type_hints = {'__all_columns__': column_type_hints}
        elif type(column_type_hints) is list:
            type_hints = dict(zip(['__X%d__' % i for i in range(len(column_type_hints))], column_type_hints))
        elif type(column_type_hints) is dict:
            # we need to fill in a potentially incomplete dictionary
            try:
                # Get the first 100 rows (using all the desired arguments).
                first_rows = SFrame.read_csv(url, nrows=100,
                                 column_type_hints=type(None),
                                 header=header,
                                 delimiter=delimiter,
                                 comment_char=comment_char,
                                 escape_char=escape_char,
                                 double_quote=double_quote,
                                 quote_char=quote_char,
                                 skip_initial_space=skip_initial_space,
                                 na_values=na_values,
                                 line_terminator=line_terminator,
                                 usecols=usecols,
                                 skiprows=skiprows,
                                 verbose=verbose)
                inferred_types = SFrame._infer_column_types_from_lines(first_rows)
                # make a dict of column_name to type
                inferred_types = dict(zip(first_rows.column_names(), inferred_types))
                # overwrite with the user's specified types
                for key in column_type_hints:
                    inferred_types[key] = column_type_hints[key]
                column_type_hints = inferred_types
            except RuntimeError as e:
                if type(e) == RuntimeError and ("cancel" in e.message or "Cancel" in e.message):
                    raise e
                # If the above fails, default back to str for unmatched columns
                if verbose != False:
                    print 'Could not detect types. Using str for all unspecified columns.'
            type_hints = column_type_hints
        else:
            raise TypeError("Invalid type for column_type_hints. Must be a dictionary, list or a single type.")


        _mt._get_metric_tracker().track('sframe.csv.parse')

        suffix=''
        if url.find('://') == -1:
            suffix = 'local'
        else:
            suffix = url.split('://')[0]

        _mt._get_metric_tracker().track(('sframe.location.%s' % (suffix)), value=1)
        try:
            if (not verbose):
                glconnect.get_client().set_log_progress(False)
            with cython_context():
                errors = proxy.load_from_csvs(internal_url, parsing_config, type_hints)
        except Exception as e:
            if type(e) == RuntimeError and "CSV parsing cancelled" in e.message:
                raise e
            if column_type_inference_was_used:
                # try again
                if verbose != False:
                    print "Unable to parse the file with automatic type inference."
                    print "Defaulting to column_type_hints=str"
                type_hints = {'__all_columns__': str}
                try:
                    with cython_context():
                        errors = proxy.load_from_csvs(internal_url, parsing_config, type_hints)
                except:
                    glconnect.get_client().set_log_progress(True)
                    raise
            else:
                glconnect.get_client().set_log_progress(True)
                raise

        glconnect.get_client().set_log_progress(True)

        return (cls(_proxy=proxy), { f: SArray(_proxy = es) for (f, es) in errors.iteritems() })

    @classmethod
    def read_csv_with_errors(cls,
                             url,
                             delimiter=',',
                             header=True,
                             comment_char='',
                             escape_char='\\',
                             double_quote=True,
                             quote_char='\"',
                             skip_initial_space=True,
                             column_type_hints=None,
                             na_values=["NA"],
                             line_terminator='\n',
                             usecols = [],
                             nrows=None,
                             skiprows=0,
                             verbose=True,
                             **kwargs):
        """
        Constructs an SFrame from a CSV file or a path to multiple CSVs, and
        returns a pair containing the SFrame and a dict of filenames to SArrays
        indicating for each file, what are the incorrectly parsed lines
        encountered.

        Parameters
        ----------
        url : string
            Location of the CSV file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        delimiter : string, optional
            This describes the delimiter used for parsing csv files.

        header : bool, optional
            If true, uses the first row as the column names. Otherwise use the
            default column names: 'X1, X2, ...'.

        comment_char : string, optional
            The character which denotes that the
            remainder of the line is a comment.

        escape_char : string, optional
            Character which begins a C escape sequence

        double_quote : bool, optional
            If True, two consecutive quotes in a string are parsed to a single
            quote.

        quote_char : string, optional
            Character sequence that indicates a quote.

        skip_initial_space : bool, optional
            Ignore extra spaces at the start of a field

        column_type_hints : None, type, list[type], dict[string, type], optional
            This provides type hints for each column. By default, this method
            attempts to detect the type of each column automatically.

            Supported types are int, float, str, list, dict, and array.array.

            * If a single type is provided, the type will be
              applied to all columns. For instance, column_type_hints=float
              will force all columns to be parsed as float.
            * If a list of types is provided, the types applies
              to each column in order, e.g.[int, float, str]
              will parse the first column as int, second as float and third as
              string.
            * If a dictionary of column name to type is provided,
              each type value in the dictionary is applied to the key it
              belongs to.
              For instance {'user':int} will hint that the column called "user"
              should be parsed as an integer, and the rest will be type inferred.

        na_values : str | list of str, optional
            A string or list of strings to be interpreted as missing values.

        line_terminator : str, optional
            A string to be interpreted as the line terminator. Defaults to "\n"
            which will also correctly match Mac, Linux and Windows line endings
            ("\r", "\n" and "\r\n" respectively)

        usecols : list of str, optional
            A subset of column names to output. If unspecified (default),
            all columns will be read. This can provide performance gains if the
            number of columns are large. If the input file has no headers,
            usecols=['X1','X3'] will read columns 1 and 3.

        nrows : int, optional
            If set, only this many rows will be read from the file.

        skiprows : int, optional
            If set, this number of rows at the start of the file are skipped.

        verbose : bool, optional
            If True, print the progress.

        Returns
        -------
        out : tuple
            The first element is the SFrame with good data. The second element
            is a dictionary of filenames to SArrays indicating for each file,
            what are the incorrectly parsed lines encountered.

        See Also
        --------
        read_csv, SFrame

        Examples
        --------
        >>> bad_url = 'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv'
        >>> (sf, bad_lines) = graphlab.SFrame.read_csv_with_errors(bad_url)
        >>> sf
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [98 rows x 3 columns]

        >>> bad_lines
        {'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv': dtype: str
         Rows: 1
         ['x,y,z,a,b,c']}
       """
        return cls._read_csv_impl(url,
                                  delimiter=delimiter,
                                  header=header,
                                  error_bad_lines=False, # we are storing errors,
                                                         # thus we must not fail
                                                         # on bad lines
                                  comment_char=comment_char,
                                  escape_char=escape_char,
                                  double_quote=double_quote,
                                  quote_char=quote_char,
                                  skip_initial_space=skip_initial_space,
                                  column_type_hints=column_type_hints,
                                  na_values=na_values,
                                  line_terminator=line_terminator,
                                  usecols=usecols,
                                  nrows=nrows,
                                  verbose=verbose,
                                  skiprows=skiprows,
                                  store_errors=True,
                                  **kwargs)
    @classmethod
    def read_csv(cls,
                 url,
                 delimiter=',',
                 header=True,
                 error_bad_lines=False,
                 comment_char='',
                 escape_char='\\',
                 double_quote=True,
                 quote_char='\"',
                 skip_initial_space=True,
                 column_type_hints=None,
                 na_values=["NA"],
                 line_terminator='\n',
                 usecols=[],
                 nrows=None,
                 skiprows=0,
                 verbose=True,
                 **kwargs):
        """
        Constructs an SFrame from a CSV file or a path to multiple CSVs.

        Parameters
        ----------
        url : string
            Location of the CSV file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        delimiter : string, optional
            This describes the delimiter used for parsing csv files.

        header : bool, optional
            If true, uses the first row as the column names. Otherwise use the
            default column names : 'X1, X2, ...'.

        error_bad_lines : bool
            If true, will fail upon encountering a bad line. If false, will
            continue parsing skipping lines which fail to parse correctly.
            A sample of the first 10 encountered bad lines will be printed.

        comment_char : string, optional
            The character which denotes that the remainder of the line is a
            comment.

        escape_char : string, optional
            Character which begins a C escape sequence

        double_quote : bool, optional
            If True, two consecutive quotes in a string are parsed to a single
            quote.

        quote_char : string, optional
            Character sequence that indicates a quote.

        skip_initial_space : bool, optional
            Ignore extra spaces at the start of a field

        column_type_hints : None, type, list[type], dict[string, type], optional
            This provides type hints for each column. By default, this method
            attempts to detect the type of each column automatically.

            Supported types are int, float, str, list, dict, and array.array.

            * If a single type is provided, the type will be
              applied to all columns. For instance, column_type_hints=float
              will force all columns to be parsed as float.
            * If a list of types is provided, the types applies
              to each column in order, e.g.[int, float, str]
              will parse the first column as int, second as float and third as
              string.
            * If a dictionary of column name to type is provided,
              each type value in the dictionary is applied to the key it
              belongs to.
              For instance {'user':int} will hint that the column called "user"
              should be parsed as an integer, and the rest will be type inferred.

        na_values : str | list of str, optional
            A string or list of strings to be interpreted as missing values.

        line_terminator : str, optional
            A string to be interpreted as the line terminator. Defaults to "\n"
            which will also correctly match Mac, Linux and Windows line endings
            ("\r", "\n" and "\r\n" respectively)

        usecols : list of str, optional
            A subset of column names to output. If unspecified (default),
            all columns will be read. This can provide performance gains if the
            number of columns are large. If the input file has no headers,
            usecols=['X1','X3'] will read columns 1 and 3.

        nrows : int, optional
            If set, only this many rows will be read from the file.

        skiprows : int, optional
            If set, this number of rows at the start of the file are skipped.

        verbose : bool, optional
            If True, print the progress.

        Returns
        -------
        out : SFrame

        See Also
        --------
        read_csv_with_errors, SFrame

        Examples
        --------

        Read a regular csv file, with all default options, automatically
        determine types:

        >>> url = 'http://s3.amazonaws.com/gl-testdata/rating_data_example.csv'
        >>> sf = graphlab.SFrame.read_csv(url)
        >>> sf
        Columns:
          user_id int
          movie_id  int
          rating  int
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Read only the first 100 lines of the csv file:

        >>> sf = graphlab.SFrame.read_csv(url, nrows=100)
        >>> sf
        Columns:
          user_id int
          movie_id  int
          rating  int
        Rows: 100
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [100 rows x 3 columns]

        Read all columns as str type

        >>> sf = graphlab.SFrame.read_csv(url, column_type_hints=str)
        >>> sf
        Columns:
          user_id  str
          movie_id  str
          rating  str
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Specify types for a subset of columns and leave the rest to be str.

        >>> sf = graphlab.SFrame.read_csv(url,
        ...                               column_type_hints={
        ...                               'user_id':int, 'rating':float
        ...                               })
        >>> sf
        Columns:
          user_id str
          movie_id  str
          rating  float
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |  3.0   |
        |  25907  |   1663   |  3.0   |
        |  25923  |   1663   |  3.0   |
        |  25924  |   1663   |  3.0   |
        |  25928  |   1663   |  2.0   |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Not treat first line as header:

        >>> sf = graphlab.SFrame.read_csv(url, header=False)
        >>> sf
        Columns:
          X1  str
          X2  str
          X3  str
        Rows: 10001
        +---------+----------+--------+
        |    X1   |    X2    |   X3   |
        +---------+----------+--------+
        | user_id | movie_id | rating |
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10001 rows x 3 columns]

        Treat '3' as missing value:

        >>> sf = graphlab.SFrame.read_csv(url, na_values=['3'], column_type_hints=str)
        >>> sf
        Columns:
          user_id str
          movie_id  str
          rating  str
        Rows: 10000
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |  None  |
        |  25907  |   1663   |  None  |
        |  25923  |   1663   |  None  |
        |  25924  |   1663   |  None  |
        |  25928  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Throw error on parse failure:

        >>> bad_url = 'https://s3.amazonaws.com/gl-testdata/bad_csv_example.csv'
        >>> sf = graphlab.SFrame.read_csv(bad_url, error_bad_lines=True)
        RuntimeError: Runtime Exception. Unable to parse line "x,y,z,a,b,c"
        Set error_bad_lines=False to skip bad lines
        """

        return cls._read_csv_impl(url,
                                  delimiter=delimiter,
                                  header=header,
                                  error_bad_lines=error_bad_lines,
                                  comment_char=comment_char,
                                  escape_char=escape_char,
                                  double_quote=double_quote,
                                  quote_char=quote_char,
                                  skip_initial_space=skip_initial_space,
                                  column_type_hints=column_type_hints,
                                  na_values=na_values,
                                  line_terminator=line_terminator,
                                  usecols=usecols,
                                  nrows=nrows,
                                  skiprows=skiprows,
                                  verbose=verbose,
                                  store_errors=False,
                                  **kwargs)[0]


    @classmethod
    def read_json(cls,
                 url,
                 orient='records'):
        """
        Reads a JSON file representing a table into an SFrame.

        Parameters
        ----------
        url : string
            Location of the CSV file or directory to load. If URL is a directory
            or a "glob" pattern, all matching files will be loaded.

        orient : string, optional. Either "records" or "lines"
            If orient="records" the file is expected to contain a single JSON
            array, where each array element is a dictionary. If orient="lines",
            the file is expected to contain a JSON element per line.

        Examples
        --------
        The orient parameter describes the expected input format of the JSON
        file.

        If orient="records", the JSON file is expected to contain a single
        JSON Array where each array element is a dictionary describing the row.
        For instance:

        >>> !cat input.json
        [{'a':1,'b':1}, {'a':2,'b':2}, {'a':3,'b':3}]
        >>> SFrame.read_json('input.json', orient='records')
        Columns:
                a	int
                b	int
        Rows: 3
        Data:
        +---+---+
        | a | b |
        +---+---+
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
        +---+---+

        If orient="lines", the JSON file is expected to contain a JSON element
        per line. If each line contains a dictionary, it is automatically
        unpacked.

        >>> !cat input.json
        {'a':1,'b':1}
        {'a':2,'b':2}
        {'a':3,'b':3}
        >>> g = SFrame.read_json('input.json', orient='lines')
        Columns:
                a	int
                b	int
        Rows: 3
        Data:
        +---+---+
        | a | b |
        +---+---+
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
        +---+---+

        If the lines are not dictionaries, the original format is maintained.

        >>> !cat input.json
        ['a','b','c']
        ['d','e','f']
        ['g','h','i']
        [1,2,3]
        >>> g = SFrame.read_json('input.json', orient='lines')
        Columns:
                X1	list
        Rows: 3
        Data:
        +-----------+
        |     X1    |
        +-----------+
        | [a, b, c] |
        | [d, e, f] |
        | [g, h, i] |
        +-----------+
        [3 rows x 1 columns]
        """
        if orient == "records":
            g = cls.read_csv(url, line_terminator='',
                    header=False, column_type_hints=list)
            if g.num_cols() != 1:
                raise RuntimeError("Input JSON not of expected format")
            return g.stack('X1').unpack('X1','')
        elif orient == "lines":
            g = cls.read_csv(url, header=False)
            if g.num_cols() != 1:
                raise RuntimeError("Input JSON not of expected format")
            if g['X1'].dtype() == dict:
                return g.unpack('X1','')
            else:
                return g
        else:
            raise ValueError("Invalid value for orient parameter (" + str(orient) + ")")

    @classmethod
    def __get_graphlabutil_reference_on_spark_unity_jar(cls,sc):
        '''
        A utility function to get a handle to GraphLabUtil object in jar file.
        This utility function dynamically loads the GraphLabUtil class and call
        factory method ``getUtil()`` that returns a GraphLabUtil object.

        Parameters
        ----------
        sc : SparkContext
            Current SparkContext.
        '''
        global SFRAME_GRAPHLABUTIL_REF

        if SFRAME_GRAPHLABUTIL_REF is None:
            jar_path = get_spark_integration_jar_path()
            jar_file = sc._jvm.java.io.File(jar_path)
            jar_url = jar_file.toURL()
            url_array = sc._gateway.new_array(sc._jvm.java.net.URL,1)
            url_array[0] = jar_url
            child = sc._jvm.java.net.URLClassLoader(url_array)
            load_class = child.loadClass("org.graphlab.create.GraphLabUtil")
            method = load_class.getDeclaredMethod("getUtil",None)
            SFRAME_GRAPHLABUTIL_REF = method.invoke(load_class,None)

        return SFRAME_GRAPHLABUTIL_REF

    def to_spark_dataframe(self,sc,sql,number_of_partitions=4):
        """
        Convert the current SFrame to the Spark DataFrame.

        Parameters
        ----------
        sc : SparkContext
            sc is an existing SparkContext.

        sql : SQLContext
            sql is an existing SQLContext.

        number_of_partitions : int
            number of partitions for the output rdd

        Returns
        ----------
        out: pyspark.sql.DataFrame

        Notes
        -----
        - Look at to_rdd()
        - Look at from_rdd()

        Examples
        --------

        >>> from pyspark import SparkContext, SQLContext
        >>> from graphlab import SFrame
        >>> from pyspark.sql import SQLContext
        >>> sc = SparkContext('local')
        >>> sql = SQLContext(sc)
        >>> sf = SFrame({'x': [1,2,3], 'y': ['fish', 'chips', 'salad']})
        >>> df = sf.to_spark_dataframe(sc, sql)
        >>> df.show()
        x y
        1 fish
        2 chips
        3 salad
        """

        def homogeneous_type(seq):
            if seq is None or len(seq) == 0:
                return True
            iseq = iter(seq)
            first_type = type(next(iseq))
            return True if all( (type(x) is first_type) for x in iseq ) else False

        if len(self) == 0:
            raise ValueError("SFrame is empty")

        column_names = self.column_names()
        first_row = self.head(1)[0]

        for name in column_names:
            if hasattr(first_row[name],'__iter__') and homogeneous_type(first_row[name]) is not True:
                raise TypeError("Support for translation to Spark DataFrame not enabled for heterogeneous iterable type (column: %s). Use SFrame.to_rdd()." % name)

        rdd = self.to_rdd(sc,number_of_partitions);
        from pyspark.sql import Row
        rowRdd = rdd.map(lambda x: Row(**x))
        if(hasattr(sql,'createDataFrame')):
            return sql.createDataFrame(rowRdd)
        else:
            return sql.inferSchema(rowRdd)

    def to_rdd(self, sc, number_of_partitions=4):
        """
        Convert the current SFrame to the Spark RDD.

        Parameters
        ----------
        sc : SparkContext
            sc is an existing SparkContext.

        number_of_partitions: int
            number of partitions for the output rdd

        Returns
        ----------
        out: pyspark.rdd.RDD

        Notes
        ----------
        - Look at from_rdd().
        - Look at to_spark_dataframe().

        Examples
        --------

        >>> from pyspark import SparkContext
        >>> from graphlab import SFrame
        >>> sc = SparkContext('local')
        >>> sf = SFrame({'x': [1,2,3], 'y': ['fish', 'chips', 'salad']})
        >>> rdd = sf.to_rdd(sc)
        >>> rdd.collect()
        [{'x': 1L, 'y': 'fish'}, {'x': 2L, 'y': 'chips'}, {'x': 3L, 'y': 'salad'}]
        """
        _mt._get_metric_tracker().track('sframe.to_rdd')
        if not RDD_SUPPORT:
            raise Exception("Support for translation to Spark RDDs not enabled.")

        for _type in self.column_types():
                if(_type.__name__ == 'Image'):
                    raise TypeError("Support for translation to Spark RDDs not enabled for Image type.")

        if number_of_partitions is None:
            number_of_partitions = sc.defaultParallelism

        if type(number_of_partitions) is not int:
            raise ValueError("number_of_partitions parameter expects an integer type")
        if number_of_partitions == 0:
            raise ValueError("number_of_partitions can not be initialized to zero")

        # get a handle to GraphLabUtil java object
        graphlab_util_ref = self.__get_graphlabutil_reference_on_spark_unity_jar(sc)

        # Save SFrame in a temporary place
        tmp_loc = self.__get_staging_dir__(sc,graphlab_util_ref)
        sf_loc = os.path.join(tmp_loc, str(uuid.uuid4()))
        self.save(sf_loc)
        print sf_loc
        # Keep track of the temporary sframe that is saved(). We need to delete it eventually.
        dummysf = load_sframe(sf_loc)
        dummysf.__proxy__.delete_on_close()
        SFRAME_GARBAGE_COLLECTOR.append(dummysf)

        # Get the environment variables as a java map
        # env  = sys_util.make_unity_server_env()
        # java_env_map = MapConverter().convert(env, sc._gateway._gateway_client)


        # Run the spark job
        javaRDD = graphlab_util_ref.pySparkToRDD(
            sc._jsc.sc(),sf_loc,number_of_partitions,"")

        from pyspark import RDD
        import pyspark
        output_rdd = RDD(javaRDD,sc,pyspark.serializers.PickleSerializer())

        return output_rdd


    @classmethod
    def __get_staging_dir__(cls,cur_sc,graphlab_util_ref):
        if not RDD_SUPPORT_INITED:
            __rdd_support_init__(cur_sc,graphlab_util_ref)


        return STAGING_DIR

    @classmethod
    def from_rdd(cls, rdd, cur_sc):
        """
        Convert a Spark RDD into an SFrame.

        Parameters
        ----------
        rdd : pyspark.rdd.RDD or pyspark.sql.DataFrame
            The input Spark RDD that is going to be converted to an SFrame.
        cur_sc : SparkContext
            An instance object of an SparkContext.

        Returns
        -------
        out : SFrame

        Notes
        ----------
        - look at to_rdd().
        - look at to_spark_dataframe().

        Examples
        --------

        >>> from pyspark import SparkContext
        >>> from graphlab import SFrame
        >>> sc = SparkContext('local')
        >>> rdd = sc.parallelize([1,2,3])
        >>> sf = SFrame.from_rdd(rdd, sc)
        >>> sf
        Data:
        +-----+
        |  X1 |
        +-----+
        | 1.0 |
        | 2.0 |
        | 3.0 |
        +-----+
        [3 rows x 1 columns]
        """
        _mt._get_metric_tracker().track('sframe.from_rdd')
        if not RDD_SUPPORT:
            raise Exception("Support for translation to Spark RDDs not enabled.")

        jrdd_deserializer = None
        from pyspark import RDD
        from pyspark.sql import DataFrame

        if isinstance(rdd,DataFrame):
            jrdd_deserializer = rdd.rdd._jrdd_deserializer
        elif isinstance(rdd,RDD):
            jrdd_deserializer = rdd._jrdd_deserializer
        else:
            raise RuntimeError("RDD type " + rdd.__class__.__name__ + " is currently unsupported.")

        # Determine the encoding of the RDD
        encoding = "batch" # default encoding in spark is batch
        if(jrdd_deserializer.__class__.__name__ == 'PickleSerializer'):
            encoding = "pickle"
        elif(jrdd_deserializer.__class__.__name__ == 'UTF8Deserializer'):
            encoding = "utf8"

        # get a handle to GraphLabUtil java object
        graphlab_util_ref = SFrame.__get_graphlabutil_reference_on_spark_unity_jar(cur_sc)

        # Prep the sframe environment
        tmp_loc = SFrame.__get_staging_dir__(cur_sc,graphlab_util_ref)
        if tmp_loc is None:
            raise RuntimeError("Could not determine staging directory for SFrame files.")

        # directory to stage the SFrame
        outputDir = tmp_loc
        # prefix for the final SFrame name.  This would normally be a meaningful name
        # however since this sframe will immediately be opened we use a random name
        finalSFramePrefix = str(uuid.uuid4())

        # run the spark job which returns the filename of the final sframe index
        if isinstance(rdd,DataFrame):
            # if its a dataframe use the special java code path.
            df = rdd._jdf
            finalSFrameFilename = graphlab_util_ref.toSFrame(
                df, tmp_loc, finalSFramePrefix)
        else:
            if encoding == 'utf8':
                ## TODO: This is a temporary solution. Here we are completely bypassing 
                ## toSFrame() codepath when encoding is 'utf8'. This is because of Spark1.5 error
                ## for closure cleaning issue on deep nested functions.

                def f(iterator): 
                    for obj in iterator:
                        yield obj.encode("utf-8")

                rdd = rdd.mapPartitions(f)
                encoding = "batch"
                if(rdd._jrdd_deserializer.__class__.__name__ == 'PickleSerializer'):
                    encoding = "pickle"
                
                #finalSFrameFilename = graphlab_util_ref.toSFrame(
                #    rdd._jrdd.rdd(),tmp_loc, finalSFramePrefix)
            #else:
            # Prep the additional arguments to feed into the pySparkToSFrame function in Java
            # that will call the spark_unity binary which does the actual encoding
            additiona_args = os.path.join(" --encoding=%s " % encoding +\
                                " --type=rdd ")
            finalSFrameFilename = graphlab_util_ref.pySparkToSFrame(
                rdd._jrdd, tmp_loc, finalSFramePrefix, additiona_args)

        # Load and return the sframe
        sf = SFrame()
        sf.__proxy__.load_from_sframe_index(_make_internal_url(finalSFrameFilename))
        return sf

    @classmethod
    def from_odbc(cls, db, sql, verbose=False):
        """
        Convert a table or query from a database to an SFrame.

        This function does not do any checking on the given SQL query, and
        cannot know what effect it will have on the database.  Any side effects
        from the query will be reflected on the database.  If no result
        rows are returned, an empty SFrame is created.

        Keep in mind the default case your database stores table names in.  In
        some cases, you may need to add quotation marks (or whatever character
        your database uses to quote identifiers), especially if you created the
        table using `to_odbc`.

        Parameters
        ----------
        db :  `graphlab.extensions._odbc_connection.unity_odbc_connection`
          An ODBC connection object.  This can only be obtained by calling
          `graphlab.connect_odbc`.  Check that documentation for how to create
          this object.

        sql : str
          A SQL query.  The query must be acceptable by the ODBC driver used by
          `graphlab.extensions._odbc_connection.unity_odbc_connection`.

        Returns
        -------
        out : SFrame

        Notes
        -----
        This functionality is only supported when using GraphLab Create
        entirely on your local machine.  Therefore, GraphLab Create's EC2 and
        Hadoop execution modes will not be able to use ODBC.  Note that this
        does not apply to the machine your database is running, which can (and
        often will) be running on a separate machine.

        Examples
        --------
        >>> db = graphlab.connect_odbc("DSN=my_awesome_dsn;UID=user;PWD=mypassword")

        >>> a_table = graphlab.SFrame.from_odbc(db, "SELECT * FROM a_table")

        >>> join_result = graphlab.SFrame.from_odbc(db, 'SELECT * FROM "MyTable" a, "AnotherTable" b WHERE a.id=b.id')
        """
        result = db.execute_query(sql)
        if not isinstance(result, SFrame):
            raise RuntimeError("Cannot create an SFrame for query. No result set.")
        cls = result
        return cls

    def to_odbc(self, db, table_name, append_if_exists=False, verbose=True):
        """
        Convert an SFrame to a table in a database.

        By default, searches for a table in the database with the given name.
        If found, this will attempt to append all the rows of the SFrame to the
        end of the table.  If not, this will create a new table with the given
        name.  This behavior is toggled with the `append_if_exists` flag.

        When creating a new table, GraphLab Create uses a heuristic approach to
        pick a corresponding type for each column in the SFrame using the type
        information supplied by the database's ODBC driver.  Your driver must
        support giving this type information for GraphLab Create to support
        writing to the database.

        To allow more expressive and accurate naming, `to_odbc` puts quotes
        around each identifier (table names and column names).  Depending on
        your database, you may need to refer to the created table with quote
        characters around the name.  This character is not the same for all
        databases, but '"' is the most common.

        Parameters
        ----------
        db :  `graphlab.extensions._odbc_connection.unity_odbc_connection`
          An ODBC connection object.  This can only be obtained by calling
          `graphlab.connect_odbc`.  Check that documentation for how to create
          this object.

        table_name : str
          The name of the table you would like to create/append to.

        append_if_exists : bool
          If True, this will attempt to append to the table named `table_name`
          if it is found to exist in the database.

        verbose : bool
          Print progress updates on the insertion process.

        Notes
        -----
        This functionality is only supported when using GraphLab Create
        entirely on your local machine.  Therefore, GraphLab Create's EC2 and
        Hadoop execution modes will not be able to use ODBC.  Note that this
        "local machine" rule does not apply to the machine your database is
        running on, which can (and often will) be running on a separate
        machine.

        Examples
        --------
        >>> db = graphlab.connect_odbc("DSN=my_awesome_dsn;UID=user;PWD=mypassword")

        >>> sf = graphlab.SFrame({'a':[1,2,3],'b':['hi','pika','bye']})

        >>> sf.to_odbc(db, 'a_cool_table')
        """
        if (not verbose):
            glconnect.get_client().set_log_progress(False)

        db._insert_sframe(self, table_name, append_if_exists)

        if (not verbose):
            glconnect.get_client().set_log_progress(True)

    def __repr__(self):
        """
        Returns a string description of the frame
        """
        ret = self.__get_column_description__()
        (is_empty, data_str) = self.__str_impl__()
        if is_empty:
            data_str = "\t[]"

        if self.__has_size__():
            ret = ret + "Rows: " + str(len(self)) + "\n\n"
        else:
            ret = ret + "Rows: Unknown" + "\n\n"

        ret = ret + "Data:\n"
        ret = ret + data_str
        return ret

    def __get_column_description__(self):
        colnames = self.column_names()
        coltypes = self.column_types()
        ret = "Columns:\n"
        if len(colnames) > 0:
            for i in range(len(colnames)):
                ret = ret + "\t" + colnames[i] + "\t" + coltypes[i].__name__ + "\n"
            ret = ret + "\n"
        else:
            ret = ret + "\tNone\n\n"
        return ret

    def __get_pretty_tables__(self, wrap_text=False, max_row_width=80,
                              max_column_width=30, max_columns=20,
                              max_rows_to_display=60):
        """
        Returns a list of pretty print tables representing the current SFrame.
        If the number of columns is larger than max_columns, the last pretty
        table will contain an extra column of "...".
        Parameters
        ----------
        wrap_text : bool, optional
        max_row_width : int, optional
            Max number of characters per table.
        max_column_width : int, optional
            Max number of characters per column.
        max_columns : int, optional
            Max number of columns per table.
        max_rows_to_display : int, optional
            Max number of rows to display.
        Returns
        -------
        out : list[PrettyTable]
        """
        if (len(self) <= max_rows_to_display):
            headsf = self.__copy__()
        else:
            headsf = self.head(max_rows_to_display)

        if headsf.shape == (0, 0):
            return [PrettyTable()]

        # convert array.array column to list column so they print like [...]
        # and not array('d', ...)
        for col in headsf.column_names():
            if headsf[col].dtype() is array.array:
                headsf[col] = headsf[col].astype(list)

        def _value_to_str(value):
            if (type(value) is array.array):
                return str(list(value))
            elif (type(value) is list):
                return '[' + ", ".join(_value_to_str(x) for x in value) + ']'
            else:
                return str(value)

        def _escape_space(s):
            return "".join([ch.encode('string_escape') if ch.isspace() else ch for ch in s])

        def _truncate_respect_unicode(s, max_length):
            if (len(s) <= max_length):
                return s
            else:
                u = unicode(s, 'utf-8', errors='replace')
                return u[:max_length].encode('utf-8')

        def _truncate_str(s, wrap_str=False):
            """
            Truncate and optionally wrap the input string as unicode, replace
            unconvertible character with a diamond ?.
            """
            s = _escape_space(s)

            if len(s) <= max_column_width:
                return unicode(s, 'utf-8', errors='replace')
            else:
                ret = ''
                # if wrap_str is true, wrap the text and take at most 2 rows
                if wrap_str:
                    wrapped_lines = wrap(s, max_column_width)
                    if len(wrapped_lines) == 1:
                        return wrapped_lines[0]
                    last_line = wrapped_lines[1]
                    if len(last_line) >= max_column_width:
                        last_line = _truncate_respect_unicode(last_line, max_column_width - 4)
                    ret = wrapped_lines[0] + '\n' + last_line + ' ...'
                else:
                    ret = _truncate_respect_unicode(s, max_column_width - 4) + '...'
                return unicode(ret, 'utf-8', errors='replace')

        columns = self.column_names()[:max_columns]
        columns.reverse()  # reverse the order of columns and we will pop from the end

        num_column_of_last_table = 0
        row_of_tables = []
        # let's build a list of tables with max_columns
        # each table should satisfy, max_row_width, and max_column_width
        while len(columns) > 0:
            tbl = PrettyTable()
            table_width = 0
            num_column_of_last_table = 0
            while len(columns) > 0:
                col = columns.pop()
                # check the max length of element in the column
                if len(headsf) > 0:
                    col_width = min(max_column_width, max(len(str(x)) for x in headsf[col]))
                else:
                    col_width = max_column_width
                if (table_width + col_width < max_row_width):
                    # truncate the header if necessary
                    header = _truncate_str(col, wrap_text)
                    tbl.add_column(header, [_truncate_str(_value_to_str(x), wrap_text) for x in headsf[col]])
                    table_width = str(tbl).find('\n')
                    num_column_of_last_table += 1
                else:
                    # the column does not fit in the current table, push it back to columns
                    columns.append(col)
                    break
            tbl.align = 'c'
            row_of_tables.append(tbl)

        # add a column of all "..." if there are more columns than displayed
        if self.num_cols() > max_columns:
            row_of_tables[-1].add_column('...', ['...'] * len(headsf))
            num_column_of_last_table += 1

        # add a row of all "..." if there are more rows than displayed
        if self.__has_size__() and self.num_rows() > headsf.num_rows():
            row_of_tables[-1].add_row(['...'] * num_column_of_last_table)
        return row_of_tables

    def print_rows(self, num_rows=10, num_columns=40, max_column_width=30,
                   max_row_width=80, output_file=sys.stdout):
        """
        Print the first M rows and N columns of the SFrame in human readable
        format.

        Parameters
        ----------
        num_rows : int, optional
            Number of rows to print.

        num_columns : int, optional
            Number of columns to print.

        max_column_width : int, optional
            Maximum width of a column. Columns use fewer characters if possible.

        max_row_width : int, optional
            Maximum width of a printed row. Columns beyond this width wrap to a
            new line. `max_row_width` is automatically reset to be the
            larger of itself and `max_column_width`.

        See Also
        --------
        head, tail
        """

        max_row_width = max(max_row_width, max_column_width + 1)

        printed_sf = self._imagecols_to_stringcols(num_rows)
        row_of_tables = printed_sf.__get_pretty_tables__(wrap_text=False,
                                                         max_rows_to_display=num_rows,
                                                         max_columns=num_columns,
                                                         max_column_width=max_column_width,
                                                         max_row_width=max_row_width)
        footer = "[%d rows x %d columns]\n" % self.shape
        print >> output_file, '\n'.join([str(tb) for tb in row_of_tables]) + "\n" + footer

    def _imagecols_to_stringcols(self, num_rows=10):
        # A list of column types
        types = self.column_types()
        # A list of indexable column names
        names = self.column_names()

        # Constructing names of sframe columns that are of image type
        image_column_names = [names[i] for i in range(len(names)) if types[i] == _Image]

        #If there are image-type columns, copy the SFrame and cast the top MAX_NUM_ROWS_TO_DISPLAY of those columns to string
        printed_sf = self.__copy__()
        if len(image_column_names) > 0:
            for t in names:
                if t in image_column_names:
                    printed_sf[t] = self[t].astype(str)
        return printed_sf.head(num_rows)

    def __str_impl__(self, num_rows=10, footer=True):
        """
        Returns a string containing the first 10 elements of the frame, along
        with a description of the frame.
        """
        MAX_ROWS_TO_DISPLAY = num_rows

        printed_sf = self._imagecols_to_stringcols(MAX_ROWS_TO_DISPLAY)

        row_of_tables = printed_sf.__get_pretty_tables__(wrap_text=False, max_rows_to_display=MAX_ROWS_TO_DISPLAY)
        is_empty = len(printed_sf) == 0

        if (not footer):
            return (is_empty, '\n'.join([str(tb) for tb in row_of_tables]))

        if self.__has_size__():
            footer = '[%d rows x %d columns]\n' % self.shape
            if (self.num_rows() > MAX_ROWS_TO_DISPLAY):
                footer += '\n'.join(FOOTER_STRS)
        else:
            footer = '[? rows x %d columns]\n' % self.num_columns()
            footer += '\n'.join(LAZY_FOOTER_STRS)
        return (is_empty, '\n'.join([str(tb) for tb in row_of_tables]) + "\n" + footer)

    def __str__(self, num_rows=10, footer=True):
        """
        Returns a string containing the first 10 elements of the frame, along
        with a description of the frame.
        """
        return self.__str_impl__(num_rows, footer)[1]

    def _repr_html_(self):
        MAX_ROWS_TO_DISPLAY = 10

        printed_sf = self._imagecols_to_stringcols(MAX_ROWS_TO_DISPLAY)

        row_of_tables = printed_sf.__get_pretty_tables__(wrap_text=True,
                                                         max_row_width=120,
                                                         max_columns=40,
                                                         max_column_width=25,
                                                         max_rows_to_display=MAX_ROWS_TO_DISPLAY)
        if self.__has_size__():
            footer = '[%d rows x %d columns]<br/>' % self.shape
            if (self.num_rows() > MAX_ROWS_TO_DISPLAY):
                footer += '<br/>'.join(FOOTER_STRS)
        else:
            footer = '[? rows x %d columns]<br/>' % self.num_columns()
            footer += '<br/>'.join(LAZY_FOOTER_STRS)
        begin = '<div style="max-height:1000px;max-width:1500px;overflow:auto;">'
        end = '\n</div>'
        return begin + '\n'.join([tb.get_html_string(format=True) for tb in row_of_tables]) + "\n" + footer + end

    def __nonzero__(self):
        """
        Returns true if the frame is not empty.
        """
        return self.num_rows() != 0

    def __len__(self):
        """
        Returns the number of rows of the sframe.
        """
        return self.num_rows()

    def __copy__(self):
        """
        Returns a shallow copy of the sframe.
        """
        return self.select_columns(self.column_names())

    def copy(self):
        """
        Returns a shallow copy of the sframe.
        """
        return self.__copy__()

    def __eq__(self, other):
        raise NotImplementedError

    def __ne__(self, other):
        raise NotImplementedError

    def _row_selector(self, other):
        """
        Where other is an SArray of identical length as the current Frame,
        this returns a selection of a subset of rows in the current SFrame
        where the corresponding row in the selector is non-zero.
        """
        if type(other) is SArray:
            if len(other) != len(self):
                raise IndexError("Cannot perform logical indexing on arrays of different length.")
            with cython_context():
                return SFrame(_proxy=self.__proxy__.logical_filter(other.__proxy__))

    def dtype(self):
        """
        The type of each column.

        Returns
        -------
        out : list[type]
            Column types of the SFrame.

        See Also
        --------
        column_types
        """
        return self.column_types()

    def num_rows(self):
        """
        The number of rows in this SFrame.

        Returns
        -------
        out : int
            Number of rows in the SFrame.

        See Also
        --------
        num_columns
        """
        return self.__proxy__.num_rows()

    def num_cols(self):
        """
        The number of columns in this SFrame.

        Returns
        -------
        out : int
            Number of columns in the SFrame.

        See Also
        --------
        num_columns, num_rows
        """
        return self.__proxy__.num_columns()

    def num_columns(self):
        """
        The number of columns in this SFrame.

        Returns
        -------
        out : int
            Number of columns in the SFrame.

        See Also
        --------
        num_cols, num_rows
        """
        return self.__proxy__.num_columns()

    def column_names(self):
        """
        The name of each column in the SFrame.

        Returns
        -------
        out : list[string]
            Column names of the SFrame.

        See Also
        --------
        rename
        """
        return self.__proxy__.column_names()

    def column_types(self):
        """
        The type of each column in the SFrame.

        Returns
        -------
        out : list[type]
            Column types of the SFrame.

        See Also
        --------
        dtype
        """
        return self.__proxy__.dtype()

    def head(self, n=10):
        """
        The first n rows of the SFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows to fetch.

        Returns
        -------
        out : SFrame
            A new SFrame which contains the first n rows of the current SFrame

        See Also
        --------
        tail, print_rows
        """
        return SFrame(_proxy=self.__proxy__.head(n))

    def to_dataframe(self):
        """
        Convert this SFrame to pandas.DataFrame.

        This operation will construct a pandas.DataFrame in memory. Care must
        be taken when size of the returned object is big.

        Returns
        -------
        out : pandas.DataFrame
            The dataframe which contains all rows of SFrame
        """
        assert HAS_PANDAS
        df = pandas.DataFrame()
        for i in range(self.num_columns()):
            column_name = self.column_names()[i]
            df[column_name] = list(self[column_name])
            if len(df[column_name]) == 0:
                df[column_name] = df[column_name].astype(self.column_types()[i])
        return df

    def tail(self, n=10):
        """
        The last n rows of the SFrame.

        Parameters
        ----------
        n : int, optional
            The number of rows to fetch.

        Returns
        -------
        out : SFrame
            A new SFrame which contains the last n rows of the current SFrame

        See Also
        --------
        head, print_rows
        """
        return SFrame(_proxy=self.__proxy__.tail(n))

    def apply(self, fn, dtype=None, seed=None):
        """
        Transform each row to an :class:`~graphlab.SArray` according to a
        specified function. Returns a new SArray of ``dtype`` where each element
        in this SArray is transformed by `fn(x)` where `x` is a single row in
        the sframe represented as a dictionary.  The ``fn`` should return
        exactly one value which can be cast into type ``dtype``. If ``dtype`` is
        not specified, the first 100 rows of the SFrame are used to make a guess
        of the target data type.

        Parameters
        ----------
        fn : function
            The function to transform each row of the SFrame. The return
            type should be convertible to `dtype` if `dtype` is not None.
            This can also be a toolkit extension function which is compiled
            as a native shared library using SDK.

        dtype : dtype, optional
            The dtype of the new SArray. If None, the first 100
            elements of the array are used to guess the target
            data type.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : SArray
            The SArray transformed by fn.  Each element of the SArray is of
            type ``dtype``

        Examples
        --------
        Concatenate strings from several columns:

        >>> sf = graphlab.SFrame({'user_id': [1, 2, 3], 'movie_id': [3, 3, 6],
                                  'rating': [4, 5, 1]})
        >>> sf.apply(lambda x: str(x['user_id']) + str(x['movie_id']) + str(x['rating']))
        dtype: str
        Rows: 3
        ['134', '235', '361']

        Using native toolkit extension function:

        .. code-block:: c++

            #include <graphlab/sdk/toolkit_function_macros.hpp>
            double mean(const std::map<flexible_type, flexible_type>& dict) {
              double sum = 0.0;
              for (const auto& kv: dict) sum += (double)kv.second;
              return sum / dict.size();
            }

            BEGIN_FUNCTION_REGISTRATION
            REGISTER_FUNCTION(mean, "row");
            END_FUNCTION_REGISTRATION

        compiled into example.so

        >>> import example

        >>> sf = graphlab.SFrame({'x0': [1, 2, 3], 'x1': [2, 3, 1],
        ...                       'x2': [3, 1, 2]})
        >>> sf.apply(example.mean)
        dtype: float
        Rows: 3
        [2.0,2.0,2.0]
        """
        assert callable(fn), "Input must be callable"
        test_sf = self[:10]
        dryrun = [fn(row) for row in test_sf]
        if dtype is None:
            dtype = SArray(dryrun).dtype()

        if not seed:
            seed = int(time.time())

        _mt._get_metric_tracker().track('sframe.apply')

        nativefn = None
        try:
            from .. import extensions as extensions
            nativefn = extensions._build_native_function_call(fn)
        except:
            pass

        if nativefn is not None:
            # this is a toolkit lambda. We can do something about it
            with cython_context():
                return SArray(_proxy=self.__proxy__.transform_native(nativefn, dtype, seed))

        with cython_context():
            return SArray(_proxy=self.__proxy__.transform(fn, dtype, seed))

    def flat_map(self, column_names, fn, column_types='auto', seed=None):
        """
        Map each row of the SFrame to multiple rows in a new SFrame via a
        function.

        The output of `fn` must have type List[List[...]].  Each inner list
        will be a single row in the new output, and the collection of these
        rows within the outer list make up the data for the output SFrame.
        All rows must have the same length and the same order of types to
        make sure the result columns are homogeneously typed.  For example, if
        the first element emitted into in the outer list by `fn` is
        [43, 2.3, 'string'], then all other elements emitted into the outer
        list must be a list with three elements, where the first is an int,
        second is a float, and third is a string.  If column_types is not
        specified, the first 10 rows of the SFrame are used to determine the
        column types of the returned sframe.

        Parameters
        ----------
        column_names : list[str]
            The column names for the returned SFrame.

        fn : function
            The function that maps each of the sframe row into multiple rows,
            returning List[List[...]].  All outputted rows must have the same
            length and order of types.

        column_types : list[type], optional
            The column types of the output SFrame. Default value will be
            automatically inferred by running `fn` on the first 10 rows of the
            input. If the types cannot be inferred from the first 10 rows, an
            error is raised.

        seed : int, optional
            Used as the seed if a random number generator is included in `fn`.

        Returns
        -------
        out : SFrame
            A new SFrame containing the results of the flat_map of the
            original SFrame.

        Examples
        ---------
        Repeat each row according to the value in the 'number' column.

        >>> sf = graphlab.SFrame({'letter': ['a', 'b', 'c'],
        ...                       'number': [1, 2, 3]})
        >>> sf.flat_map(['number', 'letter'],
        ...             lambda x: [list(x.itervalues()) for i in range(0, x['number'])])
        +--------+--------+
        | number | letter |
        +--------+--------+
        |   1    |   a    |
        |   2    |   b    |
        |   2    |   b    |
        |   3    |   c    |
        |   3    |   c    |
        |   3    |   c    |
        +--------+--------+
        [6 rows x 2 columns]
        """
        assert callable(fn), "Input must be callable"
        if not seed:
            seed = int(time.time())

        _mt._get_metric_tracker().track('sframe.flat_map')

        # determine the column_types
        if column_types == 'auto':
            types = set()
            sample = self[0:10]
            results = [fn(row) for row in sample]

            for rows in results:
                if type(rows) is not list:
                    raise TypeError("Output type of the lambda function must be a list of lists")

                # note: this skips empty lists
                for row in rows:
                    if type(row) is not list:
                        raise TypeError("Output type of the lambda function must be a list of lists")
                    types.add(tuple([type(v) for v in row]))

            if len(types) == 0:
                raise TypeError, \
                    "Could not infer output column types from the first ten rows " +\
                    "of the SFrame. Please use the 'column_types' parameter to " +\
                    "set the types."

            if len(types) > 1:
                raise TypeError("Mapped rows must have the same length and types")

            column_types = list(types.pop())

        assert type(column_types) is list
        assert len(column_types) == len(column_names), "Number of output columns must match the size of column names"
        with cython_context():
            return SFrame(_proxy=self.__proxy__.flat_map(fn, column_names, column_types, seed))

    def sample(self, fraction, seed=None):
        """
        Sample the current SFrame's rows.

        Parameters
        ----------
        fraction : float
            Approximate fraction of the rows to fetch. Must be between 0 and 1.
            The number of rows returned is approximately the fraction times the
            number of rows.

        seed : int, optional
            Seed for the random number generator used to sample.

        Returns
        -------
        out : SFrame
            A new SFrame containing sampled rows of the current SFrame.

        Examples
        --------
        Suppose we have an SFrame with 6,145 rows.

        >>> import random
        >>> sf = SFrame({'id': range(0, 6145)})

        Retrieve about 30% of the SFrame rows with repeatable results by
        setting the random seed.

        >>> len(sf.sample(.3, seed=5))
        1783
        """
        if not seed:
            seed = int(time.time())

        if (fraction > 1 or fraction < 0):
            raise ValueError('Invalid sampling rate: ' + str(fraction))

        _mt._get_metric_tracker().track('sframe.sample')

        if (self.num_rows() == 0 or self.num_cols() == 0):
            return self
        else:
            with cython_context():
                return SFrame(_proxy=self.__proxy__.sample(fraction, seed))

    def random_split(self, fraction, seed=None):
        """
        Randomly split the rows of an SFrame into two SFrames. The first SFrame
        contains *M* rows, sampled uniformly (without replacement) from the
        original SFrame. *M* is approximately the fraction times the original
        number of rows. The second SFrame contains the remaining rows of the
        original SFrame.

        Parameters
        ----------
        fraction : float
            Approximate fraction of the rows to fetch for the first returned
            SFrame. Must be between 0 and 1.

        seed : int, optional
            Seed for the random number generator used to split.

        Returns
        -------
        out : tuple [SFrame]
            Two new SFrames.

        Examples
        --------
        Suppose we have an SFrame with 1,024 rows and we want to randomly split
        it into training and testing datasets with about a 90%/10% split.

        >>> sf = graphlab.SFrame({'id': range(1024)})
        >>> sf_train, sf_test = sf.random_split(.9, seed=5)
        >>> print len(sf_train), len(sf_test)
        922 102
        """
        if (fraction > 1 or fraction < 0):
            raise ValueError('Invalid sampling rate: ' + str(fraction))
        if (self.num_rows() == 0 or self.num_cols() == 0):
            return (SFrame(), SFrame())

        if seed is None:
            # Include the nanosecond component as well.
            seed = abs(hash("%0.20f" % time.time())) % (2 ** 31)

        # The server side requires this to be an int, so cast if we can
        try:
            seed = int(seed)
        except ValueError:
            raise ValueError('The \'seed\' parameter must be of type int.')

        _mt._get_metric_tracker().track('sframe.random_split')

        with cython_context():
            proxy_pair = self.__proxy__.random_split(fraction, seed)
            return (SFrame(data=[], _proxy=proxy_pair[0]), SFrame(data=[], _proxy=proxy_pair[1]))

    def topk(self, column_name, k=10, reverse=False):
        """
        Get top k rows according to the given column. Result is according to and
        sorted by `column_name` in the given order (default is descending).
        When `k` is small, `topk` is more efficient than `sort`.

        Parameters
        ----------
        column_name : string
            The column to sort on

        k : int, optional
            The number of rows to return

        reverse : bool, optional
            If True, return the top k rows in ascending order, otherwise, in
            descending order.

        Returns
        -------
        out : SFrame
            an SFrame containing the top k rows sorted by column_name.

        See Also
        --------
        sort

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': range(1000)})
        >>> sf['value'] = -sf['id']
        >>> sf.topk('id', k=3)
        +--------+--------+
        |   id   |  value |
        +--------+--------+
        |   999  |  -999  |
        |   998  |  -998  |
        |   997  |  -997  |
        +--------+--------+
        [3 rows x 2 columns]

        >>> sf.topk('value', k=3)
        +--------+--------+
        |   id   |  value |
        +--------+--------+
        |   1    |  -1    |
        |   2    |  -2    |
        |   3    |  -3    |
        +--------+--------+
        [3 rows x 2 columns]
        """
        if type(column_name) is not str:
            raise TypeError("column_name must be a string")

        _mt._get_metric_tracker().track('sframe.topk')

        sf = self[self[column_name].topk_index(k, reverse)]
        return sf.sort(column_name, ascending=reverse)

    def save(self, filename, format=None):
        """
        Save the SFrame to a file system for later use.

        Parameters
        ----------
        filename : string
            The location to save the SFrame. Either a local directory or a
            remote URL. If the format is 'binary', a directory will be created
            at the location which will contain the sframe.

        format : {'binary', 'csv', 'json'}, optional
            Format in which to save the SFrame. Binary saved SFrames can be
            loaded much faster and without any format conversion losses. If not
            given, will try to infer the format from filename given. If file
            name ends with 'csv' or '.csv.gz', then save as 'csv' format,
            otherwise save as 'binary' format.
            See export_csv for more csv saving options.

        See Also
        --------
        load_sframe, SFrame

        Examples
        --------
        >>> # Save the sframe into binary format
        >>> sf.save('data/training_data_sframe')

        >>> # Save the sframe into csv format
        >>> sf.save('data/training_data.csv', format='csv')
        """

        _mt._get_metric_tracker().track('sframe.save', properties={'format':format})
        if format == None:
            if filename.endswith(('.csv', '.csv.gz')):
                format = 'csv'
            else:
                format = 'binary'
        else:
            if format is 'csv':
                if not filename.endswith(('.csv', '.csv.gz')):
                    filename = filename + '.csv'
            elif format is not 'binary' and format is not 'json':
                raise ValueError("Invalid format: {}. Supported formats are 'csv' and 'binary' and 'json'".format(format))

        ## Save the SFrame
        url = _make_internal_url(filename)

        with cython_context():
            if format is 'binary':
                self.__proxy__.save(url)
            elif format is 'csv':
                assert filename.endswith(('.csv', '.csv.gz'))
                self.__proxy__.save_as_csv(url, {})
            elif format is 'json':
                self.export_json(url)
            else:
                raise ValueError("Unsupported format: {}".format(format))

    def export_csv(self, filename, delimiter=',', line_terminator='\n',
            header=True, quote_level=csv.QUOTE_NONNUMERIC, double_quote=True,
            escape_char='\\', quote_char='\"', na_rep='',
            file_header='', file_footer='', line_prefix='',
            _no_prefix_on_first_value=False, **kwargs):
        """
        Writes an SFrame to a CSV file.

        Parameters
        ----------
        filename : string
            The location to save the CSV.

        delimiter : string, optional
            This describes the delimiter used for writing csv files.

        line_terminator: string, optional
            The newline character

        header : bool, optional
            If true, the column names are emitted as a header.

        quote_level: csv.QUOTE_ALL | csv.QUOTE_NONE | csv.QUOTE_NONNUMERIC, optional
            The quoting level. If csv.QUOTE_ALL, every field is quoted.
            if csv.quote_NONE, no field is quoted. If csv.QUOTE_NONNUMERIC, only
            non-numeric fileds are quoted. csv.QUOTE_MINIMAL is interpreted as
            csv.QUOTE_NONNUMERIC.

        double_quote : bool, optional
            If True, quotes are escaped as two consecutive quotes

        escape_char : string, optional
            Character which begins a C escape sequence

        quote_char: string, optional
            Character used to quote fields

        na_rep: string, optional
            The value used to denote a missing value.

        file_header: string, optional
            A string printed to the start of the file

        file_footer: string, optional
            A string printed to the end of the file

        line_prefix: string, optional
            A string printed at the start of each value line
        """
        # Pandas argument compatibility
        if "sep" in kwargs:
            delimiter = kwargs['sep']
            del kwargs['sep']
        if "quotechar" in kwargs:
            quote_char = kwargs['quotechar']
            del kwargs['quotechar']
        if "doublequote" in kwargs:
            double_quote = kwargs['doublequote']
            del kwargs['doublequote']
        if "lineterminator" in kwargs:
            line_terminator = kwargs['lineterminator']
            del kwargs['lineterminator']
        if len(kwargs) > 0:
            raise TypeError("Unexpected keyword arguments " + str(kwargs.keys()))

        write_csv_options = {}
        write_csv_options['delimiter'] = delimiter
        write_csv_options['escape_char'] = escape_char
        write_csv_options['double_quote'] = double_quote
        write_csv_options['quote_char'] = quote_char
        if quote_level == csv.QUOTE_MINIMAL:
            write_csv_options['quote_level'] = 0
        elif quote_level == csv.QUOTE_ALL:
            write_csv_options['quote_level'] = 1
        elif quote_level == csv.QUOTE_NONNUMERIC:
            write_csv_options['quote_level'] = 2
        elif quote_level == csv.QUOTE_NONE:
            write_csv_options['quote_level'] = 3
        write_csv_options['header'] = header
        write_csv_options['line_terminator'] = line_terminator
        write_csv_options['na_value'] = na_rep
        write_csv_options['file_header'] = file_header
        write_csv_options['file_footer'] = file_footer
        write_csv_options['line_prefix'] = line_prefix

        # undocumented option. Disables line prefix on the first value line
        write_csv_options['_no_prefix_on_first_value'] = _no_prefix_on_first_value

        url = _make_internal_url(filename)
        self.__proxy__.save_as_csv(url, write_csv_options)

    def export_json(self,
                    filename,
                    orient='records'):
        """
        Writes an SFrame to a CSV file.

        Parameters
        ----------
        filename : string
            The location to save the JSON file.

        orient : string, optional. Either "records" or "lines"
            If orient="records" the file is saved as a single JSON array.
            If orient="lines", the file is saves as a JSON value per line.

        Examples
        --------
        The orient parameter describes the expected input format of the JSON
        file.

        If orient="records", the output will be a single JSON Array where
        each array element is a dictionary describing the row.

        >>> g
        Columns:
                a	int
                b	int
        Rows: 3
        Data:
        +---+---+
        | a | b |
        +---+---+
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
        +---+---+
        >>> g.export('output.json', orient='records')
        >>> !cat output.json
        [
        {'a':1,'b':1},
        {'a':2,'b':2},
        {'a':3,'b':3},
        ]

        If orient="rows", each row will be emitted as a JSON dictionary to
        each file line.

        >>> g
        Columns:
                a	int
                b	int
        Rows: 3
        Data:
        +---+---+
        | a | b |
        +---+---+
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
        +---+---+
        >>> g.export('output.json', orient='rows')
        >>> !cat output.json
        {'a':1,'b':1}
        {'a':2,'b':2}
        {'a':3,'b':3}
        """
        if orient == "records":
            self.pack_columns(dtype=dict).export_csv(
                    filename, file_header='[', file_footer=']',
                    header=False, double_quote=False,
                    quote_level=csv.QUOTE_NONE,
                    line_prefix=',',
                    _no_prefix_on_first_value=True)
        elif orient == "lines":
            self.pack_columns(dtype=dict).export_csv(
                    filename, header=False, double_quote=False, quote_level=csv.QUOTE_NONE)
        else:
            raise ValueError("Invalid value for orient parameter (" + str(orient) + ")")

    def _save_reference(self, filename):
        """
        Performs an incomplete save of an existing SFrame into a directory.
        This saved SFrame may reference SFrames in other locations in the same
        filesystem for certain resources.

        Parameters
        ----------
        filename : string
            The location to save the SFrame. Either a local directory or a
            remote URL.

        See Also
        --------
        load_sframe, SFrame

        Examples
        --------
        >>> # Save the sframe into binary format
        >>> sf.save_reference('data/training_data_sframe')
        """
        _mt._get_metric_tracker().track('sframe.save_reference')
        ## Save the SFrame
        url = _make_internal_url(filename)

        with cython_context():
            self.__proxy__.save_reference(url)


    def select_column(self, key):
        """
        Get a reference to the :class:`~graphlab.SArray` that corresponds with
        the given key. Throws an exception if the key is something other than a
        string or if the key is not found.

        Parameters
        ----------
        key : str
            The column name.

        Returns
        -------
        out : SArray
            The SArray that is referred by ``key``.

        See Also
        --------
        select_columns

        Examples
        --------
        >>> sf = graphlab.SFrame({'user_id': [1,2,3],
        ...                       'user_name': ['alice', 'bob', 'charlie']})
        >>> # This line is equivalent to `sa = sf['user_name']`
        >>> sa = sf.select_column('user_name')
        >>> sa
        dtype: str
        Rows: 3
        ['alice', 'bob', 'charlie']
        """
        if not isinstance(key, str):
            raise TypeError("Invalid key type: must be str")
        with cython_context():
            return SArray(data=[], _proxy=self.__proxy__.select_column(key))

    def select_columns(self, keylist):
        """
        Selects all columns where the name of the column or the type of column
        is included in the keylist. An exception is raised if duplicate columns
        are selected i.e. sf.select_columns(['a','a']), or non-existant columns
        are selected.

        Throws an exception for all other input types.

        Parameters
        ----------
        keylist : list[str or type]
            The list of column names or a list of types.

        Returns
        -------
        out : SFrame
            A new SFrame that is made up of the columns referred to in
            ``keylist`` from the current SFrame.

        See Also
        --------
        select_column

        Examples
        --------
        >>> sf = graphlab.SFrame({'user_id': [1,2,3],
        ...                       'user_name': ['alice', 'bob', 'charlie'],
        ...                       'zipcode': [98101, 98102, 98103]
        ...                      })
        >>> # This line is equivalent to `sf2 = sf[['user_id', 'zipcode']]`
        >>> sf2 = sf.select_columns(['user_id', 'zipcode'])
        >>> sf2
        +---------+---------+
        | user_id | zipcode |
        +---------+---------+
        |    1    |  98101  |
        |    2    |  98102  |
        |    3    |  98103  |
        +---------+---------+
        [3 rows x 2 columns]
        """
        if not hasattr(keylist, '__iter__'):
            raise TypeError("keylist must be an iterable")
        if not (all([isinstance(x, str) or isinstance(x, type) for x in keylist])):
            raise TypeError("Invalid key type: must be str or type")

        column_names_set = set(self.column_names())
        # quick validation to make sure all selected string columns exist
        requested_str_columns = [s for s in keylist if isinstance(s, str)]
        for i in requested_str_columns:
            if i not in column_names_set:
                raise RuntimeError("Column name " +  i + " does not exist")

        # Make sure there are no duplicates keys
        from collections import Counter
        keylist_counter = Counter(keylist)
        if (len(keylist)) != len(keylist_counter):
            for key in keylist_counter:
                if keylist_counter[key] > 1:
                    raise ValueError("There are duplicate keys in key list: '" + key + "'")

        colnames_and_types = zip(self.column_names(), self.column_types())

        # Ok. we want the string columns to be in the ordering defined by the
        # argument.  And then all the type selection columns.
        selected_columns = requested_str_columns
        typelist = [s for s in keylist if isinstance(s, type)]

        # next the type selection columns
        # loop through all the columns, adding all columns with types in
        # typelist. But don't add a column if it has already been added.
        for i in colnames_and_types:
            if i[1] in typelist and i[0] not in selected_columns:
                selected_columns += [i[0]]


        with cython_context():
            return SFrame(data=[], _proxy=self.__proxy__.select_columns(selected_columns))

    def add_column(self, data, name=""):
        """
        Add a column to this SFrame. The number of elements in the data given
        must match the length of every other column of the SFrame. This
        operation modifies the current SFrame in place and returns self. If no
        name is given, a default name is chosen.

        Parameters
        ----------
        data : SArray
            The 'column' of data to add.

        name : string, optional
            The name of the column. If no name is given, a default name is
            chosen.

        Returns
        -------
        out : SFrame
            The current SFrame.

        See Also
        --------
        add_columns

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> sa = graphlab.SArray(['cat', 'dog', 'fossa'])
        >>> # This line is equivalant to `sf['species'] = sa`
        >>> sf.add_column(sa, name='species')
        >>> sf
        +----+-----+---------+
        | id | val | species |
        +----+-----+---------+
        | 1  |  A  |   cat   |
        | 2  |  B  |   dog   |
        | 3  |  C  |  fossa  |
        +----+-----+---------+
        [3 rows x 3 columns]
        """
        # Check type for pandas dataframe or SArray?
        if not isinstance(data, SArray):
            raise TypeError("Must give column as SArray")
        if not isinstance(name, str):
            raise TypeError("Invalid column name: must be str")
        with cython_context():
            self.__proxy__.add_column(data.__proxy__, name)
        self._cache = None
        return self

    def add_columns(self, data, namelist=None):
        """
        Adds multiple columns to this SFrame. The number of elements in all
        columns must match the length of every other column of the SFrame. This
        operation modifies the current SFrame in place and returns self.

        Parameters
        ----------
        data : list[SArray] or SFrame
            The columns to add.

        namelist : list of string, optional
            A list of column names. All names must be specified. ``namelist`` is
            ignored if data is an SFrame.

        Returns
        -------
        out : SFrame
            The current SFrame.

        See Also
        --------
        add_column

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> sf2 = graphlab.SFrame({'species': ['cat', 'dog', 'fossa'],
        ...                        'age': [3, 5, 9]})
        >>> sf.add_columns(sf2)
        >>> sf
        +----+-----+-----+---------+
        | id | val | age | species |
        +----+-----+-----+---------+
        | 1  |  A  |  3  |   cat   |
        | 2  |  B  |  5  |   dog   |
        | 3  |  C  |  9  |  fossa  |
        +----+-----+-----+---------+
        [3 rows x 4 columns]
        """
        datalist = data
        if isinstance(data, SFrame):
            other = data
            datalist = [other.select_column(name) for name in other.column_names()]
            namelist = other.column_names()

            my_columns = set(self.column_names())
            for name in namelist:
                if name in my_columns:
                    raise ValueError("Column '" + name + "' already exists in current SFrame")
        else:
            if not hasattr(datalist, '__iter__'):
                raise TypeError("datalist must be an iterable")
            if not hasattr(namelist, '__iter__'):
                raise TypeError("namelist must be an iterable")

            if not all([isinstance(x, SArray) for x in datalist]):
                raise TypeError("Must give column as SArray")
            if not all([isinstance(x, str) for x in namelist]):
                raise TypeError("Invalid column name in list : must all be str")

        with cython_context():
            self.__proxy__.add_columns([x.__proxy__ for x in datalist], namelist)
        self._cache = None
        return self

    def remove_column(self, name):
        """
        Remove a column from this SFrame. This operation modifies the current
        SFrame in place and returns self.

        Parameters
        ----------
        name : string
            The name of the column to remove.

        Returns
        -------
        out : SFrame
            The SFrame with given column removed.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> # This is equivalent to `del sf['val']`
        >>> sf.remove_column('val')
        >>> sf
        +----+
        | id |
        +----+
        | 1  |
        | 2  |
        | 3  |
        +----+
        [3 rows x 1 columns]
        """
        name = str(name)
        if name not in self.column_names():
            raise KeyError('Cannot find column %s' % name)
        colid = self.column_names().index(name)
        with cython_context():
            self.__proxy__.remove_column(colid)
        self._cache = None
        return self

    def remove_columns(self, column_names):
        """
        Remove one or more columns from this SFrame. This operation modifies the current
        SFrame in place and returns self.

        Parameters
        ----------
        column_names : list or iterable
            A list or iterable of column names.

        Returns
        -------
        out : SFrame
            The SFrame with given columns removed.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3], 'val1': ['A', 'B', 'C'], 'val2' : [10, 11, 12]})
        >>> sf.remove_columns(['val1', 'val2'])
        >>> sf
        +----+
        | id |
        +----+
        | 1  |
        | 2  |
        | 3  |
        +----+
        [3 rows x 1 columns]
        """
        column_names = list(column_names)
        existing_columns = dict((k, i) for i, k in enumerate(self.column_names()))

        for name in column_names:
            if name not in existing_columns:
                raise KeyError('Cannot find column %s' % name)

        # Delete it going backwards so we don't invalidate indices
        deletion_indices = sorted(existing_columns[name] for name in column_names)
        for colid in reversed(deletion_indices):
            with cython_context():
                self.__proxy__.remove_column(colid)
        self._cache = None
        return self


    def swap_columns(self, column_1, column_2):
        """
        Swap the columns with the given names. This operation modifies the
        current SFrame in place and returns self.

        Parameters
        ----------
        column_1 : string
            Name of column to swap

        column_2 : string
            Name of other column to swap

        Returns
        -------
        out : SFrame
            The SFrame with swapped columns.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> sf.swap_columns('id', 'val')
        >>> sf
        +-----+-----+
        | val | id  |
        +-----+-----+
        |  A  |  1  |
        |  B  |  2  |
        |  C  |  3  |
        +----+-----+
        [3 rows x 2 columns]
        """
        colnames = self.column_names()
        colid_1 = colnames.index(column_1)
        colid_2 = colnames.index(column_2)
        with cython_context():
            self.__proxy__.swap_columns(colid_1, colid_2)
        self._cache = None
        return self

    def rename(self, names):
        """
        Rename the given columns. ``names`` is expected to be a dict specifying
        the old and new names. This changes the names of the columns given as
        the keys and replaces them with the names given as the values.  This
        operation modifies the current SFrame in place and returns self.

        Parameters
        ----------
        names : dict [string, string]
            Dictionary of [old_name, new_name]

        Returns
        -------
        out : SFrame
            The current SFrame.

        See Also
        --------
        column_names

        Examples
        --------
        >>> sf = SFrame({'X1': ['Alice','Bob'],
        ...              'X2': ['123 Fake Street','456 Fake Street']})
        >>> sf.rename({'X1': 'name', 'X2':'address'})
        >>> sf
        +-------+-----------------+
        |  name |     address     |
        +-------+-----------------+
        | Alice | 123 Fake Street |
        |  Bob  | 456 Fake Street |
        +-------+-----------------+
        [2 rows x 2 columns]
        """
        if (type(names) is not dict):
            raise TypeError('names must be a dictionary: oldname -> newname')
        all_columns = set(self.column_names())
        for k in names:
            if not k in all_columns:
                raise ValueError('Cannot find column %s in the SFrame' % k)
        with cython_context():
            for k in names:
                colid = self.column_names().index(k)
                self.__proxy__.set_column_name(colid, names[k])
        self._cache = None
        return self

    def __getitem__(self, key):
        """
        This method does things based on the type of `key`.

        If `key` is:
            * str
                selects column with name 'key'
            * type
                selects all columns with types matching the type
            * list of str or type
                selects all columns with names or type in the list
            * SArray
                Performs a logical filter.  Expects given SArray to be the same
                length as all columns in current SFrame.  Every row
                corresponding with an entry in the given SArray that is
                equivalent to False is filtered from the result.
            * int
                Returns a single row of the SFrame (the `key`th one) as a dictionary.
            * slice
                Returns an SFrame including only the sliced rows.
        """
        if type(key) is SArray:
            return self._row_selector(key)
        elif type(key) is str:
            return self.select_column(key)
        elif type(key) is type:
            return self.select_columns([key])
        elif hasattr(key, '__iter__'):
            return self.select_columns(key)
        elif isinstance(key, numbers.Integral):
            sf_len = len(self)

            if key < 0:
                key = sf_len + key
            if key >= sf_len:
                raise IndexError("SFrame index out of range")

            if not hasattr(self, '_cache') or self._cache is None:
                self._cache = {}

            try:
                lb, ub, value_list = self._cache["getitem_cache"]
                if lb <= key < ub:
                    return value_list[key - lb]

            except KeyError:
                pass

            # Not in cache, need to grab it.  Smaller here than with sarray

            # Do we have a good block size that won't cause memory to blow up?
            if not "getitem_cache_blocksize" in self._cache:
                block_size = \
                  (8*1024) / sum( (2 if dt in [int, long, float] else 8) for dt in self.column_types())

                block_size = max(16, block_size)
                self._cache["getitem_cache_blocksize"] = block_size
            else:
                block_size = self._cache["getitem_cache_blocksize"]

            block_num = int(key // block_size)

            lb = block_num * block_size
            ub = min(sf_len, lb + block_size)

            val_list = list(SFrame(_proxy = self.__proxy__.copy_range(lb, 1, ub)))
            self._cache["getitem_cache"] = (lb, ub, val_list)
            return val_list[key - lb]

        elif type(key) is slice:
            start = key.start
            stop = key.stop
            step = key.step
            if start is None:
                start = 0
            if stop is None:
                stop = len(self)
            if step is None:
                step = 1
            # handle negative indices
            if start < 0:
                start = len(self) + start
            if stop < 0:
                stop = len(self) + stop
            return SFrame(_proxy = self.__proxy__.copy_range(start, step, stop))
        else:
            raise TypeError("Invalid index type: must be SArray, list, int, or str")

    def __setitem__(self, key, value):
        """
        A wrapper around add_column(s).  Key can be either a list or a str.  If
        value is an SArray, it is added to the SFrame as a column.  If it is a
        constant value (int, str, or float), then a column is created where
        every entry is equal to the constant value.  Existing columns can also
        be replaced using this wrapper.
        """
        if type(key) is list:
            self.add_columns(value, key)
        elif type(key) is str:
            sa_value = None
            if (type(value) is SArray):
                sa_value = value
            elif hasattr(value, '__iter__'):  # wrap list, array... to sarray
                sa_value = SArray(value)
            else:  # create an sarray  of constant value
                sa_value = SArray.from_const(value, self.num_rows())

            # set new column
            if not key in self.column_names():
                with cython_context():
                    self.add_column(sa_value, key)
            else:
                # special case if replacing the only column.
                # server would fail the replacement if the new column has different
                # length than current one, which doesn't make sense if we are replacing
                # the only column. To support this, we first take out the only column
                # and then put it back if exception happens
                single_column = (self.num_cols() == 1)
                if (single_column):
                    tmpname = key
                    saved_column = self.select_column(key)
                    self.remove_column(key)
                else:
                    # add the column to a unique column name.
                    tmpname = '__' + '-'.join(self.column_names())
                try:
                    self.add_column(sa_value, tmpname)
                except Exception as e:
                    if (single_column):
                        self.add_column(saved_column, key)
                    raise

                if (not single_column):
                    # if add succeeded, remove the column name and rename tmpname->columnname.
                    self.swap_columns(key, tmpname)
                    self.remove_column(key)
                    self.rename({tmpname: key})

        else:
            raise TypeError('Cannot set column with key type ' + str(type(key)))

    def __delitem__(self, key):
        """
        Wrapper around remove_column.
        """
        self.remove_column(key)

    def __materialize__(self):
        """
        For an SFrame that is lazily evaluated, force the persistence of the
        SFrame to disk, committing all lazy evaluated operations.
        """
        with cython_context():
            self.__proxy__.materialize()

    def __is_materialized__(self):
        """
        Returns whether or not the SFrame has been materialized.
        """
        return self.__proxy__.is_materialized()

    def __has_size__(self):
        """
        Returns whether or not the size of the SFrame is known.
        """
        return self.__proxy__.has_size()

    def __query_plan_str__(self):
        """
        Returns the query plan as a dot graph string
        """
        return self.__proxy__.query_plan_string()

    def __iter__(self):
        """
        Provides an iterator to the rows of the SFrame.
        """

        _mt._get_metric_tracker().track('sframe.__iter__')

        def generator():
            elems_at_a_time = 262144
            self.__proxy__.begin_iterator()
            ret = self.__proxy__.iterator_get_next(elems_at_a_time)
            column_names = self.column_names()
            while(True):
                for j in ret:
                    yield dict(zip(column_names, j))

                if len(ret) == elems_at_a_time:
                    ret = self.__proxy__.iterator_get_next(elems_at_a_time)
                else:
                    break

        return generator()

    def append(self, other):
        """
        Add the rows of an SFrame to the end of this SFrame.

        Both SFrames must have the same set of columns with the same column
        names and column types.

        Parameters
        ----------
        other : SFrame
            Another SFrame whose rows are appended to the current SFrame.

        Returns
        -------
        out : SFrame
            The result SFrame from the append operation.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [4, 6, 8], 'val': ['D', 'F', 'H']})
        >>> sf2 = graphlab.SFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        >>> sf = sf.append(sf2)
        >>> sf
        +----+-----+
        | id | val |
        +----+-----+
        | 4  |  D  |
        | 6  |  F  |
        | 8  |  H  |
        | 1  |  A  |
        | 2  |  B  |
        | 3  |  C  |
        +----+-----+
        [6 rows x 2 columns]
        """
        _mt._get_metric_tracker().track('sframe.append')
        if type(other) is not SFrame:
            raise RuntimeError("SFrame append can only work with SFrame")

        left_empty = len(self.column_names()) == 0
        right_empty = len(other.column_names()) == 0

        if (left_empty and right_empty):
            return SFrame()

        if (left_empty or right_empty):
            non_empty_sframe = self if right_empty else other
            return non_empty_sframe

        my_column_names = self.column_names()
        my_column_types = self.column_types()
        other_column_names = other.column_names()
        if (len(my_column_names) != len(other_column_names)):
            raise RuntimeError("Two SFrames have to have the same number of columns")

        # check if the order of column name is the same
        column_name_order_match = True
        for i in range(len(my_column_names)):
            if other_column_names[i] != my_column_names[i]:
                column_name_order_match = False
                break;


        processed_other_frame = other
        if not column_name_order_match:
            # we allow name order of two sframes to be different, so we create a new sframe from
            # "other" sframe to make it has exactly the same shape
            processed_other_frame = SFrame()
            for i in range(len(my_column_names)):
                col_name = my_column_names[i]
                if(col_name not in other_column_names):
                    raise RuntimeError("Column " + my_column_names[i] + " does not exist in second SFrame")

                other_column = other.select_column(col_name);
                processed_other_frame.add_column(other_column, col_name)

                # check column type
                if my_column_types[i] != other_column.dtype():
                    raise RuntimeError("Column " + my_column_names[i] + " type is not the same in two SFrames, one is " + str(my_column_types[i]) + ", the other is " + str(other_column.dtype()))

        with cython_context():
            return SFrame(_proxy=self.__proxy__.append(processed_other_frame.__proxy__))

    def groupby(self, key_columns, operations, *args):
        """
        Perform a group on the key_columns followed by aggregations on the
        columns listed in operations.

        The operations parameter is a dictionary that indicates which
        aggregation operators to use and which columns to use them on. The
        available operators are SUM, MAX, MIN, COUNT, AVG, VAR, STDV, CONCAT,
        SELECT_ONE, ARGMIN, ARGMAX, and QUANTILE. For convenience, aggregators
        MEAN, STD, and VARIANCE are available as synonyms for AVG, STDV, and
        VAR. See :mod:`~graphlab.aggregate` for more detail on the aggregators.

        Parameters
        ----------
        key_columns : string | list[string]
            Column(s) to group by. Key columns can be of any type other than
            dictionary.

        operations : dict, list
            Dictionary of columns and aggregation operations. Each key is a
            output column name and each value is an aggregator. This can also
            be a list of aggregators, in which case column names will be
            automatically assigned.

        *args
            All other remaining arguments will be interpreted in the same
            way as the operations argument.

        Returns
        -------
        out_sf : SFrame
            A new SFrame, with a column for each groupby column and each
            aggregation operation.

        See Also
        --------
        aggregate

        Examples
        --------
        Suppose we have an SFrame with movie ratings by many users.

        >>> import graphlab.aggregate as agg
        >>> url = 'http://s3.amazonaws.com/gl-testdata/rating_data_example.csv'
        >>> sf = graphlab.SFrame.read_csv(url)
        >>> sf
        +---------+----------+--------+
        | user_id | movie_id | rating |
        +---------+----------+--------+
        |  25904  |   1663   |   3    |
        |  25907  |   1663   |   3    |
        |  25923  |   1663   |   3    |
        |  25924  |   1663   |   3    |
        |  25928  |   1663   |   2    |
        |  25933  |   1663   |   4    |
        |  25934  |   1663   |   4    |
        |  25935  |   1663   |   4    |
        |  25936  |   1663   |   5    |
        |  25937  |   1663   |   2    |
        |   ...   |   ...    |  ...   |
        +---------+----------+--------+
        [10000 rows x 3 columns]

        Compute the number of occurrences of each user.

        >>> user_count = sf.groupby(key_columns='user_id',
        ...                         operations={'count': agg.COUNT()})
        >>> user_count
        +---------+-------+
        | user_id | count |
        +---------+-------+
        |  62361  |   1   |
        |  30727  |   1   |
        |  40111  |   1   |
        |  50513  |   1   |
        |  35140  |   1   |
        |  42352  |   1   |
        |  29667  |   1   |
        |  46242  |   1   |
        |  58310  |   1   |
        |  64614  |   1   |
        |   ...   |  ...  |
        +---------+-------+
        [9852 rows x 2 columns]

        Compute the mean and standard deviation of ratings per user.

        >>> user_rating_stats = sf.groupby(key_columns='user_id',
        ...                                operations={
        ...                                    'mean_rating': agg.MEAN('rating'),
        ...                                    'std_rating': agg.STD('rating')
        ...                                })
        >>> user_rating_stats
        +---------+-------------+------------+
        | user_id | mean_rating | std_rating |
        +---------+-------------+------------+
        |  62361  |     5.0     |    0.0     |
        |  30727  |     4.0     |    0.0     |
        |  40111  |     2.0     |    0.0     |
        |  50513  |     4.0     |    0.0     |
        |  35140  |     4.0     |    0.0     |
        |  42352  |     5.0     |    0.0     |
        |  29667  |     4.0     |    0.0     |
        |  46242  |     5.0     |    0.0     |
        |  58310  |     2.0     |    0.0     |
        |  64614  |     2.0     |    0.0     |
        |   ...   |     ...     |    ...     |
        +---------+-------------+------------+
        [9852 rows x 3 columns]

        Compute the movie with the minimum rating per user.

        >>> chosen_movies = sf.groupby(key_columns='user_id',
        ...                            operations={
        ...                                'worst_movies': agg.ARGMIN('rating','movie_id')
        ...                            })
        >>> chosen_movies
        +---------+-------------+
        | user_id | worst_movies |
        +---------+-------------+
        |  62361  |     1663    |
        |  30727  |     1663    |
        |  40111  |     1663    |
        |  50513  |     1663    |
        |  35140  |     1663    |
        |  42352  |     1663    |
        |  29667  |     1663    |
        |  46242  |     1663    |
        |  58310  |     1663    |
        |  64614  |     1663    |
        |   ...   |     ...     |
        +---------+-------------+
        [9852 rows x 2 columns]

        Compute the movie with the max rating per user and also the movie with
        the maximum imdb-ranking per user.

        >>> sf['imdb-ranking'] = sf['rating'] * 10
        >>> chosen_movies = sf.groupby(key_columns='user_id',
        ...         operations={('max_rating_movie','max_imdb_ranking_movie'): agg.ARGMAX(('rating','imdb-ranking'),'movie_id')})
        >>> chosen_movies
        +---------+------------------+------------------------+
        | user_id | max_rating_movie | max_imdb_ranking_movie |
        +---------+------------------+------------------------+
        |  62361  |       1663       |          16630         |
        |  30727  |       1663       |          16630         |
        |  40111  |       1663       |          16630         |
        |  50513  |       1663       |          16630         |
        |  35140  |       1663       |          16630         |
        |  42352  |       1663       |          16630         |
        |  29667  |       1663       |          16630         |
        |  46242  |       1663       |          16630         |
        |  58310  |       1663       |          16630         |
        |  64614  |       1663       |          16630         |
        |   ...   |       ...        |          ...           |
        +---------+------------------+------------------------+
        [9852 rows x 3 columns]

        Compute the movie with the max rating per user.

        >>> chosen_movies = sf.groupby(key_columns='user_id',
                    operations={'best_movies': agg.ARGMAX('rating','movie')})

        Compute the movie with the max rating per user and also the movie with the maximum imdb-ranking per user.

        >>> chosen_movies = sf.groupby(key_columns='user_id',
                   operations={('max_rating_movie','max_imdb_ranking_movie'): agg.ARGMAX(('rating','imdb-ranking'),'movie')})

        Compute the count, mean, and standard deviation of ratings per (user,
        time), automatically assigning output column names.

        >>> sf['time'] = sf.apply(lambda x: (x['user_id'] + x['movie_id']) % 11 + 2000)
        >>> user_rating_stats = sf.groupby(['user_id', 'time'],
        ...                                [agg.COUNT(),
        ...                                 agg.AVG('rating'),
        ...                                 agg.STDV('rating')])
        >>> user_rating_stats
        +------+---------+-------+---------------+----------------+
        | time | user_id | Count | Avg of rating | Stdv of rating |
        +------+---------+-------+---------------+----------------+
        | 2006 |  61285  |   1   |      4.0      |      0.0       |
        | 2000 |  36078  |   1   |      4.0      |      0.0       |
        | 2003 |  47158  |   1   |      3.0      |      0.0       |
        | 2007 |  34446  |   1   |      3.0      |      0.0       |
        | 2010 |  47990  |   1   |      3.0      |      0.0       |
        | 2003 |  42120  |   1   |      5.0      |      0.0       |
        | 2007 |  44940  |   1   |      4.0      |      0.0       |
        | 2008 |  58240  |   1   |      4.0      |      0.0       |
        | 2002 |   102   |   1   |      1.0      |      0.0       |
        | 2009 |  52708  |   1   |      3.0      |      0.0       |
        | ...  |   ...   |  ...  |      ...      |      ...       |
        +------+---------+-------+---------------+----------------+
        [10000 rows x 5 columns]


        The groupby function can take a variable length list of aggregation
        specifiers so if we want the count and the 0.25 and 0.75 quantiles of
        ratings:

        >>> user_rating_stats = sf.groupby(['user_id', 'time'], agg.COUNT(),
        ...                                {'rating_quantiles': agg.QUANTILE('rating',[0.25, 0.75])})
        >>> user_rating_stats
        +------+---------+-------+------------------------+
        | time | user_id | Count |    rating_quantiles    |
        +------+---------+-------+------------------------+
        | 2006 |  61285  |   1   | array('d', [4.0, 4.0]) |
        | 2000 |  36078  |   1   | array('d', [4.0, 4.0]) |
        | 2003 |  47158  |   1   | array('d', [3.0, 3.0]) |
        | 2007 |  34446  |   1   | array('d', [3.0, 3.0]) |
        | 2010 |  47990  |   1   | array('d', [3.0, 3.0]) |
        | 2003 |  42120  |   1   | array('d', [5.0, 5.0]) |
        | 2007 |  44940  |   1   | array('d', [4.0, 4.0]) |
        | 2008 |  58240  |   1   | array('d', [4.0, 4.0]) |
        | 2002 |   102   |   1   | array('d', [1.0, 1.0]) |
        | 2009 |  52708  |   1   | array('d', [3.0, 3.0]) |
        | ...  |   ...   |  ...  |          ...           |
        +------+---------+-------+------------------------+
        [10000 rows x 4 columns]

        To put all items a user rated into one list value by their star rating:

        >>> user_rating_stats = sf.groupby(["user_id", "rating"],
        ...                                {"rated_movie_ids":agg.CONCAT("movie_id")})
        >>> user_rating_stats
        +--------+---------+----------------------+
        | rating | user_id |     rated_movie_ids  |
        +--------+---------+----------------------+
        |   3    |  31434  | array('d', [1663.0]) |
        |   5    |  25944  | array('d', [1663.0]) |
        |   4    |  38827  | array('d', [1663.0]) |
        |   4    |  51437  | array('d', [1663.0]) |
        |   4    |  42549  | array('d', [1663.0]) |
        |   4    |  49532  | array('d', [1663.0]) |
        |   3    |  26124  | array('d', [1663.0]) |
        |   4    |  46336  | array('d', [1663.0]) |
        |   4    |  52133  | array('d', [1663.0]) |
        |   5    |  62361  | array('d', [1663.0]) |
        |  ...   |   ...   |         ...          |
        +--------+---------+----------------------+
        [9952 rows x 3 columns]

        To put all items and rating of a given user together into a dictionary
        value:

        >>> user_rating_stats = sf.groupby("user_id",
        ...                                {"movie_rating":agg.CONCAT("movie_id", "rating")})
        >>> user_rating_stats
        +---------+--------------+
        | user_id | movie_rating |
        +---------+--------------+
        |  62361  |  {1663: 5}   |
        |  30727  |  {1663: 4}   |
        |  40111  |  {1663: 2}   |
        |  50513  |  {1663: 4}   |
        |  35140  |  {1663: 4}   |
        |  42352  |  {1663: 5}   |
        |  29667  |  {1663: 4}   |
        |  46242  |  {1663: 5}   |
        |  58310  |  {1663: 2}   |
        |  64614  |  {1663: 2}   |
        |   ...   |     ...      |
        +---------+--------------+
        [9852 rows x 2 columns]
        """
        # some basic checking first
        # make sure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        # check that every column is a string, and is a valid column name
        my_column_names = self.column_names()
        key_columns_array = []
        for column in key_columns:
            if not isinstance(column, str):
                raise TypeError("Column name must be a string")
            if column not in my_column_names:
                raise KeyError("Column " + column + " does not exist in SFrame")
            if self[column].dtype() == dict:
                raise TypeError("Cannot group on a dictionary column.")
            key_columns_array.append(column)

        group_output_columns = []
        group_columns = []
        group_ops = []

        all_ops = [operations] + list(args)
        for op_entry in all_ops:
            # if it is not a dict, nor a list, it is just a single aggregator
            # element (probably COUNT). wrap it in a list so we can reuse the
            # list processing code
            operation = op_entry
            if not(isinstance(operation, list) or isinstance(operation, dict)):
              operation = [operation]
            if isinstance(operation, dict):
              # now sweep the dict and add to group_columns and group_ops
              for key in operation:
                  val = operation[key]
                  if type(val) is tuple:
                    (op, column) = val
                    if (op == '__builtin__avg__' and self[column[0]].dtype() is array.array):
                        op = '__builtin__vector__avg__'

                    if (op == '__builtin__sum__' and self[column[0]].dtype() is array.array):
                        op = '__builtin__vector__sum__'

                    if (op == '__builtin__argmax__' or op == '__builtin__argmin__') and ((type(column[0]) is tuple) != (type(key) is tuple)):
                        raise TypeError("Output column(s) and aggregate column(s) for aggregate operation should be either all tuple or all string.")

                    if (op == '__builtin__argmax__' or op == '__builtin__argmin__') and type(column[0]) is tuple:
                      for (col,output) in zip(column[0],key):
                        group_columns = group_columns + [[col,column[1]]]
                        group_ops = group_ops + [op]
                        group_output_columns = group_output_columns + [output]
                    else:
                      group_columns = group_columns + [column]
                      group_ops = group_ops + [op]
                      group_output_columns = group_output_columns + [key]

                    if (op == '__builtin__concat__dict__'):
                        key_column = column[0]
                        key_column_type = self.select_column(key_column).dtype()
                        if not key_column_type in (int, float, str):
                            raise TypeError('CONCAT key column must be int, float or str type')

                  elif val == aggregate.COUNT:
                    group_output_columns = group_output_columns + [key]
                    val = aggregate.COUNT()
                    (op, column) = val
                    group_columns = group_columns + [column]
                    group_ops = group_ops + [op]
                  else:
                    raise TypeError("Unexpected type in aggregator definition of output column: " + key)
            elif isinstance(operation, list):
              # we will be using automatically defined column names
              for val in operation:
                  if type(val) is tuple:
                    (op, column) = val
                    if (op == '__builtin__avg__' and self[column[0]].dtype() is array.array):
                        op = '__builtin__vector__avg__'

                    if (op == '__builtin__sum__' and self[column[0]].dtype() is array.array):
                        op = '__builtin__vector__sum__'

                    if (op == '__builtin__argmax__' or op == '__builtin__argmin__') and type(column[0]) is tuple:
                      for col in column[0]:
                        group_columns = group_columns + [[col,column[1]]]
                        group_ops = group_ops + [op]
                        group_output_columns = group_output_columns + [""]
                    else:
                      group_columns = group_columns + [column]
                      group_ops = group_ops + [op]
                      group_output_columns = group_output_columns + [""]

                    if (op == '__builtin__concat__dict__'):
                        key_column = column[0]
                        key_column_type = self.select_column(key_column).dtype()
                        if not key_column_type in (int, float, str):
                            raise TypeError('CONCAT key column must be int, float or str type')

                  elif val == aggregate.COUNT:
                    group_output_columns = group_output_columns + [""]
                    val = aggregate.COUNT()
                    (op, column) = val
                    group_columns = group_columns + [column]
                    group_ops = group_ops + [op]
                  else:
                    raise TypeError("Unexpected type in aggregator definition.")


        # let's validate group_columns and group_ops are valid
        for (cols, op) in zip(group_columns, group_ops):
            for col in cols:
                if not isinstance(col, str):
                    raise TypeError("Column name must be a string")

            if not isinstance(op, str):
                raise TypeError("Operation type not recognized.")

            if op is not aggregate.COUNT()[0]:
                for col in cols:
                    if col not in my_column_names:
                        raise KeyError("Column " + col + " does not exist in SFrame")

            _mt._get_metric_tracker().track('sframe.groupby', properties={'operator':op})

        with cython_context():
            return SFrame(_proxy=self.__proxy__.groupby_aggregate(key_columns_array, group_columns,
                                                                  group_output_columns, group_ops))

    def join(self, right, on=None, how='inner'):
        """
        Merge two SFrames. Merges the current (left) SFrame with the given
        (right) SFrame using a SQL-style equi-join operation by columns.

        Parameters
        ----------
        right : SFrame
            The SFrame to join.

        on : None | str | list | dict, optional
            The column name(s) representing the set of join keys.  Each row that
            has the same value in this set of columns will be merged together.

            * If 'None' is given, join will use all columns that have the same
              name as the set of join keys.

            * If a str is given, this is interpreted as a join using one column,
              where both SFrames have the same column name.

            * If a list is given, this is interpreted as a join using one or
              more column names, where each column name given exists in both
              SFrames.

            * If a dict is given, each dict key is taken as a column name in the
              left SFrame, and each dict value is taken as the column name in
              right SFrame that will be joined together. e.g.
              {'left_col_name':'right_col_name'}.

        how : {'left', 'right', 'outer', 'inner'}, optional
            The type of join to perform.  'inner' is default.

            * inner: Equivalent to a SQL inner join.  Result consists of the
              rows from the two frames whose join key values match exactly,
              merged together into one SFrame.

            * left: Equivalent to a SQL left outer join. Result is the union
              between the result of an inner join and the rest of the rows from
              the left SFrame, merged with missing values.

            * right: Equivalent to a SQL right outer join.  Result is the union
              between the result of an inner join and the rest of the rows from
              the right SFrame, merged with missing values.

            * outer: Equivalent to a SQL full outer join. Result is
              the union between the result of a left outer join and a right
              outer join.

        Returns
        -------
        out : SFrame

        Examples
        --------
        >>> animals = graphlab.SFrame({'id': [1, 2, 3, 4],
        ...                           'name': ['dog', 'cat', 'sheep', 'cow']})
        >>> sounds = graphlab.SFrame({'id': [1, 3, 4, 5],
        ...                          'sound': ['woof', 'baa', 'moo', 'oink']})
        >>> animals.join(sounds, how='inner')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        +----+-------+-------+
        [3 rows x 3 columns]

        >>> animals.join(sounds, on='id', how='left')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 2  |  cat  |  None |
        +----+-------+-------+
        [4 rows x 3 columns]

        >>> animals.join(sounds, on=['id'], how='right')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 5  |  None |  oink |
        +----+-------+-------+
        [4 rows x 3 columns]

        >>> animals.join(sounds, on={'id':'id'}, how='outer')
        +----+-------+-------+
        | id |  name | sound |
        +----+-------+-------+
        | 1  |  dog  |  woof |
        | 3  | sheep |  baa  |
        | 4  |  cow  |  moo  |
        | 5  |  None |  oink |
        | 2  |  cat  |  None |
        +----+-------+-------+
        [5 rows x 3 columns]
        """
        _mt._get_metric_tracker().track('sframe.join', properties={'type':how})
        available_join_types = ['left','right','outer','inner']

        if not isinstance(right, SFrame):
            raise TypeError("Can only join two SFrames")

        if how not in available_join_types:
            raise ValueError("Invalid join type")

        if (self.num_cols() <= 0) or (right.num_cols() <= 0):
            raise ValueError("Cannot join an SFrame with no columns.")

        join_keys = dict()
        if on is None:
            left_names = self.column_names()
            right_names = right.column_names()
            common_columns = [name for name in left_names if name in right_names]
            for name in common_columns:
                join_keys[name] = name
        elif type(on) is str:
            join_keys[on] = on
        elif type(on) is list:
            for name in on:
                if type(name) is not str:
                    raise TypeError("Join keys must each be a str.")
                join_keys[name] = name
        elif type(on) is dict:
            join_keys = on
        else:
            raise TypeError("Must pass a str, list, or dict of join keys")

        with cython_context():
            return SFrame(_proxy=self.__proxy__.join(right.__proxy__, how, join_keys))

    def filter_by(self, values, column_name, exclude=False):
        """
        Filter an SFrame by values inside an iterable object. Result is an
        SFrame that only includes (or excludes) the rows that have a column
        with the given ``column_name`` which holds one of the values in the
        given ``values`` :class:`~graphlab.SArray`. If ``values`` is not an
        SArray, we attempt to convert it to one before filtering.

        Parameters
        ----------
        values : SArray | list | numpy.ndarray | pandas.Series | str
            The values to use to filter the SFrame.  The resulting SFrame will
            only include rows that have one of these values in the given
            column.

        column_name : str
            The column of the SFrame to match with the given `values`.

        exclude : bool
            If True, the result SFrame will contain all rows EXCEPT those that
            have one of ``values`` in ``column_name``.

        Returns
        -------
        out : SFrame
            The filtered SFrame.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id': [1, 2, 3, 4],
        ...                      'animal_type': ['dog', 'cat', 'cow', 'horse'],
        ...                      'name': ['bob', 'jim', 'jimbob', 'bobjim']})
        >>> household_pets = ['cat', 'hamster', 'dog', 'fish', 'bird', 'snake']
        >>> sf.filter_by(household_pets, 'animal_type')
        +-------------+----+------+
        | animal_type | id | name |
        +-------------+----+------+
        |     dog     | 1  | bob  |
        |     cat     | 2  | jim  |
        +-------------+----+------+
        [2 rows x 3 columns]
        >>> sf.filter_by(household_pets, 'animal_type', exclude=True)
        +-------------+----+--------+
        | animal_type | id |  name  |
        +-------------+----+--------+
        |    horse    | 4  | bobjim |
        |     cow     | 3  | jimbob |
        +-------------+----+--------+
        [2 rows x 3 columns]
        """
        _mt._get_metric_tracker().track('sframe.filter_by')
        if type(column_name) is not str:
            raise TypeError("Must pass a str as column_name")

        existing_columns = self.column_names()
        if column_name not in existing_columns:
            raise KeyError("Column '" + column_name + "' not in SFrame.")

        if type(values) is not SArray:
            # If we were given a single element, try to put in list and convert
            # to SArray
            if not hasattr(values, '__iter__'):
                values = [values]
            values = SArray(values)

        value_sf = SFrame()
        value_sf.add_column(values, column_name)

        existing_type = self.column_types()[self.column_names().index(column_name)]
        given_type = value_sf.column_types()[0]
        if given_type != existing_type:
            raise TypeError("Type of given values does not match type of column '" +
                column_name + "' in SFrame.")

        # Make sure the values list has unique values, or else join will not
        # filter.
        value_sf = value_sf.groupby(column_name, {})

        with cython_context():
            if exclude:
                id_name = "id"
                # Make sure this name is unique so we know what to remove in
                # the result
                while id_name in existing_columns:
                    id_name += "1"
                value_sf = value_sf.add_row_number(id_name)

                tmp = SFrame(_proxy=self.__proxy__.join(value_sf.__proxy__,
                                                     'left',
                                                     {column_name:column_name}))
                ret_sf = tmp[tmp[id_name] == None]
                del ret_sf[id_name]
                return ret_sf
            else:
                return SFrame(_proxy=self.__proxy__.join(value_sf.__proxy__,
                                                     'inner',
                                                     {column_name:column_name}))

    def show(self, columns=None, view=None, x=None, y=None):
        """
        show(columns=None, view=None, x=None, y=None)

        Visualize the SFrame with GraphLab Create :mod:`~graphlab.canvas`. This function
        starts Canvas if it is not already running. If the SFrame has already been plotted,
        this function will update the plot.

        Parameters
        ----------
        view : str, optional
            The name of the SFrame view to show. Can be one of:

            - None: Use the default (depends on which Canvas target is set).
            - 'Table': Show a scrollable, tabular view of the data in the
              SFrame.
            - 'Summary': Show a list of columns with some summary statistics
              and plots for each column.
            - 'Scatter Plot': Show a scatter plot of two numeric columns.
            - 'Heat Map': Show a heat map of two numeric columns.
            - 'Bar Chart': Show a bar chart of one numeric and one categorical
              column.
            - 'BoxWhisker Plot': Show a box and whisker plot of one numeric and one
              categorical column.
            - 'Line Chart': Show a line chart of one numeric and one
              categorical column.

        x : str, optional
            The column to use for the X axis in a Scatter Plot, Heat Map, Bar
            Chart, or Line Chart view. Must be the name of one of the columns
            in this SFrame. For Scatter Plot and Heat Map, the column must be
            numeric (int or float). If not set, defaults to the first available
            valid column.

        y : str, optional
            The column to use for the Y axis in a Scatter Plot, Heat Map, Bar
            Chart, or Line Chart view. Must be the name of one of the numeric
            columns in this SFrame. If not set, defaults to the second
            available numeric column.

        Returns
        -------
        view : graphlab.canvas.view.View
            An object representing the GraphLab Canvas view.

        See Also
        --------
        canvas

        Examples
        --------
        Suppose 'sf' is an SFrame, we can view it in GraphLab Canvas using:

        >>> sf.show()

        To choose a specific view of the SFrame:

        >>> sf.show(view="Summary")
        >>> sf.show(view="Table")
        >>> sf.show(view="Bar Chart", x="col1", y="col2")
        >>> sf.show(view="Line Chart", x="col1", y="col2")
        >>> sf.show(view="Scatter Plot", x="col1", y="col2")
        >>> sf.show(view="Heat Map", x="col1", y="col2")
        """
        if columns is not None:
            __LOGGER__.warn("Column selection for SFrame.show is deprecated. To show only certain columns, use the sf[['column1', 'column2']] syntax or construct a new SFrame with the desired columns.")
        from ..visualization.show import show
        show(self, view=view, x=x, y=y)

    def pack_columns(self, columns=None, column_prefix=None, dtype=list,
                     fill_na=None, remove_prefix=True, new_column_name=None):
        """
        Pack columns of the current SFrame into one single column. The result
        is a new SFrame with the unaffected columns from the original SFrame
        plus the newly created column.

        The list of columns that are packed is chosen through either the
        ``columns`` or ``column_prefix`` parameter. Only one of the parameters
        is allowed to be provided. ``columns`` explicitly specifies the list of
        columns to pack, while ``column_prefix`` specifies that all columns that
        have the given prefix are to be packed.

        The type of the resulting column is decided by the ``dtype`` parameter.
        Allowed values for ``dtype`` are dict, array.array and list:

         - *dict*: pack to a dictionary SArray where column name becomes
           dictionary key and column value becomes dictionary value

         - *array.array*: pack all values from the packing columns into an array

         - *list*: pack all values from the packing columns into a list.

        Parameters
        ----------
        columns : list[str], optional
            A list of column names to be packed.  If omitted and
            `column_prefix` is not specified, all columns from current SFrame
            are packed.  This parameter is mutually exclusive with the
            `column_prefix` parameter.

        column_prefix : str, optional
            Pack all columns with the given `column_prefix`.
            This parameter is mutually exclusive with the `columns` parameter.

        dtype : dict | array.array | list, optional
            The resulting packed column type. If not provided, dtype is list.

        fill_na : value, optional
            Value to fill into packed column if missing value is encountered.
            If packing to dictionary, `fill_na` is only applicable to dictionary
            values; missing keys are not replaced.

        remove_prefix : bool, optional
            If True and `column_prefix` is specified, the dictionary key will
            be constructed by removing the prefix from the column name.
            This option is only applicable when packing to dict type.

        new_column_name : str, optional
            Packed column name.  If not given and `column_prefix` is given,
            then the prefix will be used as the new column name, otherwise name
            is generated automatically.

        Returns
        -------
        out : SFrame
            An SFrame that contains columns that are not packed, plus the newly
            packed column.

        See Also
        --------
        unpack

        Notes
        -----
        - If packing to dictionary, missing key is always dropped. Missing
          values are dropped if fill_na is not provided, otherwise, missing
          value is replaced by 'fill_na'. If packing to list or array, missing
          values will be kept. If 'fill_na' is provided, the missing value is
          replaced with 'fill_na' value.

        Examples
        --------
        Suppose 'sf' is an an SFrame that maintains business category
        information:

        >>> sf = graphlab.SFrame({'business': range(1, 5),
        ...                       'category.retail': [1, None, 1, None],
        ...                       'category.food': [1, 1, None, None],
        ...                       'category.service': [None, 1, 1, None],
        ...                       'category.shop': [1, 1, None, 1]})
        >>> sf
        +----------+-----------------+---------------+------------------+---------------+
        | business | category.retail | category.food | category.service | category.shop |
        +----------+-----------------+---------------+------------------+---------------+
        |    1     |        1        |       1       |       None       |       1       |
        |    2     |       None      |       1       |        1         |       1       |
        |    3     |        1        |      None     |        1         |      None     |
        |    4     |       None      |       1       |       None       |       1       |
        +----------+-----------------+---------------+------------------+---------------+
        [4 rows x 5 columns]

        To pack all category columns into a list:

        >>> sf.pack_columns(column_prefix='category')
        +----------+--------------------+
        | business |         X2         |
        +----------+--------------------+
        |    1     |  [1, 1, None, 1]   |
        |    2     |  [None, 1, 1, 1]   |
        |    3     | [1, None, 1, None] |
        |    4     | [None, 1, None, 1] |
        +----------+--------------------+
        [4 rows x 2 columns]

        To pack all category columns into a dictionary, with new column name:

        >>> sf.pack_columns(column_prefix='category', dtype=dict,
        ...                 new_column_name='category')
        +----------+--------------------------------+
        | business |            category            |
        +----------+--------------------------------+
        |    1     | {'food': 1, 'shop': 1, 're ... |
        |    2     | {'food': 1, 'shop': 1, 'se ... |
        |    3     |  {'retail': 1, 'service': 1}   |
        |    4     |     {'food': 1, 'shop': 1}     |
        +----------+--------------------------------+
        [4 rows x 2 columns]

        To keep column prefix in the resulting dict key:

        >>> sf.pack_columns(column_prefix='category', dtype=dict,
                            remove_prefix=False)
        +----------+--------------------------------+
        | business |               X2               |
        +----------+--------------------------------+
        |    1     | {'category.retail': 1, 'ca ... |
        |    2     | {'category.food': 1, 'cate ... |
        |    3     | {'category.retail': 1, 'ca ... |
        |    4     | {'category.food': 1, 'cate ... |
        +----------+--------------------------------+
        [4 rows x 2 columns]

        To explicitly pack a set of columns:

        >>> sf.pack_columns(columns = ['business', 'category.retail',
                                       'category.food', 'category.service',
                                       'category.shop'])
        +-----------------------+
        |           X1          |
        +-----------------------+
        |   [1, 1, 1, None, 1]  |
        |   [2, None, 1, 1, 1]  |
        | [3, 1, None, 1, None] |
        | [4, None, 1, None, 1] |
        +-----------------------+
        [4 rows x 1 columns]

        To pack all columns with name starting with 'category' into an array
        type, and with missing value replaced with 0:

        >>> sf.pack_columns(column_prefix="category", dtype=array.array,
        ...                 fill_na=0)
        +----------+--------------------------------+
        | business |               X2               |
        +----------+--------------------------------+
        |    1     | array('d', [1.0, 1.0, 0.0, ... |
        |    2     | array('d', [0.0, 1.0, 1.0, ... |
        |    3     | array('d', [1.0, 0.0, 1.0, ... |
        |    4     | array('d', [0.0, 1.0, 0.0, ... |
        +----------+--------------------------------+
        [4 rows x 2 columns]
        """
        if columns != None and column_prefix != None:
            raise ValueError("'columns' and 'column_prefix' parameter cannot be given at the same time.")

        if new_column_name == None and column_prefix != None:
            new_column_name = column_prefix

        if column_prefix != None:
            if type(column_prefix) != str:
                raise TypeError("'column_prefix' must be a string")
            columns = [name for name in self.column_names() if name.startswith(column_prefix)]
            if len(columns) == 0:
                raise ValueError("There is no column starts with prefix '" + column_prefix + "'")
        elif columns == None:
            columns = self.column_names()
        else:
            if not hasattr(columns, '__iter__'):
                raise TypeError("columns must be an iterable type")

            column_names = set(self.column_names())
            for column in columns:
                if (column not in column_names):
                    raise ValueError("Current SFrame has no column called '" + str(column) + "'.")

            # check duplicate names
            if len(set(columns)) != len(columns):
                raise ValueError("There is duplicate column names in columns parameter")

        if (dtype not in (dict, list, array.array)):
            raise ValueError("Resulting dtype has to be one of dict/array.array/list type")

        # fill_na value for array needs to be numeric
        if dtype == array.array:
            if (fill_na != None) and (type(fill_na) not in (int, float)):
                raise ValueError("fill_na value for array needs to be numeric type")
            # all columns have to be numeric type
            for column in columns:
                if self[column].dtype() not in (int, float):
                    raise TypeError("Column '" + column + "' type is not numeric, cannot pack into array type")

        # generate dict key names if pack to dictionary
        # we try to be smart here
        # if all column names are like: a.b, a.c, a.d,...
        # we then use "b", "c", "d", etc as the dictionary key during packing
        if (dtype == dict) and (column_prefix != None) and (remove_prefix == True):
            size_prefix = len(column_prefix)
            first_char = set([c[size_prefix:size_prefix+1] for c in columns])
            if ((len(first_char) == 1) and first_char.pop() in ['.','-','_']):
                dict_keys = [name[size_prefix+1:] for name in columns]
            else:
                dict_keys = [name[size_prefix:] for name in columns]

        else:
            dict_keys = columns

        rest_columns = [name for name in self.column_names() if name not in columns]
        if new_column_name != None:
            if type(new_column_name) != str:
                raise TypeError("'new_column_name' has to be a string")
            if new_column_name in rest_columns:
                raise KeyError("Current SFrame already contains a column name " + new_column_name)
        else:
            new_column_name = ""

        _mt._get_metric_tracker().track('sframe.pack_columns')

        ret_sa = None
        with cython_context():
            ret_sa = SArray(_proxy=self.__proxy__.pack_columns(columns, dict_keys, dtype, fill_na))

        new_sf = self.select_columns(rest_columns)
        new_sf.add_column(ret_sa, new_column_name)
        return new_sf


    def split_datetime(self, expand_column, column_name_prefix=None, limit=None, tzone=False):
        """
        Splits a datetime column of SFrame to multiple columns, with each value in a
        separate column. Returns a new SFrame with the expanded column replaced with
        a list of new columns. The expanded column must be of datetime type.

        For more details regarding name generation and
        other, refer to :py:func:`graphlab.SArray.split_datetime()`

        Parameters
        ----------
        expand_column : str
            Name of the unpacked column.

        column_name_prefix : str, optional
            If provided, expanded column names would start with the given prefix.
            If not provided, the default value is the name of the expanded column.

        limit: list[str], optional
            Limits the set of datetime elements to expand.
            Possible values are 'year','month','day','hour','minute','second',
            'weekday', 'isoweekday', 'tmweekday', and 'us'.
            If not provided, only ['year','month','day','hour','minute','second']
            are expanded.

        tzone : bool, optional
            A boolean parameter that determines whether to show the timezone
            column or not. Defaults to False.

        Returns
        -------
        out : SFrame
            A new SFrame that contains rest of columns from original SFrame with
            the given column replaced with a collection of expanded columns.

        Examples
        ---------

        >>> sf
        Columns:
            id   int
            submission  datetime
        Rows: 2
        Data:
            +----+-------------------------------------------------+
            | id |               submission                        |
            +----+-------------------------------------------------+
            | 1  | datetime(2011, 1, 21, 7, 17, 21, tzinfo=GMT(+1))|
            | 2  | datetime(2011, 1, 21, 5, 43, 21, tzinfo=GMT(+1))|
            +----+-------------------------------------------------+

        >>> sf.split_datetime('submission',limit=['hour','minute'])
        Columns:
            id  int
            submission.hour int
            submission.minute int
        Rows: 2
        Data:
        +----+-----------------+-------------------+
        | id | submission.hour | submission.minute |
        +----+-----------------+-------------------+
        | 1  |        7        |        17         |
        | 2  |        5        |        43         |
        +----+-----------------+-------------------+
        """
        if expand_column not in self.column_names():
            raise KeyError("column '" + expand_column + "' does not exist in current SFrame")

        if column_name_prefix == None:
            column_name_prefix = expand_column

        new_sf = self[expand_column].split_datetime(column_name_prefix, limit, tzone)

        # construct return SFrame, check if there is conflict
        rest_columns =  [name for name in self.column_names() if name != expand_column]
        new_names = new_sf.column_names()
        while set(new_names).intersection(rest_columns):
            new_names = [name + ".1" for name in new_names]
        new_sf.rename(dict(zip(new_sf.column_names(), new_names)))

        _mt._get_metric_tracker().track('sframe.split_datetime')
        ret_sf = self.select_columns(rest_columns)
        ret_sf.add_columns(new_sf)
        return ret_sf

    def unpack(self, unpack_column, column_name_prefix=None, column_types=None,
               na_value=None, limit=None):
        """
        Expand one column of this SFrame to multiple columns with each value in
        a separate column. Returns a new SFrame with the unpacked column
        replaced with a list of new columns.  The column must be of
        list/array/dict type.

        For more details regarding name generation, missing value handling and
        other, refer to the SArray version of
        :py:func:`~graphlab.SArray.unpack()`.

        Parameters
        ----------
        unpack_column : str
            Name of the unpacked column

        column_name_prefix : str, optional
            If provided, unpacked column names would start with the given
            prefix. If not provided, default value is the name of the unpacked
            column.

        column_types : [type], optional
            Column types for the unpacked columns.
            If not provided, column types are automatically inferred from first
            100 rows. For array type, default column types are float.  If
            provided, column_types also restricts how many columns to unpack.

        na_value : flexible_type, optional
            If provided, convert all values that are equal to "na_value" to
            missing value (None).

        limit : list[str] | list[int], optional
            Control unpacking only a subset of list/array/dict value. For
            dictionary SArray, `limit` is a list of dictionary keys to restrict.
            For list/array SArray, `limit` is a list of integers that are
            indexes into the list/array value.

        Returns
        -------
        out : SFrame
            A new SFrame that contains rest of columns from original SFrame with
            the given column replaced with a collection of unpacked columns.

        See Also
        --------
        pack_columns, SArray.unpack

        Examples
        ---------
        >>> sf = graphlab.SFrame({'id': [1,2,3],
        ...                      'wc': [{'a': 1}, {'b': 2}, {'a': 1, 'b': 2}]})
        +----+------------------+
        | id |        wc        |
        +----+------------------+
        | 1  |     {'a': 1}     |
        | 2  |     {'b': 2}     |
        | 3  | {'a': 1, 'b': 2} |
        +----+------------------+
        [3 rows x 2 columns]

        >>> sf.unpack('wc')
        +----+------+------+
        | id | wc.a | wc.b |
        +----+------+------+
        | 1  |  1   | None |
        | 2  | None |  2   |
        | 3  |  1   |  2   |
        +----+------+------+
        [3 rows x 3 columns]

        To not have prefix in the generated column name:

        >>> sf.unpack('wc', column_name_prefix="")
        +----+------+------+
        | id |  a   |  b   |
        +----+------+------+
        | 1  |  1   | None |
        | 2  | None |  2   |
        | 3  |  1   |  2   |
        +----+------+------+
        [3 rows x 3 columns]

        To limit subset of keys to unpack:

        >>> sf.unpack('wc', limit=['b'])
        +----+------+
        | id | wc.b |
        +----+------+
        | 1  | None |
        | 2  |  2   |
        | 3  |  2   |
        +----+------+
        [3 rows x 3 columns]

        To unpack an array column:

        >>> sf = graphlab.SFrame({'id': [1,2,3],
        ...                       'friends': [array.array('d', [1.0, 2.0, 3.0]),
        ...                                   array.array('d', [2.0, 3.0, 4.0]),
        ...                                   array.array('d', [3.0, 4.0, 5.0])]})
        >>> sf
        +----+-----------------------------+
        | id |            friends          |
        +----+-----------------------------+
        | 1  | array('d', [1.0, 2.0, 3.0]) |
        | 2  | array('d', [2.0, 3.0, 4.0]) |
        | 3  | array('d', [3.0, 4.0, 5.0]) |
        +----+-----------------------------+
        [3 rows x 2 columns]

        >>> sf.unpack('friends')
        +----+-----------+-----------+-----------+
        | id | friends.0 | friends.1 | friends.2 |
        +----+-----------+-----------+-----------+
        | 1  |    1.0    |    2.0    |    3.0    |
        | 2  |    2.0    |    3.0    |    4.0    |
        | 3  |    3.0    |    4.0    |    5.0    |
        +----+-----------+-----------+-----------+
        [3 rows x 4 columns]
        """
        if unpack_column not in self.column_names():
            raise KeyError("column '" + unpack_column + "' does not exist in current SFrame")

        if column_name_prefix == None:
            column_name_prefix = unpack_column

        new_sf = self[unpack_column].unpack(column_name_prefix, column_types, na_value, limit)

        # construct return SFrame, check if there is conflict
        rest_columns =  [name for name in self.column_names() if name != unpack_column]
        new_names = new_sf.column_names()
        while set(new_names).intersection(rest_columns):
            new_names = [name + ".1" for name in new_names]
        new_sf.rename(dict(zip(new_sf.column_names(), new_names)))

        _mt._get_metric_tracker().track('sframe.unpack')
        ret_sf = self.select_columns(rest_columns)
        ret_sf.add_columns(new_sf)
        return ret_sf

    def stack(self, column_name, new_column_name=None, drop_na=False, new_column_type=None):
        """
        Convert a "wide" column of an SFrame to one or two "tall" columns by
        stacking all values.

        The stack works only for columns of dict, list, or array type.  If the
        column is dict type, two new columns are created as a result of
        stacking: one column holds the key and another column holds the value.
        The rest of the columns are repeated for each key/value pair.

        If the column is array or list type, one new column is created as a
        result of stacking. With each row holds one element of the array or list
        value, and the rest columns from the same original row repeated.

        The new SFrame includes the newly created column and all columns other
        than the one that is stacked.

        Parameters
        --------------
        column_name : str
            The column to stack. This column must be of dict/list/array type

        new_column_name : str | list of str, optional
            The new column name(s). If original column is list/array type,
            new_column_name must a string. If original column is dict type,
            new_column_name must be a list of two strings. If not given, column
            names are generated automatically.

        drop_na : boolean, optional
            If True, missing values and empty list/array/dict are all dropped
            from the resulting column(s). If False, missing values are
            maintained in stacked column(s).

        new_column_type : type | list of types, optional
            The new column types. If original column is a list/array type
            new_column_type must be a single type, or a list of one type. If
            original column is of dict type, new_column_type must be a list of
            two types. If not provided, the types are automatically inferred
            from the first 100 values of the SFrame.

        Returns
        -------
        out : SFrame
            A new SFrame that contains newly stacked column(s) plus columns in
            original SFrame other than the stacked column.

        See Also
        --------
        unstack

        Examples
        ---------
        Suppose 'sf' is an SFrame that contains a column of dict type:

        >>> sf = graphlab.SFrame({'topic':[1,2,3,4],
        ...                       'words': [{'a':3, 'cat':2},
        ...                                 {'a':1, 'the':2},
        ...                                 {'the':1, 'dog':3},
        ...                                 {}]
        ...                      })
        +-------+----------------------+
        | topic |        words         |
        +-------+----------------------+
        |   1   |  {'a': 3, 'cat': 2}  |
        |   2   |  {'a': 1, 'the': 2}  |
        |   3   | {'the': 1, 'dog': 3} |
        |   4   |          {}          |
        +-------+----------------------+
        [4 rows x 2 columns]

        Stack would stack all keys in one column and all values in another
        column:

        >>> sf.stack('words', new_column_name=['word', 'count'])
        +-------+------+-------+
        | topic | word | count |
        +-------+------+-------+
        |   1   |  a   |   3   |
        |   1   | cat  |   2   |
        |   2   |  a   |   1   |
        |   2   | the  |   2   |
        |   3   | the  |   1   |
        |   3   | dog  |   3   |
        |   4   | None |  None |
        +-------+------+-------+
        [7 rows x 3 columns]

        Observe that since topic 4 had no words, an empty row is inserted.
        To drop that row, set dropna=True in the parameters to stack.

        Suppose 'sf' is an SFrame that contains a user and his/her friends,
        where 'friends' columns is an array type. Stack on 'friends' column
        would create a user/friend list for each user/friend pair:

        >>> sf = graphlab.SFrame({'topic':[1,2,3],
        ...                       'friends':[[2,3,4], [5,6],
        ...                                  [4,5,10,None]]
        ...                      })
        >>> sf
        +-------+------------------+
        | topic |     friends      |
        +-------+------------------+
        |  1    |     [2, 3, 4]    |
        |  2    |      [5, 6]      |
        |  3    | [4, 5, 10, None] |
        +----- -+------------------+
        [3 rows x 2 columns]

        >>> sf.stack('friends', new_column_name='friend')
        +------+--------+
        | user | friend |
        +------+--------+
        |  1   |  2     |
        |  1   |  3     |
        |  1   |  4     |
        |  2   |  5     |
        |  2   |  6     |
        |  3   |  4     |
        |  3   |  5     |
        |  3   |  10    |
        |  3   |  None  |
        +------+--------+
        [9 rows x 2 columns]
        """
        # validate column_name
        column_name = str(column_name)
        if column_name not in self.column_names():
            raise ValueError("Cannot find column '" + str(column_name) + "' in the SFrame.")

        stack_column_type =  self[column_name].dtype()
        if (stack_column_type not in [dict, array.array, list]):
            raise TypeError("Stack is only supported for column of dict/list/array type.")

        # user defined types. do some checking
        if new_column_type != None:
            # if new_column_type is a single type, just make it a list of one type
            if type(new_column_type) is type:
                new_column_type = [new_column_type]

            if (stack_column_type in [list, array.array]) and len(new_column_type) != 1:
                raise ValueError("Expecting a single column type to unpack list or array columns")

            if (stack_column_type in [dict]) and len(new_column_type) != 2:
                raise ValueError("Expecting two column types to unpack a dict column")

        if (new_column_name != None):
            if stack_column_type == dict:
                if (type(new_column_name) is not list):
                    raise TypeError("new_column_name has to be a list to stack dict type")
                elif (len(new_column_name) != 2):
                    raise TypeError("new_column_name must have length of two")
            else:
                if (type(new_column_name) != str):
                    raise TypeError("new_column_name has to be a str")
                new_column_name = [new_column_name]

            # check if the new column name conflicts with existing ones
            for name in new_column_name:
                if (name in self.column_names()) and (name != column_name):
                    raise ValueError("Column with name '" + name + "' already exists, pick a new column name")
        else:
            if stack_column_type == dict:
                new_column_name = ["",""]
            else:
                new_column_name = [""]

        # infer column types
        head_row = SArray(self[column_name].head(100)).dropna()
        if (len(head_row) == 0):
            raise ValueError("Cannot infer column type because there is not enough rows to infer value")

        if new_column_type == None:
            # we have to perform type inference
            if stack_column_type == dict:
                # infer key/value type
                keys = []; values = []
                for row in head_row:
                    for val in row:
                        keys.append(val)
                        if val != None: values.append(row[val])

                new_column_type = [
                    infer_type_of_list(keys),
                    infer_type_of_list(values)
                ]
            else:
                values = [v for v in itertools.chain.from_iterable(head_row)]
                new_column_type = [infer_type_of_list(values)]

        _mt._get_metric_tracker().track('sframe.stack')

        with cython_context():
            return SFrame(_proxy=self.__proxy__.stack(column_name, new_column_name, new_column_type, drop_na))

    def unstack(self, column, new_column_name=None):
        """
        Concatenate values from one or two columns into one column, grouping by
        all other columns. The resulting column could be of type list, array or
        dictionary.  If ``column`` is a numeric column, the result will be of
        array.array type.  If ``column`` is a non-numeric column, the new column
        will be of list type. If ``column`` is a list of two columns, the new
        column will be of dict type where the keys are taken from the first
        column in the list.

        Parameters
        ----------
        column : str | [str, str]
            The column(s) that is(are) to be concatenated.
            If str, then collapsed column type is either array or list.
            If [str, str], then collapsed column type is dict

        new_column_name : str, optional
            New column name. If not given, a name is generated automatically.

        Returns
        -------
        out : SFrame
            A new SFrame containing the grouped columns as well as the new
            column.

        See Also
        --------
        stack : The inverse of unstack.

        groupby : ``unstack`` is a special version of ``groupby`` that uses the
          :mod:`~graphlab.aggregate.CONCAT` aggregator

        Notes
        -----
        - There is no guarantee the resulting SFrame maintains the same order as
          the original SFrame.

        - Missing values are maintained during unstack.

        - When unstacking into a dictionary, if there is more than one instance
          of a given key for a particular group, an arbitrary value is selected.

        Examples
        --------
        >>> sf = graphlab.SFrame({'count':[4, 2, 1, 1, 2, None],
        ...                       'topic':['cat', 'cat', 'dog', 'elephant', 'elephant', 'fish'],
        ...                       'word':['a', 'c', 'c', 'a', 'b', None]})
        >>> sf.unstack(column=['word', 'count'], new_column_name='words')
        +----------+------------------+
        |  topic   |      words       |
        +----------+------------------+
        | elephant | {'a': 1, 'b': 2} |
        |   dog    |     {'c': 1}     |
        |   cat    | {'a': 4, 'c': 2} |
        |   fish   |       None       |
        +----------+------------------+
        [4 rows x 2 columns]

        >>> sf = graphlab.SFrame({'friend': [2, 3, 4, 5, 6, 4, 5, 2, 3],
        ...                      'user': [1, 1, 1, 2, 2, 2, 3, 4, 4]})
        >>> sf.unstack('friend', new_column_name='friends')
        +------+-----------------------------+
        | user |           friends           |
        +------+-----------------------------+
        |  3   |      array('d', [5.0])      |
        |  1   | array('d', [2.0, 4.0, 3.0]) |
        |  2   | array('d', [5.0, 6.0, 4.0]) |
        |  4   |    array('d', [2.0, 3.0])   |
        +------+-----------------------------+
        [4 rows x 2 columns]
        """
        if (type(column) != str and len(column) != 2):
            raise TypeError("'column' parameter has to be either a string or a list of two strings.")

        _mt._get_metric_tracker().track('sframe.unstack')

        with cython_context():
            if type(column) == str:
                key_columns = [i for i in self.column_names() if i != column]
                if new_column_name != None:
                    return self.groupby(key_columns, {new_column_name : aggregate.CONCAT(column)})
                else:
                    return self.groupby(key_columns, aggregate.CONCAT(column))
            elif len(column) == 2:
                key_columns = [i for i in self.column_names() if i not in column]
                if new_column_name != None:
                    return self.groupby(key_columns, {new_column_name: aggregate.CONCAT(column[0], column[1])})
                else:
                    return self.groupby(key_columns, aggregate.CONCAT(column[0], column[1]))

    def unique(self):
        """
        Remove duplicate rows of the SFrame. Will not necessarily preserve the
        order of the given SFrame in the new SFrame.

        Returns
        -------
        out : SFrame
            A new SFrame that contains the unique rows of the current SFrame.

        Raises
        ------
        TypeError
          If any column in the SFrame is a dictionary type.

        See Also
        --------
        SArray.unique

        Examples
        --------
        >>> sf = graphlab.SFrame({'id':[1,2,3,3,4], 'value':[1,2,3,3,4]})
        >>> sf
        +----+-------+
        | id | value |
        +----+-------+
        | 1  |   1   |
        | 2  |   2   |
        | 3  |   3   |
        | 3  |   3   |
        | 4  |   4   |
        +----+-------+
        [5 rows x 2 columns]

        >>> sf.unique()
        +----+-------+
        | id | value |
        +----+-------+
        | 2  |   2   |
        | 4  |   4   |
        | 3  |   3   |
        | 1  |   1   |
        +----+-------+
        [4 rows x 2 columns]
        """
        return self.groupby(self.column_names(),{})

    def sort(self, sort_columns, ascending=True):
        """
        Sort current SFrame by the given columns, using the given sort order.
        Only columns that are type of str, int and float can be sorted.

        Parameters
        ----------
        sort_columns : str | list of str | list of (str, bool) pairs
            Names of columns to be sorted.  The result will be sorted first by
            first column, followed by second column, and so on. All columns will
            be sorted in the same order as governed by the `ascending`
            parameter. To control the sort ordering for each column
            individually, `sort_columns` must be a list of (str, bool) pairs.
            Given this case, the first value is the column name and the second
            value is a boolean indicating whether the sort order is ascending.

        ascending : bool, optional
            Sort all columns in the given order.

        Returns
        -------
        out : SFrame
            A new SFrame that is sorted according to given sort criteria

        See Also
        --------
        topk

        Examples
        --------
        Suppose 'sf' is an sframe that has three columns 'a', 'b', 'c'.
        To sort by column 'a', ascending

        >>> sf = graphlab.SFrame({'a':[1,3,2,1],
        ...                       'b':['a','c','b','b'],
        ...                       'c':['x','y','z','y']})
        >>> sf
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 3 | c | y |
        | 2 | b | z |
        | 1 | b | y |
        +---+---+---+
        [4 rows x 3 columns]

        >>> sf.sort('a')
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 1 | b | y |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a', descending

        >>> sf.sort('a', ascending = False)
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 3 | c | y |
        | 2 | b | z |
        | 1 | a | x |
        | 1 | b | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a' and 'b', all ascending

        >>> sf.sort(['a', 'b'])
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | a | x |
        | 1 | b | y |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]

        To sort by column 'a' ascending, and then by column 'c' descending

        >>> sf.sort([('a', True), ('c', False)])
        +---+---+---+
        | a | b | c |
        +---+---+---+
        | 1 | b | y |
        | 1 | a | x |
        | 2 | b | z |
        | 3 | c | y |
        +---+---+---+
        [4 rows x 3 columns]
        """
        sort_column_names = []
        sort_column_orders = []

        # validate sort_columns
        if (type(sort_columns) == str):
            sort_column_names = [sort_columns]
        elif (type(sort_columns) == list):
            if (len(sort_columns) == 0):
                raise ValueError("Please provide at least one column to sort")

            first_param_types = set([type(i) for i in sort_columns])
            if (len(first_param_types) != 1):
                raise ValueError("sort_columns element are not of the same type")

            first_param_type = first_param_types.pop()
            if (first_param_type == tuple):
                sort_column_names = [i[0] for i in sort_columns]
                sort_column_orders = [i[1] for i in sort_columns]
            elif(first_param_type == str):
                sort_column_names = sort_columns
            else:
                raise TypeError("sort_columns type is not supported")
        else:
            raise TypeError("sort_columns type is not correct. Supported types are str, list of str or list of (str,bool) pair.")

        # use the second parameter if the sort order is not given
        if (len(sort_column_orders) == 0):
            sort_column_orders = [ascending for i in sort_column_names]

        # make sure all column exists
        my_column_names = set(self.column_names())
        for column in sort_column_names:
            if (type(column) != str):
                raise TypeError("Only string parameter can be passed in as column names")
            if (column not in my_column_names):
                raise ValueError("SFrame has no column named: '" + str(column) + "'")
            if (self[column].dtype() not in (str, int, float,datetime.datetime)):
                raise TypeError("Only columns of type (str, int, float) can be sorted")

        _mt._get_metric_tracker().track('sframe.sort')

        with cython_context():
            return SFrame(_proxy=self.__proxy__.sort(sort_column_names, sort_column_orders))

    def dropna(self, columns=None, how='any'):
        """
        Remove missing values from an SFrame. A missing value is either ``None``
        or ``NaN``.  If ``how`` is 'any', a row will be removed if any of the
        columns in the ``columns`` parameter contains at least one missing
        value.  If ``how`` is 'all', a row will be removed if all of the columns
        in the ``columns`` parameter are missing values.

        If the ``columns`` parameter is not specified, the default is to
        consider all columns when searching for missing values.

        Parameters
        ----------
        columns : list or str, optional
            The columns to use when looking for missing values. By default, all
            columns are used.

        how : {'any', 'all'}, optional
            Specifies whether a row should be dropped if at least one column
            has missing values, or if all columns have missing values.  'any' is
            default.

        Returns
        -------
        out : SFrame
            SFrame with missing values removed (according to the given rules).

        See Also
        --------
        dropna_split :  Drops missing rows from the SFrame and returns them.

        Examples
        --------
        Drop all missing values.

        >>> sf = graphlab.SFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> sf.dropna()
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]

        Drop rows where every value is missing.

        >>> sf.dropna(any="all")
        +------+---+
        |  a   | b |
        +------+---+
        |  1   | a |
        | None | b |
        +------+---+
        [2 rows x 2 columns]

        Drop rows where column 'a' has a missing value.

        >>> sf.dropna('a', any="all")
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]
        """
        _mt._get_metric_tracker().track('sframe.dropna')

        # If the user gives me an empty list (the indicator to use all columns)
        # NA values being dropped would not be the expected behavior. This
        # is a NOOP, so let's not bother the server
        if type(columns) is list and len(columns) == 0:
            return SFrame(_proxy=self.__proxy__)

        (columns, all_behavior) = self.__dropna_errchk(columns, how)

        with cython_context():
            return SFrame(_proxy=self.__proxy__.drop_missing_values(columns, all_behavior, False))

    def dropna_split(self, columns=None, how='any'):
        """
        Split rows with missing values from this SFrame. This function has the
        same functionality as :py:func:`~graphlab.SFrame.dropna`, but returns a
        tuple of two SFrames.  The first item is the expected output from
        :py:func:`~graphlab.SFrame.dropna`, and the second item contains all the
        rows filtered out by the `dropna` algorithm.

        Parameters
        ----------
        columns : list or str, optional
            The columns to use when looking for missing values. By default, all
            columns are used.

        how : {'any', 'all'}, optional
            Specifies whether a row should be dropped if at least one column
            has missing values, or if all columns have missing values.  'any' is
            default.

        Returns
        -------
        out : (SFrame, SFrame)
            (SFrame with missing values removed,
             SFrame with the removed missing values)

        See Also
        --------
        dropna

        Examples
        --------
        >>> sf = graphlab.SFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> good, bad = sf.dropna_split()
        >>> good
        +---+---+
        | a | b |
        +---+---+
        | 1 | a |
        +---+---+
        [1 rows x 2 columns]

        >>> bad
        +------+------+
        |  a   |  b   |
        +------+------+
        | None |  b   |
        | None | None |
        +------+------+
        [2 rows x 2 columns]
        """
        _mt._get_metric_tracker().track('sframe.dropna_split')

        # If the user gives me an empty list (the indicator to use all columns)
        # NA values being dropped would not be the expected behavior. This
        # is a NOOP, so let's not bother the server
        if type(columns) is list and len(columns) == 0:
            return (SFrame(_proxy=self.__proxy__), SFrame())

        (columns, all_behavior) = self.__dropna_errchk(columns, how)

        sframe_tuple = self.__proxy__.drop_missing_values(columns, all_behavior, True)

        if len(sframe_tuple) != 2:
            raise RuntimeError("Did not return two SFrames!")

        with cython_context():
            return (SFrame(_proxy=sframe_tuple[0]), SFrame(_proxy=sframe_tuple[1]))

    def __dropna_errchk(self, columns, how):
        if columns is None:
            # Default behavior is to consider every column, specified to
            # the server by an empty list (to avoid sending all the column
            # in this case, since it is the most common)
            columns = list()
        elif type(columns) is str:
            columns = [columns]
        elif type(columns) is not list:
            raise TypeError("Must give columns as a list, str, or 'None'")
        else:
            # Verify that we are only passing strings in our list
            list_types = set([type(i) for i in columns])
            if (str not in list_types) or (len(list_types) > 1):
                raise TypeError("All columns must be of 'str' type")


        if how not in ['any','all']:
            raise ValueError("Must specify 'any' or 'all'")

        if how == 'all':
            all_behavior = True
        else:
            all_behavior = False

        return (columns, all_behavior)

    def fillna(self, column, value):
        """
        Fill all missing values with a given value in a given column. If the
        ``value`` is not the same type as the values in ``column``, this method
        attempts to convert the value to the original column's type. If this
        fails, an error is raised.

        Parameters
        ----------
        column : str
            The name of the column to modify.

        value : type convertible to SArray's type
            The value used to replace all missing values.

        Returns
        -------
        out : SFrame
            A new SFrame with the specified value in place of missing values.

        See Also
        --------
        dropna

        Examples
        --------
        >>> sf = graphlab.SFrame({'a':[1, None, None],
        ...                       'b':['13.1', '17.2', None]})
        >>> sf = sf.fillna('a', 0)
        >>> sf
        +---+------+
        | a |  b   |
        +---+------+
        | 1 | 13.1 |
        | 0 | 17.2 |
        | 0 | None |
        +---+------+
        [3 rows x 2 columns]
        """
        # Normal error checking
        if type(column) is not str:
            raise TypeError("Must give column name as a str")
        ret = self[self.column_names()]
        ret[column] = ret[column].fillna(value)
        return ret

    def add_row_number(self, column_name='id', start=0):
        """
        Returns a new SFrame with a new column that numbers each row
        sequentially. By default the count starts at 0, but this can be changed
        to a positive or negative number.  The new column will be named with
        the given column name.  An error will be raised if the given column
        name already exists in the SFrame.

        Parameters
        ----------
        column_name : str, optional
            The name of the new column that will hold the row numbers.

        start : int, optional
            The number used to start the row number count.

        Returns
        -------
        out : SFrame
            The new SFrame with a column name

        Notes
        -----
        The range of numbers is constrained by a signed 64-bit integer, so
        beware of overflow if you think the results in the row number column
        will be greater than 9 quintillion.

        Examples
        --------
        >>> sf = graphlab.SFrame({'a': [1, None, None], 'b': ['a', 'b', None]})
        >>> sf.add_row_number()
        +----+------+------+
        | id |  a   |  b   |
        +----+------+------+
        | 0  |  1   |  a   |
        | 1  | None |  b   |
        | 2  | None | None |
        +----+------+------+
        [3 rows x 3 columns]
        """
        _mt._get_metric_tracker().track('sframe.add_row_number')

        if type(column_name) is not str:
            raise TypeError("Must give column_name as strs")

        if type(start) is not int:
            raise TypeError("Must give start as int")

        if column_name in self.column_names():
            raise RuntimeError("Column '" + column_name + "' already exists in the current SFrame")

        the_col = _create_sequential_sarray(self.num_rows(), start)

        # Make sure the row number column is the first column
        new_sf = SFrame()
        new_sf.add_column(the_col, column_name)
        new_sf.add_columns(self)
        return new_sf

    def _group(self, key_columns):
        """
        Left undocumented intentionally.
        """
        gsf = GroupedSFrame(self, key_columns)
        return gsf

    @property
    def shape(self):
        """
        The shape of the SFrame, in a tuple. The first entry is the number of
        rows, the second is the number of columns.

        Examples
        --------
        >>> sf = graphlab.SFrame({'id':[1,2,3], 'val':['A','B','C']})
        >>> sf.shape
        (3, 2)
        """
        return (self.num_rows(), self.num_cols())

    @property
    def __proxy__(self):
        return self._proxy

    @__proxy__.setter
    def __proxy__(self, value):
        assert type(value) is UnitySFrameProxy
        self._cache = None
        self._proxy = value
        self._cache = None
