'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
# 1st set up logging
import logging
from logging.handlers import TimedRotatingFileHandler
import logging.config
import time as _time
import tempfile as _tempfile
import os as _os
from Queue import Queue as queue
from queue_channel import QueueHandler, MutableQueueListener

import urllib as _urllib
import os as _os
import re as _re
from zipfile import ZipFile as _ZipFile
import bz2 as _bz2
import tarfile as _tarfile
import ConfigParser as _ConfigParser
import itertools as _itertools
import uuid as _uuid
import datetime as _datetime
import time as _time
import logging as _logging
import sys as _sys

def _i_am_a_lambda_worker():
    if _re.match(".*lambda_worker.*", _sys.argv[0]) is not None:
        return True
    return False


__LOGGER__ = _logging.getLogger(__name__)
# overuse the same logger so we have one logging config
root_package_name = __import__(__name__.split('.')[0]).__name__
logger = logging.getLogger(root_package_name)
client_log_file = _os.path.join(_tempfile.gettempdir(), (root_package_name + '_client_%d.log' % _time.time()))

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'brief': {
            'format': '[%(levelname)s] %(message)s'
        }
    },
    'handlers': {
        'default': {
            'class': 'logging.StreamHandler',
            'formatter': 'brief'
        },
        'file': {
            'class':'logging.FileHandler',
            'formatter':'standard',
            'filename':client_log_file,
            'encoding': 'UTF-8',
            'delay': 'False',
        }
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'propagate': 'True'
        }
    }
})

# Set module specific log levels
logging.getLogger('librato').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
if _i_am_a_lambda_worker():
    logging.getLogger(root_package_name).setLevel(logging.WARNING)
    logging.getLogger(__name__).setLevel(logging.WARNING)
else:
    logging.getLogger(root_package_name).setLevel(logging.INFO)
    logging.getLogger(__name__).setLevel(logging.INFO)

#amend the logging configuration with a handler streaming to a message queue

q = queue(-1)
ql = MutableQueueListener(q)

qh = QueueHandler(q)
logging.root.addHandler(qh)

ql.start()

def stop_queue_listener():
    ql.stop()

def _attach_log_handler(handler):
    ql.addHandler(handler)

def _detach_log_handler(handler):
    ql.removeHandler(handler)



def _convert_slashes(path):
    """
    Converts all windows-style slashes to unix-style slashes
    """
    return path.replace('\\', '/')

def _get_aws_credentials():
    """
    Returns the values stored in the AWS credential environment variables.
    Returns the value stored in the AWS_ACCESS_KEY_ID environment variable and
    the value stored in the AWS_SECRET_ACCESS_KEY environment variable.

    Returns
    -------
    out : tuple [string]
        The first string of the tuple is the value of the AWS_ACCESS_KEY_ID
        environment variable. The second string of the tuple is the value of the
        AWS_SECRET_ACCESS_KEY environment variable.

    See Also
    --------
    set_credentials

    Examples
    --------
    >>> graphlab.aws.get_credentials()
    ('RBZH792CTQPP7T435BGQ', '7x2hMqplWsLpU/qQCN6xAPKcmWo46TlPJXYTvKcv')
    """

    if (not 'AWS_ACCESS_KEY_ID' in _os.environ):
        raise KeyError('No access key found. Please set the environment variable AWS_ACCESS_KEY_ID, or using graphlab.aws.set_credentials()')
    if (not 'AWS_SECRET_ACCESS_KEY' in _os.environ):
        raise KeyError('No secret key found. Please set the environment variable AWS_SECRET_ACCESS_KEY, or using graphlab.aws.set_credentials()')
    return (_os.environ['AWS_ACCESS_KEY_ID'], _os.environ['AWS_SECRET_ACCESS_KEY'])

def _try_inject_s3_credentials(url):
    """
    Inject aws credentials into s3 url as s3://[aws_id]:[aws_key]:[bucket/][objectkey]

    If s3 url already contains secret key/id pairs, just return as is.
    """
    assert url.startswith('s3://')
    path = url[5:]
    # Check if the path already contains credentials
    tokens = path.split(':')
    # If there are two ':', its possible that we have already injected credentials
    if len(tokens) == 3:
        # Edge case: there are exactly two ':'s in the object key which is a false alarm.
        # We prevent this by checking that '/' is not in the assumed key and id.
        if ('/' not in tokens[0]) and ('/' not in tokens[1]):
            return url

    # S3 url does not contain secret key/id pair, query the environment variables
    (k, v) = _get_aws_credentials()
    return 's3://' + k + ':' + v + ':' + path

def _make_internal_url(url):
    """
    Takes a user input url string and translates into url relative to the server process.
    - URL to a local location begins with "local://" or has no "*://" modifier.
      If the server is local, returns the absolute path of the url.
      For example: "local:///tmp/foo" -> "/tmp/foo" and "./foo" -> os.path.abspath("./foo").
      If the server is not local, raise NotImplementedError.
    - URL to a server location begins with "remote://".
      Returns the absolute path after the "remote://" modifier.
      For example: "remote:///tmp/foo" -> "/tmp/foo".
    - URL to a s3 location begins with "s3://":
      Returns the s3 URL with credentials filled in using graphlab.aws.get_aws_credential().
      For example: "s3://mybucket/foo" -> "s3://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY:mybucket/foo".
    - URL to other remote locations, e.g. http://, will remain as is.
    - Expands ~ to $HOME

    Parameters
    ----------
    string
        A URL (as described above).

    Raises
    ------
    ValueError
        If a bad url is provided.
    """
    if not url:
        raise ValueError('Invalid url: %s' % url)

    # Convert Windows paths to Unix-style slashes
    url = _convert_slashes(url)

    # The final file path on server.
    path_on_server = None

    # Try to split the url into (protocol, path).
    urlsplit = url.split("://")
    from ..connect import main as _glconnect
    from ..connect import server as _server
    from .. import sys_util as _sys_util
    if len(urlsplit) == 2:
        protocol, path = urlsplit
        if not path:
            raise ValueError('Invalid url: %s' % url)
        if protocol in ['http', 'https']:
            # protocol is a remote url not on server, just return
            return url
        elif protocol == 'hdfs':
            if isinstance(_glconnect.get_server(), _server.LocalServer) and not _sys_util.get_hadoop_class_path():
                raise ValueError("HDFS URL is not supported because Hadoop not found. Please make hadoop available from PATH or set the environment variable HADOOP_HOME and try again.")
            else:
                return url
        elif protocol == 's3':
            return _try_inject_s3_credentials(url)
        elif protocol == 'remote':
        # url for files on the server
            path_on_server = path
        elif protocol == 'local':
        # url for files on local client, check if we are connecting to local server
            if (isinstance(_glconnect.get_server(), _server.LocalServer)):
                path_on_server = path
            else:
                raise ValueError('Cannot use local URL when connecting to a remote server.')
        else:
            raise ValueError('Invalid url protocol %s. Supported url protocols are: remote://, local://, s3://, https:// and hdfs://' % protocol)
    elif len(urlsplit) == 1:
        # expand ~ to $HOME
        url = _os.path.expanduser(url)
        # url for files on local client, check if we are connecting to local server
        if (isinstance(_glconnect.get_server(), _server.LocalServer)):
            path_on_server = url
        else:
            raise ValueError('Cannot use local URL when connecting to a remote server.')
    else:
        raise ValueError('Invalid url: %s' % url)

    if path_on_server:
        out_path = _os.path.abspath(_os.path.expanduser(path_on_server))
        return out_path
    else:
        raise ValueError('Invalid url: %s' % url)


def _download_dataset(url_str, extract=True, force=False, output_dir="."):
    """Download a remote dataset and extract the contents.

    Parameters
    ----------

    url_str : string
        The URL to download from

    extract : bool
        If true, tries to extract compressed file (zip/gz/bz2)

    force : bool
        If true, forces to retry the download even if the downloaded file already exists.

    output_dir : string
        The directory to dump the file. Defaults to current directory.
    """
    fname = output_dir + "/" + url_str.split("/")[-1]
    #download the file from the web
    if not _os.path.isfile(fname) or force:
        print "Downloading file from: ", url_str
        _urllib.urlretrieve(url_str, fname)
        if extract and fname[-3:] == "zip":
            print "Decompressing zip archive", fname
            _ZipFile(fname).extractall(output_dir)
        elif extract and fname[-6:] == ".tar.gz":
            print "Decompressing tar.gz archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-7:] == ".tar.bz2":
            print "Decompressing tar.bz2 archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-3:] == "bz2":
            print "Decompressing bz2 archive: ", fname
            outfile = open(fname.split(".bz2")[0], "w")
            print "Output file: ", outfile
            for line in _bz2.BZ2File(fname, "r"):
                outfile.write(line)
            outfile.close()
    else:
        print "File is already downloaded."

def is_directory_archive(path):
    """
    Utiilty function that returns True if the path provided is a directory that has an SFrame or SGraph in it.

    SFrames are written to disk as a directory archive, this function identifies if a given directory is an archive
    for an SFrame.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    True if path provided is an archive location, False otherwise
    """
    if path is None:
        return False

    if not _os.path.isdir(path):
        return False

    ini_path = '/'.join([_convert_slashes(path), 'dir_archive.ini'])

    if not _os.path.exists(ini_path):
        return False

    if _os.path.isfile(ini_path):
        return True

    return False


def get_archive_type(path):
    """
    Returns the contents type for the provided archive path.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    Returns a string of: sframe, sgraph, raises TypeError for anything else
    """
    if not is_directory_archive(path):
        raise TypeError('Unable to determine the type of archive at path: %s' % path)

    try:
        ini_path = '/'.join([_convert_slashes(path), 'dir_archive.ini'])
        parser = _ConfigParser.SafeConfigParser()
        parser.read(ini_path)

        contents = parser.get('metadata', 'contents')
        return contents
    except Exception as e:
        raise TypeError('Unable to determine type of archive for path: %s' % path, e)

def get_environment_config():
    """
    Returns all the GraphLab Create configuration variables that can only
    be set via environment variables.

    - *GRAPHLAB_FILEIO_WRITER_BUFFER_SIZE*
      The file write buffer size.

    - *GRAPHLAB_FILEIO_READER_BUFFER_SIZE*
      The file read buffer size.

    - *OMP_NUM_THREADS*
      The maximum number of threads to use for parallel processing.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    from ..connect import main as _glconnect
    unity = _glconnect.get_unity()
    return unity.list_globals(False)

def get_runtime_config():
    """
    Returns all the GraphLab Create configuration variables that can be set
    at runtime. See :py:func:`graphlab.set_runtime_config()` to set these
    values and for documentation on the effect of each variable.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    from ..connect import main as _glconnect
    unity = _glconnect.get_unity()
    return unity.list_globals(True)

def set_runtime_config(name, value):
    """
    Configures system behavior at runtime. These configuration values are also
    read from environment variables at program startup if available. See
    :py:func:`graphlab.get_runtime_config()` to get the current values for
    each variable.

    Note that defaults may change across versions and the names
    of performance tuning constants may also change as improved algorithms
    are developed and implemented.

    **Basic Configuration Variables**

    - *GRAPHLAB_CACHE_FILE_LOCATIONS*
     The directory in which intermediate SFrames/SArray are stored.
     For instance "/var/tmp".  Multiple directories can be specified separated
     by a colon (ex: "/var/tmp:/tmp") in which case intermediate SFrames will
     be striped across both directories (useful for specifying multiple disks).
     Defaults to /var/tmp if the directory exists, /tmp otherwise.

    - *GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY*
     The maximum amount of memory which will be occupied by *all* intermediate
     SFrames/SArrays. Once this limit is exceeded, SFrames/SArrays will be
     flushed out to temporary storage (as specified by
     `GRAPHLAB_CACHE_FILE_LOCATIONS`). On large systems increasing this as well
     as `GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE` can improve performance
     significantly. Defaults to 2147483648 bytes (2GB).

    - *GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE*
     The maximum amount of memory which will be occupied by any individual
     intermediate SFrame/SArray. Once this limit is exceeded, the
     SFrame/SArray will be flushed out to temporary storage (as specified by
     `GRAPHLAB_CACHE_FILE_LOCATIONS`). On large systems, increasing this as well
     as `GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY` can improve performance
     significantly for large SFrames. Defaults to 134217728 bytes (128MB).

    **SSL Configuration**
    - *GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE*
     The location of an SSL certificate file used to validate HTTPS / S3
     connections. Defaults to the the Python certifi package certificates.

    - *GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_DIR*
     The location of an SSL certificate directory used to validate HTTPS / S3
     connections. Defaults to the operating system certificates.

    - *GRAPHLAB_FILEIO_INSECURE_SSL_CERTIFICATE_CHECKS*
     If set to a non-zero value, disables all SSL certificate validation.
     Defaults to False.

    **ODBC Configuration**

    - *GRAPHLAB_LIBODBC_PREFIX*
     A directory containing libodbc.so. Also see :func:`graphlab.set_libodbc_path`
     and :func:`graphlab.connect_odbc`

    - *GRAPHLAB_ODBC_BUFFER_MAX_ROWS*
     The number of rows to read from ODBC in each batch. Increasing this
     may give better performance at increased memory consumption. Defaults to
     2000.

    - *GRAPHLAB_ODBC_BUFFER_SIZE*
     The maximum ODBC buffer size in bytes when reading. Increasing this may
     give better performance at increased memory consumption. Defaults to 3GB.

    **Sort Performance Configuration**

    - *GRAPHLAB_SFRAME_SORT_PIVOT_ESTIMATION_SAMPLE_SIZE*
     The number of random rows to sample from the SFrame to estimate the
     sort pivots used to partition the sort. Defaults to 2000000.

    - *GRAPHLAB_SFRAME_SORT_BUFFER_SIZE*
     The maximum estimated memory consumption sort is allowed to use. Increasing
     this will increase the size of each sort partition, and will increase
     performance with increased memory consumption. Defaults to 2GB.

    **Join Performance Configuration**

    - *GRAPHLAB_SFRAME_JOIN_BUFFER_NUM_CELLS*
     The maximum number of cells to buffer in memory. Increasing this will
     increase the size of each join partition and will increase performance
     with increased memory consumption.
     If you have very large cells (very long strings for instance),
     decreasing this value will help decrease memory consumption.
     Defaults to 52428800.

    **Groupby Aggregate Performance Configuration**

    - *GRAPHLAB_SFRAME_GROUPBY_BUFFER_NUM_ROWS*
     The number of groupby keys cached in memory. Increasing this will increase
     performance with increased memory consumption. Defaults to 1048576.

    **Advanced Configuration Variables**

    - *GRAPHLAB_SFRAME_FILE_HANDLE_POOL_SIZE*
     The maximum number of file handles to use when reading SFrames/SArrays.
     Once this limit is exceeded, file handles will be recycled, reducing
     performance. This limit should be rarely approached by most SFrame/SArray
     operations. Large SGraphs however may create a large a number of SFrames
     in which case increasing this limit may improve performance (You may
     also need to increase the system file handle limit with "ulimit -n").
     Defaults to 128.

    ----------
    name: A string referring to runtime configuration variable.

    value: The value to set the variable to.

    Returns
    -------
    Nothing

    Raises
    ------
    A RuntimeError if the key does not exist, or if the value cannot be
    changed to the requested value.

    """
    from ..connect import main as _glconnect
    unity = _glconnect.get_unity()
    ret = unity.set_global(name, value)
    if ret != "":
        raise RuntimeError(ret);

_GLOB_RE = _re.compile("""[*?]""")
def _split_path_elements(url):
    parts = _os.path.split(url)
    m = _GLOB_RE.search(parts[-1])
    if m:
        return (parts[0], parts[1])
    else:
        return (url, "")

def crossproduct(d):
    """
    Create an SFrame containing the crossproduct of all provided options.

    Parameters
    ----------
    d : dict
        Each key is the name of an option, and each value is a list
        of the possible values for that option.

    Returns
    -------
    out : SFrame
        There will be a column for each key in the provided dictionary,
        and a row for each unique combination of all values.

    Example
    -------
    settings = {'argument_1':[0, 1],
                'argument_2':['a', 'b', 'c']}
    print crossproduct(settings)
    +------------+------------+
    | argument_2 | argument_1 |
    +------------+------------+
    |     a      |     0      |
    |     a      |     1      |
    |     b      |     0      |
    |     b      |     1      |
    |     c      |     0      |
    |     c      |     1      |
    +------------+------------+
    [6 rows x 2 columns]
    """

    from .. import connect as _mt
    _mt._get_metric_tracker().track('util.crossproduct')
    from .. import SArray
    d = [zip(d.keys(), x) for x in _itertools.product(*d.values())]
    sa = [{k:v for (k,v) in x} for x in d]
    return SArray(sa).unpack(column_name_prefix='')


def get_graphlab_object_type(url):
    '''
    Given url where a GraphLab Create object is persisted, return the GraphLab
    Create object type: 'model', 'graph', 'sframe', or 'sarray'
    '''
    from ..connect import main as _glconnect
    ret = _glconnect.get_unity().get_graphlab_object_type(_make_internal_url(url))

    # to be consistent, we use sgraph instead of graph here
    if ret == 'graph':
        ret = 'sgraph'
    return ret


def _assert_sframe_equal(sf1,
                         sf2,
                         check_column_names=True,
                         check_column_order=True,
                         check_row_order=True):
    """
    Assert the two SFrames are equal.

    The default behavior of this function uses the strictest possible
    definition of equality, where all columns must be in the same order, with
    the same names and have the same data in the same order.  Each of these
    stipulations can be relaxed individually and in concert with another, with
    the exception of `check_column_order` and `check_column_names`, we must use
    one of these to determine which columns to compare with one another.

    This function does not attempt to apply a "close enough" definition to
    float values, and it is not recommended to rely on this function when
    SFrames may have a column of floats.

    Parameters
    ----------
    sf1 : SFrame

    sf2 : SFrame

    check_column_names : bool
        If true, assert if the data values in two columns are the same, but
        they have different names.  If False, column order is used to determine
        which columns to compare.

    check_column_order : bool
        If true, assert if the data values in two columns are the same, but are
        not in the same column position (one is the i-th column and the other
        is the j-th column, i != j).  If False, column names are used to
        determine which columns to compare.

    check_row_order : bool
        If true, assert if all rows in the first SFrame exist in the second
        SFrame, but they are not in the same order.
    """
    from .. import SFrame as _SFrame
    if (type(sf1) is not _SFrame) or (type(sf2) is not _SFrame):
        raise TypeError("Cannot function on types other than SFrames.")

    if not check_column_order and not check_column_names:
        raise ValueError("Cannot ignore both column order and column names.")

    sf1.__materialize__()
    sf2.__materialize__()

    if sf1.num_cols() != sf2.num_cols():
        raise AssertionError("Number of columns mismatched: " +
            str(sf1.num_cols()) + " != " + str(sf2.num_cols()))

    s1_names = sf1.column_names()
    s2_names = sf2.column_names()

    sorted_s1_names = sorted(s1_names)
    sorted_s2_names = sorted(s2_names)

    if check_column_names:
        if (check_column_order and (s1_names != s2_names)) or (sorted_s1_names != sorted_s2_names):
            raise AssertionError("SFrame does not have same column names: " +
                str(sf1.column_names()) + " != " + str(sf2.column_names()))

    if sf1.num_rows() != sf2.num_rows():
        raise AssertionError("Number of rows mismatched: " +
            str(sf1.num_rows()) + " != " + str(sf2.num_rows()))

    if not check_row_order and (sf1.num_rows() > 1):
        sf1 = sf1.sort(s1_names)
        sf2 = sf2.sort(s2_names)

    names_to_check = None
    if check_column_names:
      names_to_check = zip(sorted_s1_names, sorted_s2_names)
    else:
      names_to_check = zip(s1_names, s2_names)
    for i in names_to_check:
        if sf1[i[0]].dtype() != sf2[i[1]].dtype():
          raise AssertionError("Columns " + str(i) + " types mismatched.")
        if not (sf1[i[0]] == sf2[i[1]]).all():
            raise AssertionError("Columns " + str(i) + " are not equal!")

def _get_temp_file_location():
    '''
    Returns user specified temporary file location.
    The temporary location is specified through:

    >>> graphlab.set_runtime_config('GRAPHLAB_CACHE_FILE_LOCATIONS', ...)

    '''
    from ..connect import main as _glconnect
    unity = _glconnect.get_unity()
    cache_dir = _convert_slashes(unity.get_current_cache_file_location())
    if not _os.path.exists(cache_dir):
        _os.makedirs(cache_dir)
    return cache_dir

def _make_temp_directory(prefix):
    '''
    Generate a temporary directory that would not live beyond the lifetime of
    unity_server.

    Caller is expected to clean up the temp file as soon as the directory is no
    longer needed. But the directory will be cleaned as unity_server restarts
    '''
    temp_dir = _make_temp_filename(prefix=str(prefix))
    _os.makedirs(temp_dir)
    return temp_dir

def _make_temp_filename(prefix):
    '''
    Generate a temporary file that would not live beyond the lifetime of
    unity_server.

    Caller is expected to clean up the temp file as soon as the file is no
    longer needed. But temp files created using this method will be cleaned up
    when unity_server restarts
    '''
    temp_location = _get_temp_file_location()
    temp_file_name = '/'.join([temp_location, str(prefix)+str(_uuid.uuid4())])
    return temp_file_name

# datetime utilities

_ZERO = _datetime.timedelta(0)

class _UTC(_datetime.tzinfo):
    """
    A UTC datetime.tzinfo class modeled after the pytz library. It includes a
    __reduce__ method for pickling,
    """
    def fromutc(self, dt):
        if dt.tzinfo is None:
            return self.localize(dt)
        return super(_utc.__class__, self).fromutc(dt)

    def utcoffset(self, dt):
        return _ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _ZERO

    def __reduce__(self):
        return _UTC, ()

    def __repr__(self):
        return "<UTC>"

    def __str__(self):
        return "UTC"

_utc = _UTC()

def _dt_to_utc_timestamp(t):
    if t.tzname() == 'UTC':
        return (t - _datetime.datetime(1970, 1, 1, tzinfo=_utc)).total_seconds()
    elif not t.tzinfo:
        return _time.mktime(t.timetuple())
    else:
        raise ValueError('Only local time and UTC time is supported')


def _pickle_to_temp_location_or_memory(obj):
        '''
        If obj can be serialized directly into memory (via cloudpickle) this
        will return the serialized bytes.
        Otherwise, gl_pickle is attempted and it will then
        generates a temporary directory serializes an object into it, returning
        the directory name. This directory will not have lifespan greater than
        that of unity_server.
        '''
        import cloudpickle as cloudpickle
        try:
            # try cloudpickle first and see if that works
            lambda_str = cloudpickle.dumps(obj)
            return lambda_str
        except:
            pass
        # nope. that does not work! lets try again with gl pickle
        filename = _make_temp_filename('pickle')
        from .. import _gl_pickle
        pickler = _gl_pickle.GLPickler(filename)
        pickler.dump(obj)
        pickler.close()
        return filename

def _raise_error_if_not_of_type(arg, expected_type, arg_name = None):
    """
    Check if the input is of expected type.

    Parameters
    ----------
    arg            : Input argument.

    expected_type  : A type OR a list of types that the argument is expected
                     to be.

    arg_name      : The name of the variable in the function being used. No
                    name is assumed if set to None.

    Examples
    --------
    _raise_error_if_not_of_type(sf, str, 'sf')
    _raise_error_if_not_of_type(sf, [str, int], 'sf')
    """

    display_name = "%s " % arg_name if arg_name is not None else "Argument "
    lst_expected_type = [expected_type] if \
                        type(expected_type) == type else expected_type

    err_msg = "%smust be of type %s " % (display_name,
                        ' or '.join([x.__name__ for x in lst_expected_type]))
    err_msg += "(not %s)." % type(arg).__name__
    if not any(map(lambda x: isinstance(arg, x), lst_expected_type)):
      raise TypeError, err_msg

def _raise_error_if_not_function(arg, arg_name=None):
    """
    Check if the input is of expected type.

    Parameters
    ----------
    arg            : Input argument.

    arg_name      : The name of the variable in the function being used. No
                    name is assumed if set to None.

    Examples
    --------
    _raise_error_if_not_function(func, 'func')

    """


    display_name = '%s ' % arg_name if arg_name is not None else ""
    err_msg = "Argument %smust be a function." % display_name
    if not hasattr(arg, '__call__'):
      raise TypeError, err_msg

def get_log_location():
    from ..connect import main as _glconnect
    server = _glconnect.get_server()
    if hasattr(server, 'unity_log'):
        return server.unity_log
    else:
        return None

def get_client_log_location():
    return client_log_file

def get_server_log_location():
    return get_log_location()

def get_module_from_object(obj):
    mod_str = obj.__class__.__module__.split('.')[0]
    return _sys.modules[mod_str]

def infer_dbapi2_types(cursor, mod_info):
    desc = cursor.description
    result_set_types = [i[1] for i in desc]
    dbapi2_to_python = [ # a type code can match more than one, so ordered by
                         # preference (loop short-circuits when it finds a match
                        (mod_info['DATETIME'], _datetime.datetime),
                        (mod_info['ROWID'],int),
                        (mod_info['NUMBER'],float),
                       ]
    ret_types = []

    # Ugly nested loop because the standard only guarantees that a type code
    # will compare equal to the module-defined types
    for i in result_set_types:
        type_found = False
        for j in dbapi2_to_python:
            if i is None:
                break
            elif i == j[0]:
                ret_types.append(j[1])
                type_found = True
                break 
        if not type_found:
            ret_types.append(str)

    return ret_types

def pytype_to_printf(in_type):
    if in_type == int:
        return 'd'
    elif in_type == float:
        return 'f'
    else:
        return 's'
