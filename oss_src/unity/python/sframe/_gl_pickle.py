'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from . import util as _util, toolkits as _toolkits, SFrame as _SFrame, SArray as _SArray, \
    SGraph as _SGraph, load_graph as _load_graph

from .util import _get_aws_credentials as _util_get_aws_credentials, \
    cloudpickle as _cloudpickle, file_util as _file_util

import pickle as _pickle
import uuid as _uuid
import os as _os
import zipfile as _zipfile
import shutil as _shutil
import atexit as _atexit
import glob as _glob

def _get_aws_credentials():
    (key, secret) = _util_get_aws_credentials()
    return {'aws_access_key_id': key, 'aws_secret_access_key': secret}

def _get_temp_filename():
    return _util._make_temp_filename(prefix='gl_pickle_')

def _get_tmp_file_location():
    return _util._make_temp_directory(prefix='gl_pickle_')


def _get_class_from_name(module_name, class_name):
    import importlib

    # load the module, will raise ImportError if module cannot be loaded
    m = importlib.import_module(module_name)

    # get the class, will raise AttributeError if class cannot be found
    c = getattr(m, class_name)
    return c

def _is_gl_pickle_extensible(obj):
    """
    Check if an object has an external serialization prototol. We do so by
    checking if the object has the methods __gl_pickle_load__ and
    __gl_pickle_save__.

    Parameters
    ----------
    obj: An object

    Returns
    ----------
    True (if usable by gl_pickle)

    """
    obj_class = None if not hasattr(obj, '__class__') else obj.__class__
    if obj_class is None:
        return False
    else:
        return hasattr(obj_class, '__gl_pickle_load__') and \
               hasattr(obj_class, '__gl_pickle_save__')

def _get_gl_object_from_persistent_id(type_tag, gl_archive_abs_path):
    """
    (GLPickle Version 1.0).

    Get an object from a persistent ID.

    Parameters
    ----------
    type_tag : The name of the class as saved in the GLPickler.

    gl_archive_abs_path: An absolute path to the archive where the
                         object was saved.

    Returns
    ----------
    object: The deserialized object.

    """
    if type_tag == "SFrame":
        obj = _SFrame(gl_archive_abs_path)
    elif type_tag == "SGraph":
        obj = _load_graph(gl_archive_abs_path)
    elif type_tag == "SArray":
        obj = _SArray(gl_archive_abs_path)
    elif type_tag == "Model":
        from . import load_model as _load_model
        obj = _load_model(gl_archive_abs_path)
    else:
        raise _pickle.UnpicklingError("Pickling Error: Unspported object."
              " Implement the methods __gl_pickle_load__ and"
              " __gl_pickle_save__ to use GLPickle. See the docstrings"
              " for examples.")
    return obj

class GLPickler(_cloudpickle.CloudPickler):

    def _to_abs_path_set(self, l):
        return set([_os.path.abspath(x) for x in l])

    """
    # GLPickle works with:
    #
    # (1) Regular python objects
    # (2) Any object with __gl_pickle_save__ and __gl_pickle_load__
    # (3) Any combination of (1) - (2)

    Examples
    --------

    To pickle a collection of objects into a single file:

    .. sourcecode:: python

        from graphlab.util import gl_pickle
        import graphlab as gl

        obj = {'foo': gl.SFrame([1,2,3]),
               'bar': gl.SArray([1,2,3]),
               'foo-bar': ['foo-and-bar', gl.SFrame()]}

        # Setup the GLPickler
        pickler = gl_pickle.GLPickler(filename = 'foo-bar')
        pickler.dump(obj)

        # The pickler has to be closed to make sure the files get closed.
        pickler.close()

    To unpickle the collection of objects:

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler(filename = 'foo-bar')
        obj = unpickler.load()
        unpickler.close()
        print obj

    The GLPickler needs a temporary working directory to manage GLC objects.
    This temporary working path must be a local path to the file system. It can
    also be a relative path in the filesystem.

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler('foo-bar')
        obj = unpickler.load()
        unpickler.close()
        print obj


    Notes
    --------

    The GLC pickler saves the files into single zip archive with the following
    file layout.

    pickle_file_name: Name of the file in the archive that contains
                      the name of the pickle file.
                      The comment in the ZipFile contains the version number
                      of the GLC pickler used.

    "pickle_file": The pickle file that stores all python objects. For GLC objects
                   the pickle file contains a tuple with (ClassName, relative_path)
                   which stores the name of the GLC object type and a relative
                   path (in the zip archive) which points to the GLC archive
                   root directory.

    "gl_archive_dir_1" : A directory which is the GLC archive for a single
                          object.

     ....

    "gl_archive_dir_N"



    """
    def __init__(self, filename, protocol = -1, min_bytes_to_save = 0):
        """

        Construct a  GLC pickler.

        Parameters
        ----------
        filename  : Name of the file to write to. This file is all you need to pickle
                    all objects (including GLC objects).

        protocol  : Pickle protocol (see pickle docs). Note that all pickle protocols
                    may not be compatable with GLC objects.

        min_bytes_to_save : Cloud pickle option (see cloud pickle docs).

        Returns
        ----------
        GLC pickler.

        """
        # Zipfile
        # --------
        # Version None: GLC 1.2.1
        #
        # Directory:
        # ----------
        # Version 1: GLC 1.4: 1

        VERSION = "1.0"
        self.archive_filename = None
        self.gl_temp_storage_path = _get_tmp_file_location()
        self.gl_object_memo = set()
        self.mark_for_delete = set()

        if _file_util.is_s3_path(filename):
            self.s3_path = filename
            self.hdfs_path = None
        elif _file_util.is_hdfs_path(filename):
            self.s3_path = None
            self.hdfs_path = filename
            self.hadoop_conf_dir = None
        else:
            # Make sure the directory exists.
            filename = _os.path.abspath(
                         _os.path.expanduser(
                           _os.path.expandvars(filename)))
            if not _os.path.exists(filename):
                _os.makedirs(filename)
            elif _os.path.isdir(filename):
                self.mark_for_delete = self._to_abs_path_set(
                             _glob.glob(_os.path.join(filename, "*")))
                self.mark_for_delete -= self._to_abs_path_set(
                        [_os.path.join(filename, 'pickle_archive'),
                         _os.path.join(filename, 'version')])

            elif _os.path.isfile(filename):
               _os.remove(filename)
               _os.makedirs(filename)

            # Create a new directory.
            self.gl_temp_storage_path = filename
            self.s3_path = None
            self.hdfs_path = None
            self.hadoop_conf_dir = None

        # The pickle file where all the Python objects are saved.
        relative_pickle_filename = "pickle_archive"
        pickle_filename = _os.path.join(self.gl_temp_storage_path,
                                        relative_pickle_filename)

        try:
            # Initialize the pickle file with cloud _pickle. Note, cloud pickle
            # takes a file handle for initialization.
            self.file = open(pickle_filename, 'wb')
            _cloudpickle.CloudPickler.__init__(self, self.file, protocol)
        except IOError as err:
            print("GraphLab create pickling error: %s" % err)

        # Write the version number.
        with open(_os.path.join(self.gl_temp_storage_path, 'version'), 'w') as f:
            f.write(VERSION)

    def _set_hdfs_exec_dir(self, exec_dir):
        self.hdfs_exec_dir= exec_dir

    def dump(self, obj):
        _cloudpickle.CloudPickler.dump(self, obj)

    def persistent_id(self, obj):
        """
        Provide a persistant ID for "saving" GLC objects by reference. Return
        None for all non GLC objects.

        Parameters
        ----------

        obj: Name of the object whose persistant ID is extracted.

        Returns
        -------
        None if the object is not a GLC object. (ClassName, relative path)
        if the object is a GLC object.

        Examples
        --------
        For the benefit of object persistence, the pickle module supports the
        notion of a reference to an object outside the pickled data stream. To
        pickle objects that have an external persistent id, the pickler must
        have a custom persistent_id() method that takes an object as an
        argument and returns either None or the persistent id for that object.

        For extended objects, the persistent_id is merely a relative file path
        (within the ZIP archive) to the archive where the object is saved. For
        example:

            (load_sframe, 'sframe-save-path')
            (load_sgraph, 'sgraph-save-path')
            (load_model, 'model-save-path')

        To extend your object to work with gl_pickle you need to implement two
        simple functions __gl_pickle_load__ and __gl_pickle_save__.
          (1) __gl_pickle_save__: A member method to save your object to a
          filepath (not file handle) given.
          (2) __gl_pickle_load__: A static method that lets you load your object
          from a filepath (not file handle).

        A simple example is provided below:

        .. sourcecode:: python

            class SampleClass(object):
                def __init__(self, member):
                   self.member = member

                def __gl_pickle_save__(self, filename):
                    with open(filename, 'w') as f:
                        f.write(self.member)

                @classmethod
                def __gl_pickle_load__(cls, filename):
                    with open(filename, 'r') as f:
                        member = f.read().split()
                    return cls(member)

        WARNING: Version 1.0 and before of GLPickle only supported the
        following extended objects.

        - SFrame
        - SGraph
        - Model

        For these objects, the persistent_id was also  a relative file path
        (within the ZIP archive) to the archive where the object is saved. For
        example:

            ("SFrame", 'sframe-save-path')
            ("SGraph", 'sgraph-save-path')
            ("Model", 'model-save-path')

        Note that the key difference between version 1.0 and 2.0 is that 2.0 of
        GLPickle is that version 2.0 saves the load_sframe method while 1.0
        saves the string name for the class (which was hard-coded in)

        References
        ----------
         - Python Pickle Docs(https://docs.python.org/2/library/_pickle.html)
        """
        # If the object is a GL class.
        if _is_gl_pickle_extensible(obj):
            if (id(obj) in self.gl_object_memo):
                # has already been pickled
                return (None, None, id(obj))
            else:
                # Save the location of the object's archive to the pickle file.
                relative_filename = str(_uuid.uuid4())
                filename = _os.path.join(self.gl_temp_storage_path,
                        relative_filename)
                self.mark_for_delete -= set([filename])

                # Save the object
                obj.__gl_pickle_save__(filename)

                # Memoize.
                self.gl_object_memo.add(id(obj))

                # Return the tuple (load_func, relative_filename) in archive.
                return (obj.__gl_pickle_load__, relative_filename, id(obj))

        # Not a GLC object. Default to cloud pickle
        else:
            return None

    def close(self):
        """
        Close the pickle file, and the zip archive file. The single zip archive
        file can now be shipped around to be loaded by the unpickler.
        """
        if self.file is None:
            return

        # Close the pickle file.
        self.file.close()
        self.file = None

        if self.s3_path:
            _file_util.s3_recursive_delete(self.s3_path, \
                    aws_credentials = _get_aws_credentials())
            _file_util.upload_to_s3(self.gl_temp_storage_path, self.s3_path,
                                    aws_credentials = _get_aws_credentials(),
                                    is_dir = True, silent = True)
        if self.hdfs_path:
            _file_util.upload_to_hdfs(self.gl_temp_storage_path,
                                    self.hdfs_path, self.hadoop_conf_dir)

        for f in self.mark_for_delete:
            error = [False]
            def register_error(*args):
                error[0] = True
            _shutil.rmtree(f, onerror = register_error)
            if error[0]:
                _atexit.register(_shutil.rmtree, f, ignore_errors=True)

    def __del__(self):
        self.close()

class GLUnpickler(_pickle.Unpickler):
    """
    # GLC unpickler works with a GLC pickler archive or a regular pickle
    # archive.
    #
    # Works with
    # (1) GLPickler archive
    # (2) Cloudpickle archive
    # (3) Python pickle archive

    Examples
    --------
    To unpickle the collection of objects:

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler('foo-bar')
        obj = unpickler.load()
        print obj

    """

    def __init__(self, filename):
        """
        Construct a GLC unpickler.

        Parameters
        ----------
        filename  : Name of the file to read from. The file can be a GLC pickle
                    file, a cloud pickle file, or a python pickle file.
        Returns
        ----------
        GLC unpickler.
        """
        self.gl_object_memo = {}
        self.pickle_filename = None
        self.tmp_file = None
        self.file = None
        self.gl_temp_storage_path = _get_tmp_file_location()
        self.version = None

        # GLC 1.3 used Zipfiles for storing the objects.
        self.directory_mode = True

        if _file_util.is_s3_path(filename):
            self.tmp_file = _get_temp_filename()
            # GLC 1.3 uses zipfiles
            if _file_util._is_valid_s3_key(filename):
                _file_util.download_from_s3(filename, self.tmp_file, \
                        aws_credentials = _get_aws_credentials(), is_dir=False,
                        silent=True)
            # GLC 1.4 uses directories
            else:
                _file_util.download_from_s3(filename, self.tmp_file, \
                        aws_credentials = _get_aws_credentials(), is_dir=True,
                        silent=True)

            filename = self.tmp_file
        elif _file_util.is_hdfs_path(filename):
            self.tmp_file = _get_temp_filename()
            _file_util.download_from_hdfs(filename, self.tmp_file)
            filename = self.tmp_file
        else:
            filename = _os.path.abspath(
                         _os.path.expanduser(
                           _os.path.expandvars(filename)))
            if not _os.path.exists(filename):
                raise IOError('%s is not a valid file name.' % filename)

        # GLC 1.3 Pickle file
        if _zipfile.is_zipfile(filename):
            self.directory_mode = False
            pickle_filename = None

            # Get the pickle file name.
            zf = _zipfile.ZipFile(filename, allowZip64=True)
            for info in zf.infolist():
                if info.filename == 'pickle_file':
                    pickle_filename = zf.read(info.filename).decode()
            if pickle_filename is None:
                raise IOError(("Cannot pickle file of the given format. File"
                        " must be one of (a) GLPickler archive, "
                        "(b) Cloudpickle archive, or (c) python pickle archive."))

            # Extract the zip file.
            try:
                outpath = self.gl_temp_storage_path
                zf.extractall(outpath)
            except IOError as err:
                print("Graphlab pickle extraction error: %s " % err)

            self.pickle_filename = _os.path.join(self.gl_temp_storage_path,
                                                 pickle_filename)

        # GLC Pickle directory mode.
        elif _os.path.isdir(filename):
            self.directory_mode = True
            pickle_filename = _os.path.join(filename, "pickle_archive")
            if not _os.path.exists(pickle_filename):
                raise IOError("Corrupted archive: Missing pickle file %s." \
                                                  % pickle_filename)
            if not _os.path.exists(_os.path.join(filename, "version")):
                raise IOError("Corrupted archive: Missing version file.")
            try:
                version_filename = _os.path.join(filename, "version")
                self.version = open(version_filename).read().strip()
            except:
                raise IOError("Corrupted archive: Corrupted version file.")
            if self.version not in ["1.0"]:
                raise Exception(
                    "Corrupted archive: Version string must be 1.0.")
            self.pickle_filename = pickle_filename
            self.gl_temp_storage_path = _os.path.abspath(filename)

        # Pure pickle file.
        else:
            self.directory_mode = False
            self.pickle_filename = filename

        self.file = open(self.pickle_filename, 'rb')
        _pickle.Unpickler.__init__(self, self.file)


    def persistent_load(self, pid):
        """
        Reconstruct a GLC object using the persistent ID.

        This method should not be used externally. It is required by the
        unpickler super class.

        Parameters
        ----------
        pid      : The persistent ID used in pickle file to save the GLC object.

        Returns
        ----------
        The GLC object.
        """
        if len(pid) == 2:
            # Pre GLC-1.3 release behavior, without memoization
            type_tag, filename = pid
            abs_path = _os.path.join(self.gl_temp_storage_path, filename)
            return  _get_gl_object_from_persistent_id(type_tag, abs_path)
        else:
            # Post GLC-1.3 release behavior, with memoization
            type_tag, filename, object_id = pid
            if object_id in self.gl_object_memo:
                return self.gl_object_memo[object_id]
            else:
                abs_path = _os.path.join(self.gl_temp_storage_path, filename)
                if self.version in ["1.0", None]:
                    if type_tag in ["SFrame", "SGraph", "SArray", "Model"]:
                        obj = _get_gl_object_from_persistent_id(type_tag,
                                abs_path)
                    else:
                        module_name, class_name = type_tag
                        type_class = _get_class_from_name(module_name,
                                class_name)
                        obj = type_class.__gl_pickle_load__(abs_path)
                else:
                    raise Exception(
                      "Unknown version %s: Expected version in [1.0, 2.0]" \
                                   % self.version)
                self.gl_object_memo[object_id] = obj
                return obj

    def close(self):
        """
        Clean up files that were created.
        """
        if self.file:
            self.file.close()
            self.file = None

        # Remove temp file that were downloaded from S3 or HDFS
        # If temp_file is a folder, we do not remove it because we may
        # still need it after the unpickler is disposed
        if self.tmp_file and _os.path.isfile(self.tmp_file):
            _os.remove(self.tmp_file)
            self.tmp_file = None

    def __del__(self):
        """
        Clean up files that were created.
        """
        self.close()
