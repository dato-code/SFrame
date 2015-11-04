'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

'''

This package provides commonly used methods for dealing with file operation,
including working with network file system like S3, http, etc.

'''
import os
import sys
import time
import boto
import boto.s3
import logging
import shutil
import tempfile
import subprocess
from subprocess import PIPE
from boto.exception import S3ResponseError

__logger__ = logging.getLogger(__name__)

__RETRY_TIMES = 5
__SLEEP_SECONDS_BETWEEN_RETRIES = 2
__GRAPHLAB_S3_USE_BOTO = "GRAPHLAB_UNIT_TEST"

def _use_boto():
    return __GRAPHLAB_S3_USE_BOTO in os.environ and \
        os.environ[__GRAPHLAB_S3_USE_BOTO] == 'PredictiveService'

def get_protocol(path):
    '''Given a path, returns the protocol the path uses

    For example,
      's3://a/b/c/'  returns 's3'
      'http://a/b/c' returns 'http'
      'tmp/a/bc/'    returns ''

    '''
    pos = path.find('://')
    if pos < 0:
        return ''
    return path[0:pos].lower()

def is_path(string):
    if not isinstance(string, str):
        return False
    return is_local_path(string) or is_s3_path(string)

def mkdir(path):
    if is_local_path(path):
        os.makedirs(path)
    elif is_hdfs_path(path):
        hdfs_mkdir(path)
    elif is_s3_path(path):
        # no need to make any directory
        pass
    else:
        raise ValueError('Unsupported protocol %s' % path)

def exists(path, aws_credentials = {}):
    if is_local_path(path):
        return os.path.exists(path)
    elif is_hdfs_path(path):
        return hdfs_test_url(path)
    elif is_s3_path(path):
        return s3_test_url(path, aws_credentials = aws_credentials)
    else:
        raise ValueError('Unsupported protocol %s' % path)

def touch(path):
    if is_local_path(path):
        with open(path, 'a'):
            os.utime(path, None)
    elif is_hdfs_path(path):
        hdfs_touch(path)
    elif is_s3_path(path):
        s3_touch(path)
    else:
        raise ValueError('Unsupported protocol %s' % path)

def read(path):
    if is_local_path(path):
        return open(path).read()
    elif is_hdfs_path(path):
        return read_file_to_string_hdfs(path)
    elif is_s3_path(path):
        return read_file_to_string_s3(path)
    else:
        raise ValueError('Unsupported protocol %s' % path)

def copy_from_local(localpath, remotepath, aws_credentials = {}, is_dir = False, silent = True):
    if is_hdfs_path(remotepath):
        upload_to_hdfs(localpath, remotepath, silent = silent)
    elif is_s3_path(remotepath):
        upload_to_s3(localpath, remotepath, aws_credentials = aws_credentials, is_dir = is_dir, silent = silent)
    elif is_local_path(remotepath):
        if is_dir:
            shutil.copytree(localpath, remotepath)
        else:
            shutil.copy(localpath, remotepath)
    else:
        raise ValueError('Unsupported protocol %s' % remotepath)

def is_local_path(path):
    '''Returns True if the path indicates a local path, otherwise False'''
    protocol = get_protocol(path)
    return protocol != 'hdfs' and protocol != 's3' and \
          protocol != 'http' and protocol != 'https'

def is_s3_path(path):
    '''Returns True if the path indicates a s3 path, otherwise False'''
    protocol = get_protocol(path)
    return protocol == 's3'

def is_hdfs_path(path):
    '''Returns True if the path indicates a s3 path, otherwise False'''
    protocol = get_protocol(path)
    return protocol == 'hdfs'

def upload_to_local(src_path, dst_path, is_dir=False, silent=False):
    '''Copies a file/dir to a local path'''
    if not silent:
        __logger__.info('Uploading local path %s to path: %s' % (src_path, dst_path))

    if not os.path.exists(src_path):
        raise RuntimeError("Cannot find file/path: %s" % src_path)

    if not is_dir and not os.path.isfile(src_path):
        raise RuntimeError("Path %s is not a file" % src_path)

    if is_dir and not os.path.isdir(src_path):
        raise RuntimeError("Path %s is not a directory" % src_path)

    if not is_local_path(dst_path):
        raise RuntimeError("Path %s is not a valid dest path" % dst_path)

    # now upload
    if is_dir:
        shutil.copytree(src_path, dst_path)
    else:
        shutil.copy(src_path, dst_path)

    if not silent:
        __logger__.info("Successfully uploaded to path %s" % dst_path)

def read_file_to_string_s3(s3_path, max_size=None, aws_credentials={}):
    ''' Read a file in s3 to a string
    '''
    if not is_s3_path(s3_path):
        raise RuntimeError("Path %s is not a valid s3 path" % s3_path)

    k = _get_s3_key(s3_path, aws_credentials=aws_credentials)
    if k:
        k.open_read()
        if max_size and long(k.size) > max_size:
            raise RuntimeError("Cannot read file greater than max size %s." % str(max_size))
        return k.get_contents_as_string()
    return None

def list_s3(s3_path, aws_credentials={}):
    ''' List a directory in s3
    '''
    if not is_s3_path(s3_path):
        raise RuntimeError("Path %s is not a valid s3 path" % s3_path)

    (s3_bucket_name, s3_key_prefix) = parse_s3_path(s3_path)
    conn = boto.connect_s3(**aws_credentials)
    bucket = conn.get_bucket(s3_bucket_name)
    # get list of keys with the prefix as the s3_path, in other words,
    # this returns the keys to all the files under this s3_path dir.
    k_list = list(bucket.list(s3_key_prefix))
    key_list = []
    for k in k_list:
        s3_full_path = os.path.join("s3://", bucket.name, k.name)
        key_list.append({"path": s3_full_path, "size": str(k.size)})
    return key_list

def upload_to_s3(local_path, s3_path, is_dir=False, aws_credentials={}, silent=False):
    '''Upload a local file to s3
    '''
    if not silent:
        __logger__.info('Uploading local path %s to s3 path: %s' % (local_path, s3_path))

    if not os.path.exists(local_path):
        raise RuntimeError("Cannot find file: %s" % local_path)

    if not is_dir and not os.path.isfile(local_path):
        raise RuntimeError("Path %s is not a file" % local_path)

    if is_dir and not os.path.isdir(local_path):
        raise RuntimeError("Path %s is not a directory" % local_path)

    if not is_s3_path(s3_path):
        raise RuntimeError("Path %s is not a valid s3 path" % s3_path)

    # now upload
    num_retries = 0
    while num_retries < __RETRY_TIMES:
        try:

            # Use BOTO
            if _use_boto():
                # Relative and absolute paths. S3 paths are relative while
                # file paths on the local machine are not.
                abs_path = os.path.abspath(local_path)

                # Remove files that are already there.
                s3_recursive_delete(s3_path)

                # Get all the files.
                uploadFileNames = {}
                if os.path.isdir(abs_path):
                    for (source_dir, sub_dirs, files) in os.walk(abs_path):
                        for f in files:
                            path = os.path.join(source_dir, f)
                            key = '%s/%s' % (s3_path, os.path.relpath(path, abs_path))
                            uploadFileNames[key] = path
                else:
                    key = s3_path
                    uploadFileNames[key] = abs_path

                # Upload each file
                for key, path in uploadFileNames.items():
                    k = _get_s3_key(key, aws_credentials)
                    __logger__.info("Uploading %s to %s." % (path, key))
                    k.set_contents_from_filename(
                        path,
                        num_cb=10)

            # Use awscli
            else:
                _awscli_s3_op('cp', local_path, s3_path, recursive=is_dir,
                              silent = silent, aws_credentials=aws_credentials)

            # We are done
            if not silent:
                __logger__.info("Successfully uploaded to s3 path %s" % s3_path)
            break

        except Exception as e:
            num_retries = num_retries + 1
            __logger__.info("Error hit while uploading to S3: %s" % e)
            __logger__.info("Retrying %s out of %s" % (num_retries, __RETRY_TIMES))
            if num_retries == __RETRY_TIMES:
                raise e
            time.sleep(__SLEEP_SECONDS_BETWEEN_RETRIES)

def get_s3_bucket_region(s3_bucket_name, aws_credentials={}):
    conn = boto.connect_s3(**aws_credentials)
    bucket = conn.get_bucket(s3_bucket_name)
    return bucket.get_location() or "us-east-1"  # default=us-standard

def _is_valid_s3_key(s3_path, aws_credentials={}):
    key_prefix = _get_s3_key(s3_path)
    return key_prefix.exists()

def sync_s3(path1, path2, aws_credentials, silent = True):
    '''
    Sync a local path with remote path, or vice versa
    '''
    _awscli_s3_op('sync', path1, path2, aws_credentials = aws_credentials, silent = silent)

def download_from_s3(s3_path, local_path, is_dir=False, aws_credentials={}, silent=False):
    '''Download a file from S3'''

    local_path = expand_full_path(local_path)
    if not silent:
        __logger__.info('Downloading %s from s3 to local path %s' % (s3_path, local_path))

    if not is_local_path(local_path):
        raise RuntimeError("Invalid path: %s" % local_path)
    if os.path.exists(local_path):
        __logger__.debug("Overwriting file/path %s" % local_path)
    if not is_s3_path(s3_path):
        raise RuntimeError("Path %s is not a valid S3 path" % s3_path)

    # now download
    num_retries = 0
    while num_retries < __RETRY_TIMES:
        try:
            # check if should use boto
            if _use_boto():

                key_prefix = _get_s3_key(s3_path)
                bucket = key_prefix.bucket
                abs_path = os.path.abspath(local_path)

                # Download to a file
                if not is_dir:
                    if not key_prefix.exists():
                        raise RuntimeError("Key %s does not exist." % s3_path)

                    key_prefix.get_contents_to_filename(
                        abs_path,
                        cb = _s3_callback,
                        num_cb = 10)

                # Download to a directory.
                else:
                    # Get all the keys with the same prefix.
                    keys = []
                    for k in bucket.list(prefix = key_prefix.key):
                        keys.append(k)
                    if len(keys) == 0:
                        raise RuntimeError("Key %s does not exist." % s3_path)

                    # Make a new directory.
                    if os.path.exists(local_path):
                        shutil.rmtree(local_path)
                    os.makedirs(local_path)

                    for k in keys:

                        # Key-name relative to directory name.
                        s3_key = 's3://%s/%s' % (k.bucket.name, k.key)
                        rel_file = os.path.relpath(s3_key, s3_path)
                        path = '/'.join([abs_path, rel_file])
                        dirname = os.path.dirname(path)

                        # This is a directory. Make it.
                        if not os.path.exists(dirname):
                            os.makedirs(dirname)

                        k.get_contents_to_filename(
                            path,
                            cb = _s3_callback,
                            num_cb = 10)

            else:
                # use awscli
                _awscli_s3_op('cp', s3_path, local_path, recursive=is_dir, aws_credentials=aws_credentials,
                              silent=silent)

            __logger__.debug("Successfully downloaded file %s from s3" % s3_path)
            break
        except Exception as e:
            num_retries = num_retries + 1
            if not silent:
                __logger__.info("Error hit while download from S3: %s" % e)
                __logger__.info("Retrying %s out of %s" % (num_retries, __RETRY_TIMES))
            if num_retries == __RETRY_TIMES:
                raise e
            time.sleep(__SLEEP_SECONDS_BETWEEN_RETRIES)

def parse_s3_path(path):
    '''Parse a s3 path to bucket name and path name

    Parameters
    -----------
    path :  str
      s3 path like: s3://bucket_name/path/to/somewhere

    Returns
    --------
    out : (bucket_name, path_name)
    '''
    if not is_s3_path(path):
        raise ValueError('path is not a s3 path: %s' % path)

    tokens = path.split('/')
    bucket_name = tokens[2]

    s3_directory = '/'.join(tokens[3:])

    # remove trailing '/' if exists
    if s3_directory and s3_directory[-1] == '/':
        s3_directory = s3_directory[0:-1]

    return (bucket_name, s3_directory)

def s3_delete_key(s3_bucket_name, s3_key_name, aws_credentials = {}):
    conn = boto.connect_s3(**aws_credentials)
    bucket = conn.get_bucket(s3_bucket_name, validate=False)
    bucket.delete_key(s3_key_name)

def s3_touch(path, aws_credentials = {}):
    """
    Create an empty file in s3
    """
    with tempfile.NamedTemporaryFile() as f:
        upload_to_s3(f.name, path, aws_credentials=aws_credentials, silent=True)

def read_file_to_string_hdfs(hdfs_path, max_size=None, hadoop_conf_dir=None):
    ''' Read a file from hdfs and return the string content
    '''
    # list the file first, should be a single file
    file_info = list_hdfs(hdfs_path, hadoop_conf_dir=hadoop_conf_dir)
    if not file_info or len(file_info) != 1:
        return None
    if max_size and long(file_info[0]['size']) > max_size:
        raise RuntimeError("Cannot read file larger than max size %s." % str(max_size))

    base_command = 'hadoop fs -cat '
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -cat ' % hadoop_conf_dir

    base_command += '\"%s\" ' % hdfs_path
    exit_code, stdo, stde = _hdfs_exec_command(base_command)
    if exit_code == 0:
        return stdo
    return None

def remove_hdfs(hdfs_path, hadoop_conf_dir=None, recursive=False):
    base_command = 'hadoop fs -rm -f '
    '''
    Remove all file/files in the given path, recursively.
    '''
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -rm -f ' % hadoop_conf_dir
    if recursive:
        base_command += '-r '

    base_command += '\"%s\" ' % hdfs_path
    exit_code, stdo, stde = _hdfs_exec_command(base_command)

    if exit_code != 0:
        raise RuntimeError('Error encounted trying to run the following command: \n%s\n, '
                           'Output from the command: \n%s\n%s' % (base_command, stdo, stde))

def list_hdfs(hdfs_path, hadoop_conf_dir=None):
    base_command = 'hadoop fs -ls '
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -ls ' % hadoop_conf_dir

    base_command += '\"%s\" ' % hdfs_path
    exit_code, stdo, stde = _hdfs_exec_command(base_command)
    if exit_code == 0:
        files = []
        lines = stdo.split("\n")
        for l in lines:
            if len(l) == 0 or l.startswith("Found"):
                continue
            file_line = l.split(hdfs_path)
            file_info = file_line[0].split()  # split file info string
            file_name = file_line[1]  # file name
            file_size = file_info[4]  # file size
            files.append({"path": hdfs_path + file_name, "size": file_size})
        return files
    return None

def copy_to_hdfs(src_hdfs_path, dst_hdfs_path, hadoop_conf_dir=None, force=False):
    base_command = 'hadoop fs -cp '
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -cp ' % hadoop_conf_dir
    if force:
        base_command += ' -f'

    base_command += ' %s %s ' % (src_hdfs_path, dst_hdfs_path)
    exit_code, stdo, stde = _hdfs_exec_command(base_command)
    if exit_code != 0:
        raise RuntimeError("Failed to copy hdfs %s -> hdfs %s: %s" % (src_hdfs_path, dst_hdfs_path, stde))

def upload_to_hdfs(local_path, hdfs_path, hadoop_conf_dir=None, force=False, silent = True):
    base_command = 'hadoop fs -put '
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -put ' % hadoop_conf_dir
    if force:
        base_command += ' -f'

    base_command += ' %s %s ' % (local_path, hdfs_path)
    exit_code, stdo, stde = _hdfs_exec_command(base_command, silent = silent)
    if exit_code != 0:
        raise RuntimeError("Failed to copy  %s -> hdfs %s: %s" % (local_path, hdfs_path, stde))

def upload_folder_to_hdfs(local_path, hdfs_path, hadoop_conf_dir=None):
    if not os.path.isdir(local_path):
        raise RuntimeError("'%s' has to be a directory" % local_path)

    base_command = 'hadoop fs '
    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs ' % hadoop_conf_dir

    cp_command = '%s -copyFromLocal %s %s ' % (base_command, local_path, hdfs_path)
    exit_code, stdo, stde = _hdfs_exec_command(cp_command)
    if exit_code != 0:
        raise RuntimeError("Failed to copy directory %s -> hdfs %s: %s" % (local_path, hdfs_path, stde))

    # change folder permission
    chang_permission_cmd = base_command + '-chmod -R a+rwx \"%s\"' % hdfs_path
    exit_code, stdo, stde = _hdfs_exec_command(chang_permission_cmd)
    if exit_code != 0:
        raise RuntimeError("error changing permissions %s" % stde)

# Todo -- maybe use copyToLocal instead /*
def download_from_hdfs(hdfs_path, local_path, hadoop_conf_dir=None, is_dir = False):
    if is_dir:
        hdfs_path = '%s/*' % hdfs_path

    if hadoop_conf_dir:
        base_command = 'hadoop --config %s fs -get \"%s\" \"%s\" ' % (hadoop_conf_dir, hdfs_path, local_path)
    else:
        base_command = 'hadoop fs -get \"%s\" \"%s\" ' % (hdfs_path, local_path)
    exit_code, stdo, stde = _hdfs_exec_command(base_command)

    if exit_code != 0:
        raise RuntimeError("Failed to get  %s -> %s: %s" % (hdfs_path, local_path, stde))
    if not os.path.exists(local_path):
        raise RuntimeError('local file %s not found' % local_path)


def hdfs_touch(path, hadoop_conf_dir=None):
    """
    Create an empty file in HDFS
    """
    base_command = 'hadoop'
    if hadoop_conf_dir:
        base_command += ' --config %s' % hadoop_conf_dir

    touchz_cmd = base_command + ' fs -touchz \"%s\"' % path

    exit_code, stdo, stde = _hdfs_exec_command(touchz_cmd)
    if exit_code != 0:
        raise RuntimeError("error creating file '%s', error: %s" % (path, stde))

def hdfs_mkdir(dirname, hadoop_conf_dir=None):
    '''
    Create a new directory in HDFS
    '''
    if isinstance(dirname, list):
        dirname = " ".join(dirname)

    base_command = 'hadoop'
    if hadoop_conf_dir:
        base_command += ' --config %s' % hadoop_conf_dir

    mkdir_cmd = base_command + ' fs -mkdir -p \"%s\"' % dirname

    exit_code, stdo, stde = _hdfs_exec_command(mkdir_cmd)
    if exit_code != 0:
        raise RuntimeError("error making dir '%s', error: %s" % (dirname, stde))

    chang_permission_cmd = base_command + ' fs -chmod a+rwx \"%s\"' % dirname

    exit_code, stdo, stde = _hdfs_exec_command(chang_permission_cmd)
    if exit_code != 0:
        raise RuntimeError("error changing permissions %s" % stde)

def _intra_s3_copy_model(s3_src_path, s3_dest_path, aws_credentials = {}):
    assert(is_s3_path(s3_src_path) and is_s3_path(s3_dest_path))
    s3_src_bucket_name, s3_src_dir_name = parse_s3_path(s3_src_path)
    s3_dest_bucket_name, s3_dest_dir_name = parse_s3_path(s3_dest_path)

    conn = boto.connect_s3(**aws_credentials)
    s3_src_bucket = conn.get_bucket(s3_src_bucket_name, validate=False)
    s3_dest_bucket = conn.get_bucket(s3_dest_bucket_name, validate=False)

    # Get a list of all keys to copy
    num_retries = 0
    while num_retries < __RETRY_TIMES:
        try:
            keys_to_copy = s3_src_bucket.list(prefix=s3_src_dir_name)
            break
        except Exception as e:
            num_retries = num_retries + 1
            __logger__.info("Error hit while listing keys to S3: %s" % e)
            __logger__.info("Retrying %s out of %s" % (num_retries, __RETRY_TIMES))
            if num_retries == __RETRY_TIMES:
                raise e
            time.sleep(__SLEEP_SECONDS_BETWEEN_RETRIES)

    for k in keys_to_copy:
        k = k.name
        file_name = os.path.basename(k)
        new_key_name = s3_dest_dir_name + '/' + file_name

        # Do the actual copy
        num_retries = 0
        while num_retries < __RETRY_TIMES:
            try:
                s3_dest_bucket.copy_key(new_key_name, s3_src_bucket_name, k)
                break
            except Exception as e:
                num_retries = num_retries + 1
                __logger__.info("Error hit while listing keys to S3: %s" % e)
                __logger__.info("Retrying %s out of %s" % (num_retries, __RETRY_TIMES))
                if num_retries == __RETRY_TIMES:
                    raise e
                time.sleep(__SLEEP_SECONDS_BETWEEN_RETRIES)

def intra_s3_copy_model(s3_src_path, s3_dest_path, is_dir=False, aws_credentials = {}):
    '''
    copy model from a source s3 path to the target s3 path. set 'is_dir' to True if you plan
    to copy the directory. set 'is_dir' to False if you only plan to copy a single file.
    the default value for 'is_dir' is False.
    '''
    assert(is_s3_path(s3_src_path) and is_s3_path(s3_dest_path))

    # check if should use boto
    if _use_boto():
        _intra_s3_copy_model(s3_src_path, s3_dest_path, aws_credentials)
        return

    __logger__.info('Copying s3 path %s to s3 path %s' % (s3_src_path, s3_dest_path))
    # Get a list of all keys to copy
    num_retries = 0
    while num_retries < __RETRY_TIMES:
        try:
            _awscli_s3_op('cp', s3_src_path, s3_dest_path, recursive=is_dir, aws_credentials=aws_credentials)
            __logger__.info("Successfully copied from s3 path %s to s3 path %s" % (s3_src_path, s3_dest_path))
            break
        except Exception as e:
            num_retries = num_retries + 1
            __logger__.info("Error hit while copying model from %s to %s: %s" % (s3_src_path, s3_dest_path, e))
            __logger__.info("Retrying %s out of %s" % (num_retries, __RETRY_TIMES))
            if num_retries == __RETRY_TIMES:
                raise e
            time.sleep(__SLEEP_SECONDS_BETWEEN_RETRIES)

def s3_copy_model(src_path, s3_dest_path, aws_credentials = {}):
    assert(is_local_path(src_path) and is_s3_path(s3_dest_path))

    # check if should use boto
    if _use_boto():
        for base_file_name in os.listdir(src_path):
            source_file = os.path.join(src_path, base_file_name)
            file_dest = s3_dest_path + '/' + base_file_name
            upload_to_s3(source_file, file_dest, aws_credentials=aws_credentials)
    else:
        upload_to_s3(src_path, s3_dest_path, is_dir=True, aws_credentials=aws_credentials)


def s3_recursive_delete(s3_path, aws_credentials = {}):
    (s3_bucket_name, s3_key_prefix) = parse_s3_path(s3_path)
    conn = boto.connect_s3(**aws_credentials)
    bucket = conn.get_bucket(s3_bucket_name, validate=False)
    if s3_path.endswith('/'):
      s3_key_prefix += '/'
    matches = bucket.list(prefix=s3_key_prefix)
    __logger__.info('Deleting keys: %s' % [key.name for key in matches])
    bucket.delete_keys([key.name for key in matches])

def expand_full_path(path):
    '''Expand a relative path to a full path

    For example,
      '~/tmp' may be expanded to '/Users/username/tmp'
      'abc/def' may be expanded to '/pwd/abc/def'
    '''
    return os.path.abspath(os.path.expanduser(path))

def _get_s3_key(s3_path, aws_credentials = {}):
    '''Given S3 path, get the key object that represents the path'''
    conn = boto.connect_s3(**aws_credentials)
    (bucket_name, path) = parse_s3_path(s3_path)
    bucket = conn.get_bucket(bucket_name, validate=False)
    k = boto.s3.key.Key(bucket)
    k.key = path
    return k

def _s3_callback(complete, total):
    if complete > 0:
        __logger__.info('%d%% completed.' % int(1.0 * complete/total * 100))

def _awscli_s3_op(op, src, dst=None, recursive=False, silent=False, is_test=False,
                  aws_credentials = {}):
    ''' use AWS command line interface for any operations to s3 '''

    # get aws credentials from input or environment variables
    aws_access_key_id = None
    aws_secret_access_key = None
    if 'aws_access_key_id' in aws_credentials and 'aws_secret_access_key' in aws_credentials:
        aws_access_key_id = aws_credentials['aws_access_key_id']
        aws_secret_access_key = aws_credentials['aws_secret_access_key']
    elif 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    # use subprocess to perform aws s3 ops
    # "acl" grants the bucket owner full permission regardless of the uploader's account
    arglist = ['aws', 's3']
    if sys.platform == 'win32':
        arglist[0] = arglist[0] + ".cmd"
    if op == 'cp' or op == 'sync':
        arglist.extend([op, src, dst])
        if is_s3_path(dst):
            arglist.extend(['--acl', 'bucket-owner-full-control'])

        if op == 'sync':
            arglist.append('--delete')
            arglist.append('--exact-timestamps')
    elif op == 'rm':
        arglist.extend(['rm', src])
    if recursive:
        arglist.append('--recursive')
    if is_test:
        arglist.append('--dryrun')
    if silent:
        arglist.append('--quiet')

    env = os.environ.copy()
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        env['AWS_ACCESS_KEY_ID'] = aws_access_key_id
        env['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if 'PYTHONEXECUTABLE' in env:  # remove PYTHONEXECUTABLE for anaconda
        del env['PYTHONEXECUTABLE']

    # open subprocess
    proc = subprocess.Popen(arglist, env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=-1)
    # read stdout/stderr and wait for subprocess to finish
    prev = ''
    while True:
        line = proc.stdout.read(100)
        if line != '':
            # parse stdout from subprocess
            line = line.replace('\r\n', '\n').replace('\r', '\n')
            lines = line.split('\n')
            if len(lines) == 1:
                prev = prev + lines[0]
                continue
            else:
                # print status for each file, and total progress
                for index, l in enumerate(lines):
                    print_str = None
                    if index == 0:
                        print_str = prev + l
                    elif index == (len(lines) - 1):
                        prev = l
                    else:
                        print_str = l
                    if print_str:
                        _awscli_print(print_str)
        else:
            break

    proc.wait()
    # check for error
    if proc.returncode != 0:  # error
        raise RuntimeError('AWS S3 operation failed')

def _awscli_print(line):
    if line.startswith('Completed'):  # progress bar
        sys.stdout.write("\r%s" % line)
        sys.stdout.flush()
    else:  # file path
        sys.stdout.write('\r'+line+'\n')

def s3_test_url(s3_path, aws_credentials={}):
    """
    Test if a given key exist in S3. Return True if exists, otherwise False
    """
    conn = boto.connect_s3(**aws_credentials)
    (bucket_name, path) = parse_s3_path(s3_path)

    try:
        bucket = conn.get_bucket(bucket_name, validate=True)
        k = bucket.get_key(path, validate=True)
        return k is not None
    except S3ResponseError:
        return False

def hdfs_test_url(hdfs_url, test='e', hadoop_conf_dir=None):
    """
    Test the url.

    parameters
    ----------
    hdfs_url: string
        The hdfs url
    test: string, optional
        'e': existence
        'f': file
        'd': is a directory
        'z': zero length

        Default is an existence test, 'e'
    """

    if hadoop_conf_dir is None:
        command = "hadoop fs -test -%s %s" % (test, hdfs_url)
    else:
        command = "hadoop --config %s fs -test -%s %s" % (hadoop_conf_dir, test, hdfs_url)
    exit_code, stdo, stde = _hdfs_exec_command(command)
    return exit_code == 0

def _hdfs_exec_command(command, silent = True):
    """
    Execute HDFS command. and return the exit status.
    """
    if not silent:
        __logger__.info("Running hdfs command: \n%s" % command)

    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)

    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode

    return exit_code, stdo, stde

def retry(tries=3, delay=1, backoff=1, retry_exception=None):
    '''
    Retries a function or method until it has reached the maximum retries

    Parameters
    -----------
    tries : int, optional
        the number of times this function will retry

    delay : int, optional
        the number of seconds in delay to retry

    backoff : int, optional
        the number of factors by which the delay should increase after a retry

    retry_exception: Error, optional
        the type of error that only will trigger retries. Defaults to None so
        all types of error will trigger retries.

    This is derived from the original implementation of retry at:
    https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
    '''
    def deco_retry(f):
        def f_retry(*args, **kargs):
            mtries, mdelay = tries, delay  # mutables
            while mtries > 1:
                try:
                    return f(*args, **kargs)  # run function
                except Exception as e:
                    if retry_exception and not isinstance(e, retry_exception):
                        break  # break and return f if exception caught is not expected
                    mtries -= 1  # decrease retry
                    time.sleep(mdelay)  # delay to next retry
                    mdelay *= backoff  # increase delay
            return f(*args, **kargs)  # last retry
        return f_retry
    return deco_retry
