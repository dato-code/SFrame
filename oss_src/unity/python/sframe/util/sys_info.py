import os
import platform
import sys
import uuid

from ..version_info import *

try:
  from .. import minipsutil
  TOTAL_PHYMEM = minipsutil.total_memory()
  NUM_CPUS = minipsutil.cpu_count()
except ImportError:
  TOTAL_PHYMEM = 0
  NUM_CPUS = 0

def _tup_to_flat_str(tup):
    tmp_list = []
    for t in tup:
        if isinstance(t, tuple):
            tmp_str =_tup_to_flat_str(t)
            tmp_list.append(tmp_str)
        elif isinstance(t, str):
            tmp_list.append(t)
        else:
        # UNEXPECTED! Just don't crash
            try:
                tmp_list.append(str(t))
            except:
                pass
    return " ".join(tmp_list)

def get_sys_info():
    sys_info = {}

    # Get OS-specific info
    sys_info['system'] = platform.system()

    if sys_info['system'] == 'Linux':
      sys_info['os_version'] = _tup_to_flat_str(platform.linux_distribution())
      sys_info['libc_version'] = _tup_to_flat_str(platform.libc_ver())
    elif sys_info['system'] == 'Darwin':
      sys_info['os_version'] = _tup_to_flat_str(platform.mac_ver())
    elif sys_info['system'] == 'Windows':
      sys_info['os_version'] = _tup_to_flat_str(platform.win32_ver())
    elif sys_info['system'] == 'Java':
      sys_info['os_version'] = _tup_to_flat_str(platform.java_ver())

    # Python specific stuff
    sys_info['python_implementation'] = platform.python_implementation()
    sys_info['python_version'] = platform.python_version()
    sys_info['python_build'] = _tup_to_flat_str(platform.python_build())
    sys_info['python_executable'] = sys.executable

    # Dato specific stuff
    sys_info['dato_launcher'] = 'DATO_LAUNCHER' in os.environ

    # Get architecture info
    sys_info['architecture'] = _tup_to_flat_str(platform.architecture())
    sys_info['platform'] = platform.platform()
    sys_info['num_cpus'] = NUM_CPUS

    # Get RAM size
    sys_info['total_mem'] = TOTAL_PHYMEM

    return sys_info

def get_distinct_id(distinct_id="unknown"):
    if distinct_id == 'unknown':
        poss_id = 'unknown'
        gldir = os.path.join(os.path.expanduser('~'),'.graphlab')
        try:
            if not os.path.isdir(gldir):
                os.makedirs(gldir)
        except:
            pass

        id_file_path = os.path.join(gldir, "id")

        if os.path.isfile(id_file_path):
            try:
                with open(id_file_path, 'r') as f:
                    poss_id = f.readline()
            except:
                return "session-" + str(uuid.uuid4())
        else:
            # no distinct id found from installation,
            # try to create one and write it to the appropriate location
            # if not able to write to appropriate location, then create temp one
            new_id = str(uuid.uuid4())
            try:
                with open(id_file_path, "w") as id_file:
                    id_file.write(new_id)
            except:
                return "session-" + str(uuid.uuid4())
            return new_id
        return poss_id.strip()
    else:
        return distinct_id

def get_version():
    return version

def get_isgpu():
    if build_number.endswith('.gpu'):
        return True
    else:
        return False

def get_build_number():
    if build_number.endswith('.gpu'):
        return build_number.split('.gpu')[0]
    else:
        return build_number
