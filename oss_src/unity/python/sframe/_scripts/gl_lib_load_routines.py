import sys
import os
from os.path import split, abspath, join
from glob import glob
from itertools import chain
import imp

def get_main_dir():
    script_path = abspath(sys.modules[__name__].__file__)
    main_dir = split(split(script_path)[0])[0]

    return main_dir

def get_installation_flavor():

    module = split(get_main_dir())[1]

    if module == "sframe":
        return "sframe"
    elif module == "graphlab":
        return "graphlab"
    else:
        raise ImportError("Installation module does not appear to be sframe or graphlab; main dir = %s"
                          % get_main_dir())


def load_isolated_gl_module(subdir, name):

    if subdir:
        path = join(get_main_dir(), subdir)
    else:
        path = get_main_dir()

    fp, pathname, description = imp.find_module(name, [path])

    try:
        return imp.load_module(name, fp, pathname, description)
    finally:
        # Since we may exit via an exception, close fp explicitly.
        if fp:
            fp.close()
    

def setup_environment(info_log_function = None, error_log_function = None):

    def _write_log(s, error = False):
        if error:
            if error_log_function is None:
                print s
            else:
                try:
                    error_log_function(s)
                except Exception as e:
                    print "Error setting exception: repr(e)"
                    print "Error: ", s
        else:
            if info_log_function is not None:
                try:
                    info_log_function(s)            
                except Exception as e:
                    print "Error logging info: %s." % repr(e)
                    print "Message: ", s
    
    ########################################
    # Set up the system path. 
    
    system_path = os.environ.get("__GL_SYS_PATH__", "")

    del sys.path[:]
    sys.path.extend(p.strip() for p in system_path.split(os.pathsep) if p.strip())

    for i, p in enumerate(sys.path):
        _write_log("  sys.path[%d] = %s. " % (i, sys.path[i]))

    ########################################
    # Now, import thnigs

    main_dir = get_main_dir()

    _write_log("Main program directory: %s." % main_dir)

    ########################################
    # Finally, set the dll load path if we are on windows
    if sys.platform == 'win32':

        import ctypes
        import ctypes.wintypes as wintypes

        # Back up to the directory, then to the base directory as this is
        # in ./_scripts.
        lib_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        def errcheck_bool(result, func, args):
            if not result:
                last_error = ctypes.get_last_error()
                if last_error != 0:
                    raise ctypes.WinError(last_error)
                else:
                    raise OSError
            return args

        # Also need to set the dll loading directory to the main
        # folder so windows attempts to load all DLLs from this
        # directory.
        try:
            kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
            kernel32.SetDllDirectoryW.errcheck = errcheck_bool
            kernel32.SetDllDirectoryW.argtypes = (wintypes.LPCWSTR,)
            kernel32.SetDllDirectoryW(lib_path)
        except Exception as e:
            _write_log("Error setting DLL load orders: %s (things may still work).\n" % str(e), error = True)

