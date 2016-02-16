import cPickle as py_pickle
from libcpp.string cimport string
import traceback

cdef extern from "<python_callbacks/python_callbacks.hpp>" namespace "graphlab::python":
    cdef struct python_exception_info:
        string exception_pickle
        string exception_string

    void register_python_exception(const python_exception_info*)
    
cdef void register_exception(object e):
    """
    Provides a translation for handling exceptions between python and c++. 

    Process any possible exceptions by adding in information that can
    aid in the debugging of the callback functions functions. 
    """

    cdef python_exception_info pei
    cdef str traceback_str = traceback.format_exc()
    cdef str ex_str = "Exception in python callback function evaluation: \n"
    
    try:
        ex_str += repr(e)
    except Exception, e:
        ex_str += "Error expressing exception as string."

    ex_str += ": \n" + traceback_str
        
    pei.exception_string = ex_str

    try:
        pei.exception_pickle = py_pickle.dumps(e, protocol = -1)
    except Exception, e:
        pei.exception_pickle = py_pickle.dumps("<Error pickling exception>", protocol = -1)

    register_python_exception(&pei)
