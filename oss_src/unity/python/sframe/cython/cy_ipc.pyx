'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdio cimport printf
from .python_printer_callback import print_callback

cdef class PyCommClient:

    def __cinit__(self):
        pass

    def __dealloc__(self):
        pass

    cpdef stop(self):
        assert self.thisptr != NULL
        self.thisptr.stop()

    cpdef start(self):
        assert self.thisptr != NULL
        cdef reply_status r = self.thisptr.start()
        if (<size_t>r != 0):
            raise RuntimeError(reply_status_to_string(r))

def make_comm_client_from_existing_ptr(size_t client_ptr):
    ret = PyCommClient()
    ret.thisptr = <comm_client*>(client_ptr)
    return ret

cdef void print_status(string status_string) nogil:
    with gil:
        status_string = status_string.rstrip()
        print_callback(status_string)

ctypedef void* void_p
cpdef get_print_status_function_pointer():
    return <size_t>(<void_p>(print_status))
