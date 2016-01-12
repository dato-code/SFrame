'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from .cy_ipc cimport comm_client
from .cy_ipc cimport PyCommClient
from .cy_flexible_type cimport flex_type_enum
from .cy_flexible_type cimport flexible_type
from libcpp.vector cimport vector
from .cy_unity_base_types cimport *

cdef extern from "<unity/lib/api/unity_sarray_builder_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sarray_builder_proxy nogil:
        unity_sarray_builder_proxy(comm_client) except +
        void init(size_t, size_t, flex_type_enum) except +
        void append(flexible_type, size_t) except +
        void append_multiple(vector[flexible_type], size_t) except +
        flex_type_enum get_type() except +
        vector[flexible_type] read_history(size_t) except +
        unity_sarray_base_ptr close() except +

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sarray_builder_base_ptr& proxy)

cdef class UnitySArrayBuilderProxy:
    cdef unity_sarray_builder_base_ptr _base_ptr
    cdef unity_sarray_builder_proxy* thisptr
    cdef _cli

    cpdef init(self, size_t num_segments, size_t history_size, type dtype) 

    cpdef append(self, val, size_t segment)

    cpdef append_multiple(self, vals, size_t segment)

    cpdef read_history(self, size_t num_elems)

    cpdef get_type(self)

    cpdef close(self)
