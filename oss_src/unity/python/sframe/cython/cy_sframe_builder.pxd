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
from libcpp.string cimport string
from .cy_unity_base_types cimport *

cdef extern from "<unity/lib/api/unity_sframe_builder_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sframe_builder_proxy nogil:
        unity_sframe_builder_proxy(comm_client) except +
        void init(size_t, size_t, vector[string], vector[flex_type_enum]) except +
        void append(const vector[flexible_type]&, size_t) except +
        void append_multiple(const vector[vector[flexible_type]]&, size_t) except +
        vector[string] column_names() except +
        vector[flex_type_enum] column_types() except +
        vector[vector[flexible_type]] read_history(size_t) except +
        unity_sframe_base_ptr close() except +

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sframe_builder_base_ptr& proxy)

cdef class UnitySFrameBuilderProxy:
    cdef unity_sframe_builder_base_ptr _base_ptr
    cdef unity_sframe_builder_proxy* thisptr
    cdef _cli

    cpdef init(self, object column_types, vector[string] column_names, size_t num_segments, size_t history_size)

    cpdef append(self, row, size_t segment)

    cpdef append_multiple(self, object rows, size_t segment)

    cpdef read_history(self, size_t num_elems)

    cpdef column_names(self)

    cpdef column_types(self)

    cpdef close(self)
