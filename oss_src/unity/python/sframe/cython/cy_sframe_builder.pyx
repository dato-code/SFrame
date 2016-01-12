'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
### cython utils ###
from cython.operator cimport dereference as deref

from .cy_ipc cimport PyCommClient
from .cy_unity_base_types cimport *
from .cy_flexible_type cimport flex_list

### flexible_type utils ###
from .cy_flexible_type cimport flex_type_enum_from_pytype
from .cy_flexible_type cimport pytype_from_flex_type_enum
from .cy_flexible_type cimport flex_list_from_iterable
from .cy_flexible_type cimport pylist_from_flex_list

### sframe ###
from .cy_sframe cimport create_proxy_wrapper_from_existing_proxy as sframe_proxy

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sframe_builder_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnitySFrameBuilderProxy(cli, True)
    ret._base_ptr = proxy
    ret.thisptr = <unity_sframe_builder_proxy*>(ret._base_ptr.get())
    return ret

cdef class UnitySFrameBuilderProxy:

    def __cinit__(self, PyCommClient cli, do_not_allocate=None):
        assert cli, "CommClient is Null"
        self._cli = cli
        if do_not_allocate:
            self._base_ptr.reset()
            self.thisptr = NULL
        else:
            self.thisptr = new unity_sframe_builder_proxy(deref(cli.thisptr))
            self._base_ptr.reset(<unity_sframe_builder_base*>(self.thisptr))

    cpdef init(self, object column_types, vector[string] column_names, size_t num_segments, size_t history_size):
        cdef vector[flex_type_enum] tmp_column_types
        for i in column_types:
          tmp_column_types.push_back(flex_type_enum_from_pytype(i))
        with nogil:
            self.thisptr.init(num_segments, history_size, column_names, tmp_column_types)

    cpdef append(self, row, size_t segment):
        cdef flex_list c_row = flex_list_from_iterable(row)
        with nogil:
            self.thisptr.append(c_row, segment)

    cpdef append_multiple(self, object rows, size_t segment):
        cdef vector[vector[flexible_type]] c_vals 
        for i in rows:
            c_vals.push_back(flex_list_from_iterable(i))
        self.thisptr.append_multiple(c_vals, segment)

    cpdef read_history(self, size_t num_elems):
        tmp_history = self.thisptr.read_history(num_elems)
        return [pylist_from_flex_list(i) for i in tmp_history]

    cpdef column_names(self):
        return self.thisptr.column_names()

    cpdef column_types(self):
        return [pytype_from_flex_type_enum(t) for t in self.thisptr.column_types()]

    cpdef close(self):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = self.thisptr.close()
        return sframe_proxy(self._cli, proxy)
