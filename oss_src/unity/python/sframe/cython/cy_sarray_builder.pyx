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
from .cy_flexible_type cimport flexible_type_from_pyobject
from .cy_flexible_type cimport flex_list_from_iterable
from .cy_flexible_type cimport pylist_from_flex_list

### sarray ###
from .cy_sarray cimport create_proxy_wrapper_from_existing_proxy as sarray_proxy

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sarray_builder_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnitySArrayBuilderProxy(cli, True)
    ret._base_ptr = proxy
    ret.thisptr = <unity_sarray_builder_proxy*>(ret._base_ptr.get())
    return ret

cdef class UnitySArrayBuilderProxy:

    def __cinit__(self, PyCommClient cli, do_not_allocate=None):
        assert cli, "CommClient is Null"
        self._cli = cli
        if do_not_allocate:
            self._base_ptr.reset()
            self.thisptr = NULL
        else:
            self.thisptr = new unity_sarray_builder_proxy(deref(cli.thisptr))
            self._base_ptr.reset(<unity_sarray_builder_base*>(self.thisptr))

    cpdef init(self, size_t num_segments, size_t history_size, type dtype):
        cdef flex_type_enum tmp_type
        tmp_type = flex_type_enum_from_pytype(dtype)
        with nogil:
            self.thisptr.init(num_segments, history_size, tmp_type)

    cpdef append(self, val, size_t segment):
        cdef flexible_type c_val = flexible_type_from_pyobject(val)
        with nogil:
            self.thisptr.append(c_val, segment)

    cpdef append_multiple(self, object vals, size_t segment):
        cdef flex_list c_vals = flex_list_from_iterable(vals)
        with nogil:
            self.thisptr.append_multiple(c_vals, segment)

    cpdef read_history(self, size_t num_elems):
        cdef flex_list tmp_history
        with nogil:
            tmp_history = self.thisptr.read_history(num_elems)
        return pylist_from_flex_list(tmp_history)

    cpdef get_type(self):
        cdef flex_type_enum ret_type
        with nogil:
            ret_type = self.thisptr.get_type()
        return pytype_from_flex_type_enum(ret_type)

    cpdef close(self):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = self.thisptr.close()
        return sarray_proxy(self._cli, proxy)
