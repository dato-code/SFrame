'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.pair cimport pair

from cython.operator cimport dereference as deref
from cython.operator cimport preincrement as inc

#### flexible_type utils ####
from .cy_flexible_type cimport pytype_to_flex_type_enum
from .cy_flexible_type cimport pytype_from_flex_type_enum
from .cy_flexible_type cimport flexible_type_from_pyobject
from .cy_flexible_type cimport glvec_from_iterable
from .cy_flexible_type cimport gl_options_map_from_pydict
from .cy_flexible_type cimport pyobject_from_flexible_type
from .cy_flexible_type cimport pylist_from_glvec
from .cy_flexible_type cimport pydict_from_gl_options_map

#### dataframe utils ####
from .cy_dataframe cimport gl_dataframe_from_pd
from .cy_dataframe cimport gl_dataframe_from_dict_of_arrays
from .cy_dataframe cimport pd_from_gl_dataframe
from .cy_dataframe cimport is_pandas_dataframe


#### sarray ####
from .cy_sarray cimport create_proxy_wrapper_from_existing_proxy as sarray_proxy
from .cy_sarray cimport unity_sarray_proxy
from .cy_sarray cimport UnitySArrayProxy

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sframe_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnitySFrameProxy(cli, True)
    ret._base_ptr = proxy
    ret.thisptr = <unity_sframe_proxy*>(ret._base_ptr.get())
    return ret

cdef pydict_from_gl_error_map(PyCommClient cli, gl_error_map& d):
    """
    Converting map[string, sarray] to dict
    """
    cdef unity_sarray_base_ptr proxy 
    ret = {}
    it = d.begin()
    cdef pair[string, unity_sarray_base_ptr] entry
    while (it != d.end()):
        entry = deref(it)
        ret[entry.first] = sarray_proxy(cli, (entry.second))
        inc(it)
    return ret

cdef class UnitySFrameProxy:

    def __cinit__(self, PyCommClient cli, do_not_allocate=None):
        assert cli, "CommClient is Null"
        self._cli = cli
        if do_not_allocate:
            self._base_ptr.reset()
            self.thisptr = NULL
        else:
            self.thisptr = new unity_sframe_proxy(deref(cli.thisptr))
            self._base_ptr.reset(<unity_sframe_base*>(self.thisptr))

    cpdef load_from_dataframe(self, dataframe):
        cdef gl_dataframe gldf
        if is_pandas_dataframe(dataframe):
            gldf = gl_dataframe_from_pd(dataframe)
        else:
            gldf = gl_dataframe_from_dict_of_arrays(dataframe)
        with nogil:
            self.thisptr.construct_from_dataframe(gldf)

    cpdef load_from_sframe_index(self, index_file):
        cdef string str_index_file = index_file
        with nogil:
            self.thisptr.construct_from_sframe_index(str_index_file)

    cpdef load_from_csvs(self, string url, object csv_config, object column_type_hints):
        cdef map[string, flex_type_enum] c_column_type_hints
        for key in column_type_hints:
            c_column_type_hints[key] = pytype_to_flex_type_enum(column_type_hints[key])
        cdef gl_options_map csv_options = gl_options_map_from_pydict(csv_config)
        cdef gl_error_map errors
        with nogil:
            errors = self.thisptr.construct_from_csvs(url, csv_options, c_column_type_hints)
        return pydict_from_gl_error_map(self._cli, errors)

    cpdef save(self, string index_file):
        with nogil:
            self.thisptr.save_frame(index_file)

    cpdef save_reference(self, string index_file):
        with nogil:
            self.thisptr.save_frame_reference(index_file)

    cpdef num_rows(self):
        return self.thisptr.size()

    cpdef num_columns(self):
        return self.thisptr.num_columns()

    cpdef dtype(self):
        return [pytype_from_flex_type_enum(t) for t in self.thisptr.dtype()]

    cpdef column_names(self):
        return self.thisptr.column_names()

    cpdef head(self, size_t n):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.head(n))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef tail(self, size_t n):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.tail(n))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef transform(self, fn, t, int seed):
        cdef flex_type_enum flex_type_en = pytype_to_flex_type_enum(t)
        cdef string lambda_str
        if type(fn) == str:
            lambda_str = fn
        else:
            from .. import util
            lambda_str = util._pickle_to_temp_location_or_memory(fn)
        # skip_undefined options is not used for now.
        skip_undefined = 0
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.transform(lambda_str, flex_type_en, skip_undefined, seed))
        return sarray_proxy(self._cli, proxy)

    cpdef transform_native(self, closure, t, int seed):
        cdef flex_type_enum flex_type_en = pytype_to_flex_type_enum(t)
        cl = make_function_closure_info(closure)
        # skip_undefined options is not used for now.
        skip_undefined = 0
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.transform_native(cl, flex_type_en, skip_undefined, seed))
        return sarray_proxy(self._cli, proxy)

    cpdef flat_map(self, object fn, vector[string] column_names, object py_column_types, int seed):
        cdef vector[flex_type_enum] column_types
        cdef string lambda_str
        for t in py_column_types:
            column_types.push_back(pytype_to_flex_type_enum(t))
        if type(fn) == str:
            lambda_str = fn
        else:
            from .. import util
            lambda_str = util._pickle_to_temp_location_or_memory(fn)
        cdef unity_sframe_base_ptr proxy
        skip_undefined = 0
        with nogil:
            proxy = (self.thisptr.flat_map(lambda_str,
              column_names, column_types, skip_undefined, seed))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef logical_filter(self, UnitySArrayProxy other):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.logical_filter(other._base_ptr))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef select_columns(self, vector[string] keylist):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.select_columns(keylist))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef select_column(self, string key):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.select_column(key))
        return sarray_proxy(self._cli, proxy)

    cpdef add_column(self, UnitySArrayProxy data, string name):
        with nogil:
            self.thisptr.add_column(data._base_ptr, name)

    cpdef add_columns(self, object datalist, vector[string] namelist):
        cdef cpplist[unity_sarray_base_ptr] proxies
        cdef UnitySArrayProxy proxy
        for i in datalist:
          proxy = i
          proxies.push_back(proxy._base_ptr)

        with nogil:
            self.thisptr.add_columns(proxies, namelist)

    cpdef remove_column(self, size_t i):
        with nogil:
            self.thisptr.remove_column(i)

    cpdef swap_columns(self, size_t i, size_t j):
        with nogil:
            self.thisptr.swap_columns(i, j)

    cpdef set_column_name(self, size_t i, string name):
        with nogil:
            self.thisptr.set_column_name(i, name)

    cpdef begin_iterator(self):
        self.thisptr.begin_iterator()

    cpdef iterator_get_next(self, size_t length):
        tmp = self.thisptr.iterator_get_next(length)
        return [pylist_from_glvec(x) for x in tmp]

    cpdef save_as_csv(self, string url, object csv_config):
        cdef gl_options_map csv_options = gl_options_map_from_pydict(csv_config)
        with nogil:
            self.thisptr.save_as_csv(url, csv_options)

    cpdef sample(self, float percent, int random_seed):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = self.thisptr.sample(percent, random_seed)
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef random_split(self, float percent, int random_seed):
        cdef cpplist[unity_sframe_base_ptr] sf_array
        with nogil:
            sf_array = self.thisptr.random_split(percent, random_seed)
        assert sf_array.size() == 2
        cdef unity_sframe_base_ptr proxy_first = (sf_array.front())
        cdef unity_sframe_base_ptr proxy_second = (sf_array.back())
        first = create_proxy_wrapper_from_existing_proxy(self._cli, proxy_first)
        second = create_proxy_wrapper_from_existing_proxy(self._cli, proxy_second)
        return (first, second)

    cpdef groupby_aggregate(self, vector[string] key_columns, vector[vector[string]] group_column, vector[string] group_output_columns, vector[string] column_ops):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = self.thisptr.groupby_aggregate(key_columns, group_column, group_output_columns, column_ops)
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef append(self, UnitySFrameProxy other):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.append(other._base_ptr))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef materialize(self):
        self.thisptr.materialize()

    cpdef is_materialized(self):
        return self.thisptr.is_materialized()

    cpdef has_size(self):
        return self.thisptr.has_size()

    cpdef query_plan_string(self):
        return self.thisptr.query_plan_string()

    cpdef join(self, UnitySFrameProxy right, string how, map[string,string] on):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.join(right._base_ptr, how, on))

        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef pack_columns(self, vector[string] column_names, vector[string] key_names, dtype, fill_na):
        cdef unity_sarray_base_ptr proxy
        cdef flex_type_enum fl_type = pytype_to_flex_type_enum(dtype)
        cdef flexible_type na_val = flexible_type_from_pyobject(fill_na)
        with nogil:
            proxy = self.thisptr.pack_columns(column_names, key_names, fl_type, na_val)
        return sarray_proxy(self._cli, proxy)

    cpdef stack(self, string column_name, vector[string] new_column_names, new_column_types, drop_na):
        cdef vector[flex_type_enum] column_types
        cdef bint b_drop_na = drop_na
        for t in new_column_types:
            column_types.push_back(pytype_to_flex_type_enum(t))

        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = self.thisptr.stack(column_name, new_column_names, column_types, b_drop_na)
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef sort(self, vector[string] sort_columns, vector[int] sort_orders):
        cdef unity_sframe_base_ptr proxy
        # some how c++ side doesn't support vector[bool], using vector[int] here
        cdef vector[int] orders = [int(i) for i in sort_orders]
        with nogil:
            proxy = (self.thisptr.sort(sort_columns, orders))

        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef drop_missing_values(self, vector[string] columns, bint is_all, bint split):
        cdef cpplist[unity_sframe_base_ptr] sf_array
        with nogil:
            sf_array = self.thisptr.drop_missing_values(columns, is_all, split)
        assert sf_array.size() == 2
        cdef unity_sframe_base_ptr proxy_first = (sf_array.front())
        cdef unity_sframe_base_ptr proxy_second
        first = create_proxy_wrapper_from_existing_proxy(self._cli, proxy_first)
        if split:
            proxy_second = (sf_array.back())
            second = create_proxy_wrapper_from_existing_proxy(self._cli, proxy_second)
            return (first, second)
        else:
            return first

    cpdef __get_object_id(self):
        return self.thisptr.__get_object_id()


    cpdef copy_range(self, size_t start, size_t step, size_t end):
        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.copy_range(start, step, end))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef delete_on_close(self):
        with nogil:
          self.thisptr.delete_on_close()
