'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
### cython utils ###
from cython.operator cimport dereference as deref

### flexible_type utils ###
from .cy_flexible_type cimport pytype_to_flex_type_enum
from .cy_flexible_type cimport pytype_from_flex_type_enum
from .cy_flexible_type cimport flexible_type_from_pyobject
from .cy_flexible_type cimport glvec_from_iterable
from .cy_flexible_type cimport gl_options_map_from_pydict
from .cy_flexible_type cimport pyobject_from_flexible_type
from .cy_flexible_type cimport pylist_from_glvec
from .cy_flexible_type cimport pydict_from_gl_options_map

### sframe ###
from .cy_sframe cimport create_proxy_wrapper_from_existing_proxy as sframe_proxy

import inspect
import array

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sarray_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnitySArrayProxy(cli, True)
    ret._base_ptr = proxy
    ret.thisptr = <unity_sarray_proxy*>(ret._base_ptr.get())
    return ret

cdef class UnitySArrayProxy:

    def __cinit__(self, PyCommClient cli, do_not_allocate=None):
        assert cli, "CommClient is Null"
        self._cli = cli
        if do_not_allocate:
            self._base_ptr.reset()
            self.thisptr = NULL
        else:
            self.thisptr = new unity_sarray_proxy(deref(cli.thisptr))
            self._base_ptr.reset(<unity_sarray_base*>(self.thisptr))

    cpdef load_from_iterable(self, object d, type t, bint ignore_cast_failure=True):
        assert hasattr(d, '__iter__'), "object %s is not iterable" % str(d)
        #debug print "load from iterable: object: %s type %s" % (str(d), str(t))
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(t)
        cdef gl_vec vec
        datatype = pytype_to_flex_type_enum(t)
        vec = glvec_from_iterable(d, t,  ignore_cast_failure)
        with nogil:
            self.thisptr.construct_from_vector(vec, datatype)

    cpdef load_from_url(self, string url, type t):
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(t)
        with nogil:
            self.thisptr.construct_from_files(url, datatype)

    cpdef load_from_sarray_index(self, string index_file):
        with nogil:
            self.thisptr.construct_from_sarray_index(index_file)

    cpdef load_autodetect(self, string url, type t):
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(t)
        with nogil:
            self.thisptr.construct_from_autodetect(url, datatype)

    cpdef load_from_avro(self, string url):
        with nogil:
            self.thisptr.construct_from_avro(url)

    cpdef load_from_const(self, object value, size_t size):
        cdef flexible_type val = flexible_type_from_pyobject(value)
        with nogil:
            self.thisptr.construct_from_const(val, size)
    
    cpdef save(self, string index_file):
        with nogil:
            self.thisptr.save_array(index_file)

    cpdef size(self):
        return self.thisptr.size()

    cpdef has_size(self):
        return self.thisptr.has_size()

    cpdef head(self, size_t length):
        cdef unity_sarray_base_ptr proxy 
        with nogil:
            proxy = self.thisptr.head(length)
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dtype(self):
        cdef flex_type_enum flex_type_en = self.thisptr.dtype()
        return pytype_from_flex_type_enum(flex_type_en)

    cpdef vector_slice(self, size_t start, size_t end):
        if end <= start:
            raise RuntimeError("End of slice must be after start of slice")
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = self.thisptr.vector_slice(start, end)
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef transform(self, fn, t, bint skip_undefined, int seed):
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(t)
        cdef string lambda_str
        if type(fn) == str:
            lambda_str = fn
        else:
            from .. import util
            lambda_str = util._pickle_to_temp_location_or_memory(fn)

        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.transform(lambda_str, datatype, skip_undefined, seed))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef transform_native(self, closure, t, bint skip_undefined, int seed):
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(t)
        cdef unity_sarray_base_ptr proxy

        cl = make_function_closure_info(closure)
        with nogil:
            proxy = (self.thisptr.transform_native(cl, datatype, skip_undefined, seed))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef filter(self, fn, bint skip_undefined, int seed):
        cdef string lambda_str 
        if type(fn) == str:
            lambda_str = fn
        else:
            from .. import util
            lambda_str = util._pickle_to_temp_location_or_memory(fn)

        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.filter(lambda_str, skip_undefined, seed))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef logical_filter(self, UnitySArrayProxy other):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.logical_filter(other._base_ptr))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef topk_index(self, size_t topk, bint reverse):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.topk_index(topk, reverse))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef num_missing(self):
        cdef size_t ret
        with nogil:
            ret = self.thisptr.num_missing()
        return ret

    cpdef all(self):
        cdef bint ret
        with nogil:
            ret = self.thisptr.all()
        return ret

    cpdef any(self):
        cdef bint ret
        with nogil:
            ret = self.thisptr.any()
        return ret

    cpdef max(self):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.max()
        return pyobject_from_flexible_type(tmp)

    cpdef min(self):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.min()
        return pyobject_from_flexible_type(tmp)

    cpdef sum(self):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.sum()
        return pyobject_from_flexible_type(tmp)

    cpdef mean(self):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.mean()
        return pyobject_from_flexible_type(tmp)

    cpdef std(self, size_t ddof):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.std(ddof)
        return pyobject_from_flexible_type(tmp)

    cpdef var(self, size_t ddof):
        cdef flexible_type tmp
        with nogil:
            tmp = self.thisptr.var(ddof)
        return pyobject_from_flexible_type(tmp)

    cpdef nnz(self):
        return self.thisptr.nnz()

    cpdef astype(self, type dtype, bint undefined_on_failure):
        cdef flex_type_enum datatype = pytype_to_flex_type_enum(dtype)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.astype(datatype, undefined_on_failure))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef str_to_datetime(self,string str_format):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.str_to_datetime(str_format))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef datetime_to_str(self,string str_format):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.datetime_to_str(str_format))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef clip(self, lower, upper):
        cdef flexible_type l = flexible_type_from_pyobject(lower)
        cdef flexible_type u = flexible_type_from_pyobject(upper)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.clip(l, u))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef tail(self, size_t length):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.tail(length))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef begin_iterator(self):
        self.thisptr.begin_iterator()

    cpdef iterator_get_next(self, size_t length):
        cdef gl_vec tmp
        tmp = self.thisptr.iterator_get_next(length)
        return pylist_from_glvec(tmp)

    cpdef left_scalar_operator(self, object other, string op):
        cdef unity_sarray_base_ptr proxy
        cdef flexible_type val = flexible_type_from_pyobject(other)
        with nogil:
            proxy = (self.thisptr.left_scalar_operator(val, op))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)


    cpdef right_scalar_operator(self, object other, string op):
        cdef unity_sarray_base_ptr proxy
        cdef flexible_type val = flexible_type_from_pyobject(other)
        with nogil:
            proxy = (self.thisptr.right_scalar_operator(val, op))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef vector_operator(self, UnitySArrayProxy other, string op):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.vector_operator(other._base_ptr, op))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef drop_missing_values(self):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.drop_missing_values())
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef fill_missing_values(self, default_value):
        cdef flexible_type val = flexible_type_from_pyobject(default_value)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.fill_missing_values(val))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef sample(self, float percent, int seed):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.sample(percent, seed))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef materialize(self):
        with nogil:
            self.thisptr.materialize()

    cpdef is_materialized(self):
        return self.thisptr.is_materialized()

    cpdef append(self, UnitySArrayProxy other):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.append(other._base_ptr))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef count_bag_of_words(self, object op):
        cdef gl_options_map option_values = gl_options_map_from_pydict(op)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.count_bag_of_words(option_values))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)


    cpdef count_character_ngrams(self, size_t n, object op):
        cdef gl_options_map option_values = gl_options_map_from_pydict(op)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.count_character_ngrams(n, option_values))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)


    cpdef count_ngrams(self, size_t n, object op):
        cdef gl_options_map option_values = gl_options_map_from_pydict(op)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.count_ngrams(n, option_values))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_trim_by_keys(self, object keys, bint exclude):
        cdef unity_sarray_base_ptr proxy = (self.thisptr.dict_trim_by_keys(
            glvec_from_iterable(keys), exclude))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_trim_by_values(self, lower, upper):
        cdef flexible_type l = flexible_type_from_pyobject(lower)
        cdef flexible_type u = flexible_type_from_pyobject(upper)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.dict_trim_by_values(l, u))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_keys(self):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.dict_keys())
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_values(self):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.dict_values())
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_has_any_keys(self, object keys):
        cdef gl_vec vec = glvec_from_iterable(keys)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.dict_has_any_keys(vec))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef dict_has_all_keys(self, object keys):
        cdef gl_vec vec = glvec_from_iterable(keys)
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.dict_has_all_keys(vec))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef item_length(self):
        cdef unity_sarray_base_ptr proxy = (self.thisptr.item_length())
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)

    cpdef unpack_dict(self, string column_name_prefix, object limit, na_value):
        cdef unity_sframe_base_ptr proxy
        cdef flexible_type gl_na_value = flexible_type_from_pyobject(na_value)
        cdef vector[flexible_type] sub_keys
        for key in limit:
            sub_keys.push_back(flexible_type_from_pyobject(key))

        with nogil:
            proxy = self.thisptr.unpack_dict(column_name_prefix, sub_keys, gl_na_value)
        return sframe_proxy(self._cli, proxy)

    cpdef unpack(self, string column_name_prefix, object limit, value_types, na_value):
        cdef vector[flex_type_enum] column_types
        for t in value_types:
            column_types.push_back(pytype_to_flex_type_enum(t))

        cdef vector[flexible_type] sub_keys
        for key in limit:
            sub_keys.push_back(flexible_type_from_pyobject(key))

        cdef unity_sframe_base_ptr proxy
        cdef flexible_type gl_na_value = flexible_type_from_pyobject(na_value)
        with nogil:
            proxy = self.thisptr.unpack(column_name_prefix, sub_keys, column_types, gl_na_value)
        return sframe_proxy(self._cli, proxy)

    cpdef expand(self, string column_name_prefix, object limit, value_types):
        cdef vector[flex_type_enum] column_types
        for t in value_types:
            column_types.push_back(pytype_to_flex_type_enum(t))

        cdef vector[flexible_type] sub_keys
        for key in limit:
            sub_keys.push_back(flexible_type_from_pyobject(key))

        cdef unity_sframe_base_ptr proxy
        with nogil:
            proxy = self.thisptr.expand(column_name_prefix, sub_keys, column_types)
        return sframe_proxy(self._cli, proxy)

    cpdef __get_object_id(self):
        return self.thisptr.__get_object_id()

    cpdef get_content_identifier(self):
        return self.thisptr.get_content_identifier()

    cpdef copy_range(self, size_t start, size_t step, size_t end):
        cdef unity_sarray_base_ptr proxy
        with nogil:
            proxy = (self.thisptr.copy_range(start, step, end))
        return create_proxy_wrapper_from_existing_proxy(self._cli, proxy)
