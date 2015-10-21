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
from .cy_flexible_type cimport gl_vec
from .cy_flexible_type cimport gl_options_map
from libcpp.vector cimport vector
from libcpp.string cimport string
from .cy_unity_base_types cimport * 
from .cy_unity cimport function_closure_info
from .cy_unity cimport make_function_closure_info

cdef extern from "<unity/lib/api/unity_sarray_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sarray_proxy nogil:
        unity_sarray_proxy(comm_client) except +
        void construct_from_vector(const gl_vec&, flex_type_enum) except +
        void construct_from_const(const flexible_type&, size_t) except +
        void construct_from_files(string, flex_type_enum) except +
        void construct_from_sarray_index(string) except +
        void construct_from_autodetect(string, flex_type_enum) except +
        void construct_from_avro(string) except +
        void clear() except +
        void save_array(string) except +
        size_t size() except +
        bint has_size() except +
        unity_sarray_base_ptr head(size_t) except +
        flex_type_enum dtype() except +
        unity_sarray_base_ptr vector_slice(size_t, size_t) except +
        unity_sarray_base_ptr transform(const string&, flex_type_enum, bint, int) except +
        unity_sarray_base_ptr transform_native(const function_closure_info&, flex_type_enum, bint, int) except +
        unity_sarray_base_ptr filter(const string&, bint, int) except +
        unity_sarray_base_ptr logical_filter(unity_sarray_base_ptr) except +
        unity_sarray_base_ptr topk_index(size_t, bint) except +
        bint all() except +
        bint any() except +
        flexible_type max() except +
        flexible_type min() except +
        flexible_type sum() except +
        flexible_type mean() except +
        flexible_type std(size_t) except +
        flexible_type var(size_t) except +
        size_t nnz() except +
        size_t num_missing() except +
        unity_sarray_base_ptr unary_negative() except +
        unity_sarray_base_ptr _pow(double v) except +
        unity_sarray_base_ptr astype(flex_type_enum, bint) except +
        unity_sarray_base_ptr str_to_datetime(string) except +
        unity_sarray_base_ptr datetime_to_str(string) except +
        unity_sarray_base_ptr clip(flexible_type, flexible_type) except +
        unity_sarray_base_ptr nonzero() except +
        unity_sarray_base_ptr tail(size_t) except +
        void begin_iterator() except +
        gl_vec iterator_get_next(size_t) except +
        unity_sarray_base_ptr left_scalar_operator(flexible_type, string) except +
        unity_sarray_base_ptr right_scalar_operator(flexible_type, string) except +
        unity_sarray_base_ptr vector_operator(unity_sarray_base_ptr, string) except +
        unity_sarray_base_ptr drop_missing_values() except +
        unity_sarray_base_ptr fill_missing_values(flexible_type) except +
        unity_sarray_base_ptr sample(float, int) except +
        void materialize() except +
        bint is_materialized() except +
        unity_sarray_base_ptr append(unity_sarray_base_ptr) except +
        unity_sarray_base_ptr count_bag_of_words(gl_options_map) except +
        unity_sarray_base_ptr count_character_ngrams(size_t, gl_options_map) except +
        unity_sarray_base_ptr count_ngrams(size_t, gl_options_map) except +
        unity_sarray_base_ptr dict_trim_by_keys(vector[flexible_type], bint) except +
        unity_sarray_base_ptr dict_trim_by_values(flexible_type, flexible_type) except +
        unity_sarray_base_ptr dict_keys() except +
        unity_sarray_base_ptr dict_values() except +
        unity_sarray_base_ptr dict_has_any_keys(vector[flexible_type]) except +
        unity_sarray_base_ptr dict_has_all_keys(vector[flexible_type]) except +
        unity_sarray_base_ptr item_length() except +
        unity_sframe_base_ptr expand(string column_name_prefix, vector[flexible_type] limit, vector[flex_type_enum] value_types) except +
        unity_sframe_base_ptr unpack_dict(string column_name_prefix, vector[flexible_type] limit, flexible_type) except +
        unity_sframe_base_ptr unpack(string column_name_prefix, vector[flexible_type] limit, vector[flex_type_enum] value_types, flexible_type) except +
        size_t __get_object_id() except +
        size_t get_content_identifier() except +
        unity_sarray_base_ptr copy_range(size_t, size_t, size_t) except +


cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sarray_base_ptr& proxy)

cdef class UnitySArrayProxy:
    cdef unity_sarray_base_ptr _base_ptr
    cdef unity_sarray_proxy* thisptr
    cdef _cli

    cpdef load_from_iterable(self, object d, type t, bint ignore_cast_failure=*)

    cpdef load_from_url(self, string url, type t)

    cpdef load_from_sarray_index(self, string index_file)

    cpdef load_from_const(self, object value, size_t size)

    cpdef load_autodetect(self, string url, type t)

    cpdef load_from_avro(self, string url)

    cpdef save(self, string index_file)

    cpdef size(self)

    cpdef has_size(self)

    cpdef head(self, size_t length)

    cpdef dtype(self)

    cpdef vector_slice(self, size_t start, size_t end)

    cpdef transform(self, fn, t, bint skip_undefined, int seed)

    cpdef transform_native(self, fn, t, bint skip_undefined, int seed)

    cpdef filter(self, fn, bint skip_undefined, int seed)

    cpdef logical_filter(self, UnitySArrayProxy other)

    cpdef topk_index(self, size_t k, bint reverse)

    cpdef num_missing(self)

    cpdef all(self)

    cpdef any(self)

    cpdef max(self)

    cpdef min(self)

    cpdef sum(self)

    cpdef mean(self)

    cpdef std(self, size_t ddof)

    cpdef var(self, size_t ddof)

    cpdef nnz(self)
  
    cpdef str_to_datetime(self, string str_format)

    cpdef datetime_to_str(self, string str_format)

    cpdef astype(self, type dtype, bint undefined_on_failure)

    cpdef clip(self, lower, upper)

    cpdef tail(self, size_t length)

    cpdef begin_iterator(self)

    cpdef iterator_get_next(self, size_t length)

    cpdef left_scalar_operator(self, object other, string op)

    cpdef right_scalar_operator(self, object other, string op)

    cpdef vector_operator(self, UnitySArrayProxy other, string op)

    cpdef drop_missing_values(self)

    cpdef fill_missing_values(self, default_value)

    cpdef sample(self, float percent, int seed)

    cpdef materialize(self)

    cpdef is_materialized(self)

    cpdef append(self, UnitySArrayProxy other)

    cpdef count_bag_of_words(self, object op)

    cpdef count_character_ngrams(self, size_t n, object op)

    cpdef count_ngrams(self, size_t n, object op)

    cpdef dict_trim_by_keys(self, object keys, bint exclude)

    cpdef dict_trim_by_values(self, lower, upper)

    cpdef dict_keys(self)

    cpdef dict_values(self)

    cpdef dict_has_any_keys(self, object)

    cpdef dict_has_all_keys(self, object)

    cpdef item_length(self)

    cpdef unpack_dict(self, string column_name_prefix, object, na_value)
  
    cpdef expand(self, string column_name_prefix, object, value_types)

    cpdef unpack(self, string column_name_prefix, object, value_types, na_value)

    cpdef __get_object_id(self)

    cpdef get_content_identifier(self)

    cpdef copy_range(self, size_t start, size_t step, size_t end)
