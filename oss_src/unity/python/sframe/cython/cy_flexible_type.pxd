'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
#cython libcpp types
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map
from libcpp.pair cimport pair

from .cy_type_utils cimport *


#### Define flexible type base types ####
# From "<flexible_type/flexible_type_base_types.hpp>" namespace "graphlab":
ctypedef long long flex_int
ctypedef double flex_float
ctypedef pair[long long,int] pflex_date_time
ctypedef vector[double] flex_vec
ctypedef string flex_string
ctypedef vector[flexible_type] flex_list
ctypedef vector[pair[flexible_type, flexible_type]] flex_dict



####Flex_image externs
cdef extern from "<image/image_type.hpp>" namespace "graphlab":
    cdef cppclass flex_image:
        flex_image()
        flex_image(char* image_data, size_t height, size_t width, size_t channels, size_t image_data_size, int version, int format)
        const char* get_image_data()
        size_t m_height
        size_t m_width
        size_t m_channels
        size_t m_image_data_size
        char m_format
        char m_version

#### Externs from flexible_type.hpp
cdef extern from "<flexible_type/flexible_type.hpp>" namespace "graphlab":
    cdef cppclass flexible_type:
        flexible_type()
        char get_type()
        # Cython bug:
        # we should put "except + TypeError" after the delcaration,
        # but it does not compile.
        flex_int& get_int "mutable_get<graphlab::flex_int>"()
        pflex_date_time get_date_time "get_date_time_as_timestamp_and_offset"()
        int get_microsecond "get_date_time_microsecond"()
        flex_float& get_double "mutable_get<graphlab::flex_float>"()
        flex_string& get_string "mutable_get<std::string>"()
        flex_vec& get_vec "mutable_get<std::vector<double> >"()
        flex_list& get_recursive "mutable_get<graphlab::flex_list>"()
        flex_dict& get_dict "mutable_get<graphlab::flex_dict>"()
        flex_image& get_img "mutable_get<graphlab::flex_image>"()

        flexible_type& set_int "operator=<int64_t>"(const flex_int& other)
        flexible_type& set_date_time "set_date_time_from_timestamp_and_offset"(const pflex_date_time& other, int)
        flexible_type& set_double "operator=<double>"(const flex_float& other)
        flexible_type& set_string "operator=<std::string>"(const flex_string& other)
        flexible_type& set_vec "operator=<std::vector<double> >"(const flex_vec& other)
        flexible_type& set_recursive "operator=<graphlab::flex_list>"(const flex_list& other)
        flexible_type& set_dict "operator=<graphlab::flex_dict>"(const flex_dict& other)
        flexible_type& set_img "operator=<graphlab::flex_image>"(const flex_image& other)
        void reset()

    cdef flexible_type FLEX_UNDEFINED

    cdef int TIMEZONE_RESOLUTION_IN_SECONDS "graphlab::flex_date_time::TIMEZONE_RESOLUTION_IN_SECONDS"
    cdef float TIMEZONE_RESOLUTION_IN_HOURS "graphlab::flex_date_time::TIMEZONE_RESOLUTION_IN_HOURS"
    cdef int EMPTY_TIMEZONE "graphlab::flex_date_time::EMPTY_TIMEZONE"



### Other unity types ###
ctypedef map[string, flexible_type] gl_options_map
ctypedef vector[flexible_type] gl_vec

#/**************************************************************************/
#/*                                                                        */
#/*            Converting from python objects to flexible_type             */
#/*                                                                        */
#/**************************************************************************/
cdef flexible_type flexible_type_from_pyobject(object) except *
cdef flexible_type flexible_type_from_pyobject_with_type_force(object, type force) except *
cdef gl_vec glvec_from_iterable(object values, type t=*, bint ignore_cast_failure=*) except *
cdef gl_options_map gl_options_map_from_pydict(dict) except *

#/**************************************************************************/
#/*                                                                        */
#/*            Converting from flexible_type to python objects             */
#/*                                                                        */
#/**************************************************************************/
cdef pyobject_from_flexible_type(flexible_type&)
cdef pylist_from_glvec(vector[flexible_type]&)
cdef pydict_from_gl_options_map(gl_options_map&)

