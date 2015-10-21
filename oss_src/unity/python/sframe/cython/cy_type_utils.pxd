'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
cdef extern from "<flexible_type/flexible_type_base_types.hpp>" namespace "graphlab":
    cdef cppclass flex_type_enum:
        pass
    cdef flex_type_enum flex_type_enum_from_name(string)
    cdef const char* flex_type_enum_to_name(flex_type_enum)

cdef enum py_flex_type_enum:
    INTEGER = 0
    FLOAT = 1
    STRING = 2
    VECTOR = 3
    LIST = 4
    DICT = 5
    DATETIME = 6
    UNDEFINED = 7
    IMAGE = 8


#/**************************************************************************/
#/*                                                                        */
#/*             Check python type as one of the flexible_type              */
#/*                                                                        */
#/**************************************************************************/

"""Return true if python type t matches flex_string"""
cdef bint py_check_flex_string(type t)

"""Return true if python type t matches flex_int"""
cdef bint py_check_flex_int(type t)

"""Return true if python type t matches pflex_date_time"""
cdef bint py_check_flex_date_time(type t)

"""Return true if python type t matches flex_float"""
cdef bint py_check_flex_float(type t)

"""Return true if python type t matches flex_vec"""
cdef bint py_check_flex_vec(type t, object v=*)

"""Return true if python type t matches flex_list"""
cdef bint py_check_flex_list(type t)

"""Return true if python type t matches flex_dict"""
cdef bint py_check_flex_dict(type t)

"""Return true if python type t matches flex_image"""
cdef bint py_check_flex_image(type t)

#/**************************************************************************/
#/*                                                                        */
#/*               python type and flex_type_enum conversion                */
#/*                                                                        */
#/**************************************************************************/
"""Convert the python type to flex_type_enum""" 
cdef flex_type_enum pytype_to_flex_type_enum(type t) except *

"""Convert flex_type_enum to python type""" 
cdef type pytype_from_flex_type_enum(flex_type_enum t)

"""Convert numpy dtype to python type""" 
cpdef pytype_from_dtype(object dt)

"""
Infer the datatype (python type) of a list-like object
Defined in pyx file.
""" 
#def infer_type_of_list(object data)


#/**************************************************************************/
#/*                                                                        */
#/*       Useful Helper Functions to check memory view compatiblity        */
#/*                                                                        */
#/**************************************************************************/
cdef bint is_numeric_memory_view(object v)
cdef bint is_int_memory_view(object v)
cdef bint is_long_memory_view(object v)
cdef bint is_float_memory_view(object v)
cdef bint is_double_memory_view(object v)
