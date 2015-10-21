'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map
from libcpp.list cimport list as cpplist

from .cy_flexible_type cimport flex_type_enum
from .cy_flexible_type cimport flexible_type 

cdef extern from "<sframe/dataframe.hpp>" namespace 'graphlab':
    cdef cppclass dataframe_t:
        vector[string] names
        map[string, flex_type_enum] types
        map[string, vector[flexible_type]] values
        int nrows()
        int ncols()
        void clear()

ctypedef dataframe_t gl_dataframe

cdef bint is_pandas_dataframe(object v)

#/**************************************************************************/
#/*                                                                        */
#/*                Convert pandas dataframe to gl_dataframe                */
#/*                                                                        */
#/**************************************************************************/
cdef gl_dataframe gl_dataframe_from_pd(object) except *
cdef gl_dataframe gl_dataframe_from_dict_of_arrays(object) except *

#/**************************************************************************/
#/*                                                                        */
#/*                Convert gl_dataframe to pandas dataframe                */
#/*                                                                        */
#/**************************************************************************/
cdef pd_from_gl_dataframe(gl_dataframe&)
