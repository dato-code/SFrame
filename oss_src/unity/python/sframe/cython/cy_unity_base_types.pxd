'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.map cimport map
from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "<memory>" namespace 'std':
    cdef cppclass shared_ptr[T]:
        shared_ptr()
        shared_ptr(T*)
        T* get()
        void reset()
        void reset(T*)

cdef extern from "<unity/lib/api/unity_sframe_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sframe_base:
        pass

cdef extern from "<unity/lib/api/unity_sarray_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sarray_base:
        pass

cdef extern from "<unity/lib/api/unity_graph_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sgraph_base:
        pass

cdef extern from "<unity/lib/api/unity_sketch_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sketch_base:
        pass

cdef extern from "<unity/lib/api/model_interface.hpp>" namespace 'graphlab':
    cdef cppclass model_base:
        pass

cdef extern from "<unity/lib/api/unity_global_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_global_base:
        pass

cdef extern from "<unity/lib/api/unity_sarray_builder_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sarray_builder_base:
        pass

cdef extern from "<unity/lib/api/unity_sframe_builder_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_sframe_builder_base:
        pass

ctypedef shared_ptr[unity_sarray_base] unity_sarray_base_ptr
ctypedef shared_ptr[unity_sframe_base] unity_sframe_base_ptr
ctypedef shared_ptr[unity_sgraph_base] unity_sgraph_base_ptr
ctypedef shared_ptr[unity_sketch_base] unity_sketch_base_ptr
ctypedef shared_ptr[model_base] model_base_ptr
ctypedef shared_ptr[unity_global_base] unity_global_base_ptr 
ctypedef shared_ptr[unity_sarray_builder_base] unity_sarray_builder_base_ptr
ctypedef shared_ptr[unity_sframe_builder_base] unity_sframe_builder_base_ptr
