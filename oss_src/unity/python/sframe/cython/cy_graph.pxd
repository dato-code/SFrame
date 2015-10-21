'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .cy_ipc cimport comm_client
from .cy_ipc cimport PyCommClient 

from .cy_flexible_type cimport flexible_type 
from .cy_flexible_type cimport gl_vec
from .cy_flexible_type cimport gl_options_map
from .cy_flexible_type cimport flex_type_enum 

from .cy_unity_base_types cimport *
from .cy_sarray cimport  UnitySArrayProxy
from .cy_sframe cimport  UnitySFrameProxy

from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map
from .cy_unity cimport function_closure_info
from .cy_unity cimport make_function_closure_info

cdef extern from "<unity/lib/api/unity_graph_interface.hpp>" namespace 'graphlab':
    cdef cppclass unity_graph_proxy nogil:
        unity_graph_proxy(comm_client) except +
        vector[string] get_vertex_fields(size_t) except +
        vector[string] get_edge_fields(size_t, size_t) except +
        vector[flex_type_enum] get_vertex_field_types(size_t) except +
        vector[flex_type_enum] get_edge_field_types(size_t, size_t) except +

        gl_options_map summary() except +

        unity_sframe_base_ptr get_vertices(gl_vec, gl_options_map, size_t) except +
        unity_sframe_base_ptr get_edges(gl_vec, gl_vec, gl_options_map, size_t, size_t) except +

        unity_sgraph_base_ptr add_vertices(unity_sframe_base_ptr, string, size_t) except +
        unity_sgraph_base_ptr add_edges(unity_sframe_base_ptr, string, string, size_t, size_t) except +

        unity_sgraph_base_ptr copy_vertex_field (string, string, size_t) except +
        unity_sgraph_base_ptr copy_edge_field (string, string, size_t, size_t) except +

        unity_sgraph_base_ptr add_vertex_field(unity_sarray_base_ptr, string) except +
        unity_sgraph_base_ptr add_edge_field(unity_sarray_base_ptr, string) except +

        unity_sgraph_base_ptr delete_vertex_field (string, size_t) except +
        unity_sgraph_base_ptr delete_edge_field (string, size_t, size_t) except +

        unity_sgraph_base_ptr select_vertex_fields(vector[string], size_t) except +
        unity_sgraph_base_ptr select_edge_fields(vector[string], size_t, size_t) except +

        unity_sgraph_base_ptr swap_edge_fields(string, string) except +
        unity_sgraph_base_ptr swap_vertex_fields(string, string) except +

        unity_sgraph_base_ptr rename_edge_fields(vector[string], vector[string]) except +
        unity_sgraph_base_ptr rename_vertex_fields(vector[string], vector[string]) except +

        unity_sgraph_base_ptr lambda_triple_apply(string, vector[string]) except +
        unity_sgraph_base_ptr lambda_triple_apply_native(const function_closure_info&, vector[string]) except +

        bint save_graph (string, string) except +
        bint load_graph (string) except +


cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sgraph_base_ptr& proxy)

cdef class UnityGraphProxy:
    cdef unity_sgraph_base_ptr _base_ptr
    cdef unity_graph_proxy* thisptr
    cdef _cli

    cpdef get_vertices(self, object ids, object field_constraints, size_t group=*)

    cpdef get_edges(self, object src_ids, object dst_ids, object field_constraints, size_t groupa=*, size_t groupb=*) 

    cpdef add_vertices(self, UnitySFrameProxy sframe, string id_field, size_t group=*)

    cpdef add_edges(self, UnitySFrameProxy sframe, string src_id_field, string dst_id_field, size_t groupa=*, size_t groupb=*)

    cpdef summary(self)


    cpdef get_vertex_fields(self, size_t group=*)

    cpdef get_vertex_field_types(self, size_t group=*)

    cpdef select_vertex_fields(self, vector[string] fields, size_t group=*)
    
    cpdef copy_vertex_field(self, string src_field, string dst_field, size_t group=*)

    cpdef delete_vertex_field(self, string field, size_t group=*)

    cpdef add_vertex_field(self, UnitySArrayProxy data, string name)

    cpdef swap_vertex_fields(self, string field1, string field2)

    cpdef rename_vertex_fields(self, vector[string] oldnames, vector[string] newnames)


    cpdef get_edge_fields(self, size_t groupa=*, size_t groupb=*)

    cpdef get_edge_field_types(self, size_t groupa=*, size_t groupb=*)

    cpdef select_edge_fields(self, vector[string] fields, size_t groupa=*, size_t groupb=*)

    cpdef copy_edge_field(self, string src_field, string dst_field, size_t groupa=*, size_t groupb=*)

    cpdef delete_edge_field(self, string field, size_t groupa=*, size_t groupb=*)

    cpdef add_edge_field(self, UnitySArrayProxy data, string name)

    cpdef swap_edge_fields(self, string field1, string field2)

    cpdef rename_edge_fields(self, vector[string] oldnames, vector[string] newnames)


    cpdef lambda_triple_apply(self, object, vector[string])

    cpdef lambda_triple_apply_native(self, object, vector[string])

    cpdef save_graph(self, string filename, string format)

    cpdef load_graph(self, string filename)
