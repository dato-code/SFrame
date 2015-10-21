'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from cython.operator cimport dereference as deref

from cy_flexible_type cimport pytype_from_flex_type_enum 
from cy_flexible_type cimport pydict_from_gl_options_map
from cy_flexible_type cimport pylist_from_glvec
from cy_flexible_type cimport gl_options_map_from_pydict 
from cy_flexible_type cimport glvec_from_iterable

from cy_sframe import UnitySFrameProxy
from cy_sframe cimport create_proxy_wrapper_from_existing_proxy as sframe_proxy 

from ..util import cloudpickle

GL_VERTEX_ID_COLUMN = "__id" 
GL_SOURCE_VID_COLUMN = "__src_id" 
GL_TARGET_VID_COLUMN = "__dst_id" 

cdef create_proxy_wrapper_from_existing_proxy(PyCommClient cli, const unity_sgraph_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnityGraphProxy(cli, True)
    ret._base_ptr = proxy
    ret.thisptr = <unity_graph_proxy*>(ret._base_ptr.get())
    return ret

cdef class UnityGraphProxy:

    def __cinit__(self, PyCommClient cli, do_not_allocate=None):
        assert cli, "CommClient is Null"
        if do_not_allocate:
            self._base_ptr.reset()
            self.thisptr = NULL
        else:
            self.thisptr = new unity_graph_proxy(deref(cli.thisptr))
            self._base_ptr.reset(<unity_sgraph_base*>(self.thisptr))
        self._cli = cli

    cpdef summary(self):
        cdef gl_options_map ret 
        with nogil:
            ret = self.thisptr.summary()
        return pydict_from_gl_options_map(ret)

    cpdef get_vertex_fields(self, size_t group=0):
        cdef vector[string] ret
        with nogil:
            ret = self.thisptr.get_vertex_fields(group);
        return ret

    cpdef get_vertex_field_types(self, size_t group=0):
        cdef vector[flex_type_enum] flex_types
        with nogil:
            flex_types = self.thisptr.get_vertex_field_types(group)
        return [pytype_from_flex_type_enum(t) for t in flex_types]

    cpdef get_edge_fields(self, size_t groupa=0, size_t groupb=0):
        cdef vector[string] ret
        with nogil:
            ret = self.thisptr.get_edge_fields(groupa, groupb);
        return ret

    cpdef get_edge_field_types(self, size_t groupa=0, size_t groupb=0):
        cdef vector[flex_type_enum] flex_types
        with nogil:
            flex_types = self.thisptr.get_edge_field_types(groupa, groupb)
        return [pytype_from_flex_type_enum(t) for t in flex_types]

    cpdef get_vertices(self, object ids, object field_constraints,
        size_t group=0):
        cdef gl_vec id_vec = glvec_from_iterable(ids)
        cdef gl_options_map field_map = gl_options_map_from_pydict(field_constraints)
        cdef unity_sframe_base_ptr result 
        with nogil:
            result = self.thisptr.get_vertices(id_vec, field_map, group)
        return sframe_proxy(self._cli, result)

    cpdef get_edges(self, object src_ids, object dst_ids, object field_constraints,
        size_t groupa=0, size_t groupb=0):
        cdef gl_vec src_vec = glvec_from_iterable(src_ids)
        cdef gl_vec dst_vec = glvec_from_iterable(dst_ids)
        cdef gl_options_map field_map = gl_options_map_from_pydict(field_constraints)
        cdef unity_sframe_base_ptr result 
        with nogil:
            result = self.thisptr.get_edges(src_vec, dst_vec, field_map, groupa, groupb)
        return sframe_proxy(self._cli, result)

    cpdef add_vertices(self, UnitySFrameProxy sf, string id_field, size_t group=0):
        cdef unity_sgraph_base_ptr new_graph 
        with nogil:
            new_graph = self.thisptr.add_vertices(sf._base_ptr, id_field, group)
        return create_proxy_wrapper_from_existing_proxy(self._cli, new_graph)

    cpdef add_edges(self, UnitySFrameProxy sf, string src_id_field, string dst_id_field,
                    size_t groupa=0, size_t groupb=0):
        cdef unity_sgraph_base_ptr new_graph 
        with nogil:
            new_graph = self.thisptr.add_edges(sf._base_ptr, src_id_field, 
                                                                   dst_id_field, groupa, groupb)
        return create_proxy_wrapper_from_existing_proxy(self._cli, new_graph)

    cpdef select_vertex_fields(self, vector[string] fields, size_t group=0):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.select_vertex_fields(fields, group)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef copy_vertex_field(self, string src_field, string dst_field, size_t group=0):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.copy_vertex_field(src_field, dst_field, group)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef delete_vertex_field(self, string field, size_t group=0):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.delete_vertex_field(field, group)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef add_vertex_field(self, UnitySArrayProxy data, string name):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.add_vertex_field(data._base_ptr, name)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef swap_vertex_fields(self, string field1, string field2):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.swap_vertex_fields(field1, field2)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef rename_vertex_fields(self, vector[string] oldnames, vector[string] newnames):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.rename_vertex_fields(oldnames, newnames)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef select_edge_fields(self, vector[string] fields, size_t groupa=0, size_t groupb=0):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.select_edge_fields(fields, groupa, groupb)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef copy_edge_field(self, string src_field, string dst_field, size_t groupa=0, size_t groupb=0):
        cdef unity_sgraph_base_ptr newgraph = self.thisptr.copy_edge_field(src_field, dst_field, groupa, groupb)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef delete_edge_field(self, string field, size_t groupa=0, size_t groupb=0):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.delete_edge_field(field, groupa, groupb)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef add_edge_field(self, UnitySArrayProxy data, string name):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.add_edge_field(data._base_ptr, name)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef swap_edge_fields(self, string field1, string field2):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.swap_edge_fields(field1, field2)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef rename_edge_fields(self, vector[string] oldnames, vector[string] newnames):
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.rename_edge_fields(oldnames, newnames)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef lambda_triple_apply(self, object fn, vector[string] mutated_fields):
        cdef string lambda_str = cloudpickle.dumps(fn) 
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.lambda_triple_apply(lambda_str, mutated_fields)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef lambda_triple_apply_native(self, closure, vector[string] mutated_fields):
        cl = make_function_closure_info(closure)
        cdef unity_sgraph_base_ptr newgraph 
        with nogil:
            newgraph = self.thisptr.lambda_triple_apply_native(cl, mutated_fields)
        return create_proxy_wrapper_from_existing_proxy(self._cli, newgraph)

    cpdef save_graph(self, string filename, string format):
        cdef bint ret 
        with nogil:
            ret = self.thisptr.save_graph(filename, format)
        return ret

    cpdef load_graph(self, string filename):
        cdef bint ret
        with nogil:
            ret = self.thisptr.load_graph(filename)
        return ret
