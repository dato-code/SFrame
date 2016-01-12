/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SARRAY_BUILDER_INTERFACE_HPP
#define GRAPHLAB_UNITY_SARRAY_BUILDER_INTERFACE_HPP
#include <vector>
#include <flexible_type/flexible_type.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {

class unity_sarray_base;

GENERATE_INTERFACE_AND_PROXY(unity_sarray_builder_base, unity_sarray_builder_proxy,
      (void, init, (size_t)(size_t)(flex_type_enum))
      (void, append, (const flexible_type&)(size_t))
      (void, append_multiple, (const std::vector<flexible_type>&)(size_t))
      (flex_type_enum, get_type, )
      (std::vector<flexible_type>, read_history, (size_t))
      (std::shared_ptr<unity_sarray_base>, close, )
    )

} // namespace graphlab

#endif //GRAPHLAB_UNITY_SARRAY_BUILDER_INTERFACE_HPP
#include <unity/lib/api/unity_sarray_interface.hpp>
