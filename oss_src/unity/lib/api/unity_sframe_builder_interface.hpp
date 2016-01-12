#ifndef GRAPHLAB_UNITY_SFRAME_BUILDER_INTERFACE_HPP
#define GRAPHLAB_UNITY_SFRAME_BUILDER_INTERFACE_HPP
#include <vector>
#include <string>
#include <flexible_type/flexible_type.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {

class unity_sframe_base;

GENERATE_INTERFACE_AND_PROXY(unity_sframe_builder_base, unity_sframe_builder_proxy,
      (void, init, (size_t)(size_t)(std::vector<std::string>)(std::vector<flex_type_enum>))
      (void, append, (const std::vector<flexible_type>&)(size_t))
      (void, append_multiple, (const std::vector<std::vector<flexible_type>>&)(size_t))
      (std::vector<std::string>, column_names, )
      (std::vector<flex_type_enum>, column_types, )
      (std::vector<std::vector<flexible_type>>, read_history, (size_t))
      (std::shared_ptr<unity_sframe_base>, close, )
    )

} // namespace graphlab

#endif //GRAPHLAB_UNITY_SFRAME_BUILDER_INTERFACE_HPP
#include <unity/lib/api/unity_sframe_interface.hpp>
