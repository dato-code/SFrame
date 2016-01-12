/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SARRAY_BUILDER_HPP
#define GRAPHLAB_UNITY_SARRAY_BUILDER_HPP

#include <vector>
#include <sframe/sarray.hpp>
#include <boost/circular_buffer.hpp>
#include <unity/lib/api/unity_sarray_builder_interface.hpp>

namespace graphlab {

// forward declarations
template <typename T>
class sarray;

class unity_sarray_builder: public unity_sarray_builder_base {
 public:
  void init(size_t num_segments, size_t history_size, flex_type_enum dtype);

  void append(const flexible_type &val, size_t segment);
  void append_multiple(const std::vector<flexible_type> &vals, size_t segment);

  flex_type_enum get_type();

  /**
   * Return the last `num_elems` elements appended.
   */
  std::vector<flexible_type> read_history(size_t num_elems);
  std::shared_ptr<unity_sarray_base> close();
 private:
  /// Methods

  /// Variables
  bool m_inited = false;
  bool m_closed = false;
  std::shared_ptr<sarray<flexible_type>> m_sarray;
  std::vector<sarray<flexible_type>::iterator> m_out_iters;
  flex_type_enum m_ary_type = flex_type_enum::UNDEFINED;
  flex_type_enum m_given_dtype = flex_type_enum::UNDEFINED;
  std::set<flex_type_enum> m_types_inserted;

  std::shared_ptr<boost::circular_buffer<flexible_type>> m_history;
};

} // namespace graphlab
#endif // GRAPHLAB_UNITY_SARRAY_BUILDER_HPP
