/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SFRAME_BUILDER_HPP
#define GRAPHLAB_UNITY_SFRAME_BUILDER_HPP

#include <vector>
#include <set>
#include <sframe/sframe.hpp>
#include <boost/circular_buffer.hpp>
#include <unity/lib/api/unity_sframe_builder_interface.hpp>

typedef boost::circular_buffer<std::vector<graphlab::flexible_type>> row_history_t;

namespace graphlab {

class unity_sframe_builder: public unity_sframe_builder_base {
 public:
  void init(size_t num_segments,
      size_t history_size,
      std::vector<std::string> column_names,
      std::vector<flex_type_enum> column_types);

  void append(const std::vector<flexible_type> &row, size_t segment);
  void append_multiple(const std::vector<std::vector<flexible_type>> &rows,
      size_t segment);

  std::vector<std::string> column_names();
  std::vector<flex_type_enum> column_types();
  std::vector<std::vector<flexible_type>> read_history(size_t num_elems);
  std::shared_ptr<unity_sframe_base> close();
 private:
  /// Methods

  /// Variables
  bool m_inited = false;
  bool m_closed = false;
  sframe m_sframe;
  std::vector<sframe::iterator> m_out_iters;
  flex_type_enum m_ary_type = flex_type_enum::UNDEFINED;

  std::shared_ptr<row_history_t> m_history;

};

} // namespace graphlab
#endif // GRAPHLAB_UNITY_SFRAME_BUILDER_HPP
