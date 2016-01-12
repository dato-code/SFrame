/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <unity/lib/unity_sframe_builder.hpp>
#include <unity/lib/unity_sframe.hpp>

namespace graphlab {

void unity_sframe_builder::init(size_t num_segments,
                                size_t history_size,
                                std::vector<std::string> column_names,
                                std::vector<flex_type_enum> column_types) {
  if(m_inited)
    log_and_throw("This sframe_builder has already been initialized!");

  //m_sframe = std::make_shared<sframe>();
  m_sframe.open_for_write(column_names, column_types, "", num_segments);
  m_out_iters.resize(num_segments);
  for(size_t i = 0; i < num_segments; ++i) {
    m_out_iters[i] = m_sframe.get_output_iterator(i);
  }
  m_history = std::make_shared<row_history_t>(history_size);

  m_inited = true;
}

void unity_sframe_builder::append(const std::vector<flexible_type> &row, size_t segment) {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("Cannot append values when closed.");

  if(segment >= m_out_iters.size()) {
    log_and_throw("Invalid segment number!");
  }

  m_history->push_back(row);

  *(m_out_iters[segment]) = row;
}

void unity_sframe_builder::append_multiple(const std::vector<std::vector<flexible_type>> &vals, size_t segment) {
  for(const auto &i : vals) {
    this->append(i, segment);
  }
}
  
std::vector<std::string> unity_sframe_builder::column_names() {
  return m_sframe.column_names();
}

std::vector<flex_type_enum> unity_sframe_builder::column_types() {
  return m_sframe.column_types();
}

std::vector<std::vector<flexible_type>> unity_sframe_builder::read_history(size_t num_elems) {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("History is invalid when closed.");

  if(num_elems > m_history->size())
    num_elems = m_history->size();
  if(num_elems == size_t(-1))
    num_elems = m_history->size();

  std::vector<std::vector<flexible_type>> ret_vec(num_elems);

  if(num_elems == 0)
    return ret_vec;

  auto riter = m_history->rbegin();
  ssize_t ret_vec_idx = num_elems-1;
  for(; ((riter != m_history->rend()) && ret_vec_idx >= 0); ++riter, --ret_vec_idx) {
    ret_vec[ret_vec_idx] = *riter;
  }

  return ret_vec;
}

std::shared_ptr<unity_sframe_base> unity_sframe_builder::close() {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("Already closed.");

  m_sframe.close();
  m_closed = true;
  auto ret = std::make_shared<unity_sframe>();
  ret->construct_from_sframe(m_sframe);
  return ret;
}

} // namespace graphlab
