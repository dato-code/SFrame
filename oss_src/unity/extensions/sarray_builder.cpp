/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <unity/lib/unity_sarray_builder.hpp>

namespace graphlab {

//TODO: Add a hint for the size?
void unity_sarray_builder::init(size_t num_segments, size_t history_size, flex_type_enum dtype) {
  if(m_inited)
    log_and_throw("This sarray_builder has already been initialized!");


  m_sarray.open_for_write(num_segments);
  m_out_iters.resize(num_segments);
  for(size_t i = 0; i < num_segments; ++i) {
    m_out_iters[i] = m_sarray.get_output_iterator(i);
  }
  m_history = std::make_shared<boost::circular_buffer<flexible_type>>(history_size);
  m_given_dtype = dtype;
  if(dtype != flex_type_enum::UNDEFINED)
    m_ary_type = m_given_dtype;

  m_inited = true;
}

//TODO: Should I provide instant feedback when an inserted type will cause an error?
void unity_sarray_builder::append(const flexible_type &val, size_t segment) {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("Cannot append values when closed.");

  if(segment >= m_out_iters.size()) {
    log_and_throw("Invalid segment number!");
  }

  m_history->push_back(val);
  auto in_type = val.get_type();
  // Only care about types if a dtype was not provided
  if(m_given_dtype == flex_type_enum::UNDEFINED &&
      in_type != flex_type_enum::UNDEFINED) {
    auto ins_ret = m_types_inserted.insert(in_type);
    if(ins_ret.second) {
      try {
        m_ary_type = get_common_type(m_types_inserted);
      } catch(std::string &e) {
        m_types_inserted.erase(ins_ret.first);
        log_and_throw(std::string("Append failed: ") +
            flex_type_enum_to_name(in_type) + std::string(" type is "
              "incompatible with types of existing values in this SArray."));
      }
    }
  }

  *(m_out_iters[segment]) = val;
}

void unity_sarray_builder::append_multiple(const std::vector<flexible_type> &vals, size_t segment) {
  for(const auto &i : vals) {
    this->append(i, segment);
  }
}

flex_type_enum unity_sarray_builder::get_type() {
  return m_ary_type;
}

std::vector<flexible_type> unity_sarray_builder::read_history(size_t num_elems) {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("History is invalid when closed.");

  if(num_elems > m_history->size())
    num_elems = m_history->size();

  if(num_elems == size_t(-1))
    num_elems = m_history->size();

  std::vector<flexible_type> ret_vec(num_elems);
  auto riter = m_history->rbegin();
  size_t ret_vec_idx = num_elems-1;
  for(; (riter != m_history->rend()); ++riter, --ret_vec_idx) {
    ret_vec[ret_vec_idx] = *riter;
  }

  return ret_vec;
}

std::shared_ptr<unity_sarray_base> unity_sarray_builder::close() {
  if(!m_inited)
    log_and_throw("Must call 'init' first!");

  if(m_closed)
    log_and_throw("Already closed.");

  // This will fail if the user supplied types go against the dtype they provided
  m_sarray.set_type(m_ary_type);

  m_sarray.close();
  m_closed = true;
  auto ret = std::make_shared<unity_sarray>();
  ret->construct_from_sarray(m_sarray);
  return ret;
}

} // namespace graphlab
