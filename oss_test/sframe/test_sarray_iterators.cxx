/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <random>
#include <set>
#include <sstream>
#include <vector>
#include <algorithm>
#include <util/cityhash_gl.hpp>
#include <sframe/sarray_iterators.hpp>
#include <cxxtest/TestSuite.h>
#include <parallel/atomic.hpp>
#include <parallel/lambda_omp.hpp>
#include <iterator>

using namespace graphlab;

class sarray_iterator_test : public CxxTest::TestSuite {
 public:

  template <typename T>
  void _run_test_t(const std::vector<T>& values) {

    // Test different segment mode types
    for(int segment_mode : {0, 1, 2, 3}) {

      std::shared_ptr<sarray<T> > data(new sarray<T>); 
      
      size_t num_threads = thread::cpu_count(); 

      switch(segment_mode) {
        case 0: {
          // All in one segment
          data->open_for_write(1);

          auto it_out = data->get_output_iterator(0);
          for(size_t i = 0; i < values.size(); ++i, ++it_out) {
            *it_out = values[i];
          }
          
          data->close();
          break;
        }
          
        case 1: {
          // Even throughout different segments
          data->open_for_write(16);

          for(size_t seg_idx = 0; seg_idx < 16; ++seg_idx) { 
            auto it_out = data->get_output_iterator(seg_idx);

            size_t start_idx = (seg_idx * values.size()) / 16;
            size_t end_idx = ((seg_idx+1) * values.size()) / 16;

            for(size_t i = start_idx; i < end_idx; ++i, ++it_out) {
              *it_out = values[i];
            }
          }
          
          data->close();
          break;
        }

        case 2: {
          // elements in even segments, with odd segments empty in
          // between.
          data->open_for_write(16);

          for(size_t seg_idx = 0; seg_idx < 8; ++seg_idx) { 
            auto it_out = data->get_output_iterator(2 * seg_idx);

            size_t start_idx = (seg_idx * values.size()) / 8;
            size_t end_idx = ((seg_idx+1) * values.size()) / 8;

            for(size_t i = start_idx; i < end_idx; ++i, ++it_out) {
              *it_out = values[i];
            }
          }
          
          data->close();
          break;
        }

        case 3: {
          // elements in odd segments, with even segments empty in
          // between.
          data->open_for_write(16);

          for(size_t seg_idx = 0; seg_idx < 8; ++seg_idx) { 
            auto it_out = data->get_output_iterator(2 * seg_idx + 1);

            size_t start_idx = (seg_idx * values.size()) / 8;
            size_t end_idx = ((seg_idx+1) * values.size()) / 8;

            for(size_t i = start_idx; i < end_idx; ++i, ++it_out) {
              *it_out = values[i];
            }
          }
          
          data->close();
          break;
        }
      }

      ////////////////////////////////////////////////////////////////////////////////
      // Now, run though tests for each one.

      // First do it single threaded. 
      {
        auto it = make_sarray_block_iterator(data);
      
        std::vector<int> hit_count(values.size(), 0);

        size_t current_position = 0;
        std::vector<T> v;

        while(true) {

          size_t row_start; 
        
          bool done = it.read_next(&row_start, &v);

          if(done) {
            break;
          }

          DASSERT_EQ(row_start, current_position);

          for(size_t i = 0; i < v.size(); ++i) {
            DASSERT_EQ(v[i], values[row_start + i]);
            ++hit_count[row_start + i];
            ++current_position;
          }
        }

        size_t hit_count_ne_1_count =
            std::count_if(hit_count.begin(), hit_count.end(),
                          [](int i) { return i != 1; } );
        
        DASSERT_EQ(hit_count_ne_1_count, 0);
      }
      
      
      // Now do it multithreaded.
      {
        auto it = make_sarray_block_iterator(data);
      
        std::vector<int> hit_count(values.size(), 0);

        in_parallel([&](size_t thread_idx, size_t num_threads) {
            
            std::vector<T> v;
        
            while(true) {

              size_t row_start; 
        
              bool done = it.read_next(&row_start, &v);

              if(done) {
                break;
              }
              
              for(size_t i = 0; i < v.size(); ++i) {
                DASSERT_EQ(v[i], values[row_start + i]);
                ++hit_count[row_start + i];
              }
            }
          });

        size_t hit_count_ne_1_count =
            std::count_if(hit_count.begin(), hit_count.end(),
                          [](int i) { return i != 1; } );
        
        DASSERT_EQ(hit_count_ne_1_count, 0);
      }
    }
  }

  void test_int_1() {
    std::vector<size_t> v(100);
    std::iota(v.begin(), v.end(), 0);

    _run_test_t(v);
  }

  void test_int_2() {
    std::vector<size_t> v(10000);
    std::iota(v.begin(), v.end(), 0);

    _run_test_t(v);
  }

  void test_vector_1() {
    std::vector<std::vector<size_t> > v(1000);

    for(size_t i = 0; i < 1000; ++i) {
      v[i] = {i, 99999*i};
    }
    
    _run_test_t(v);
  }
};

