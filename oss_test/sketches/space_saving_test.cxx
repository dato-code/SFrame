/*
 * Copyright (C) 2016 Turi
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string>
#include <iostream>
#include <set>
#include <unordered_map>
#include <flexible_type/flexible_type.hpp>
#include <sketches/space_saving_flextype.hpp>
#include <random/random.hpp>
#include <cxxtest/TestSuite.h>
#include <util/cityhash_gl.hpp> 
#include <parallel/lambda_omp.hpp> 

using namespace graphlab;
using namespace graphlab::sketches;

class space_saving_test: public CxxTest::TestSuite {  
 private:
  template <typename SketchType> 
  double random_integer_length_test(size_t len, 
                                    size_t random_range,
                                    double epsilon) {
    SketchType ss(epsilon);
    
    std::vector<size_t> v(len);
    std::map<size_t, size_t> true_counter;
    for (size_t i = 0;i < len; ++i) {
      v[i] = graphlab::random::fast_uniform<size_t>(0, random_range - 1);
      ++true_counter[v[i]];
    }
    graphlab::timer ti;
    for (size_t i = 0;i < len; ++i) {
      if(v[i] % 2 == 0) {
        ss.add(v[i]);
      } else {
        ss.add(double(v[i]));  // So the types get a bit mixed up
      }
    }
    double rt = ti.current_time();
    std::vector<size_t> frequent_items;
    for(auto x : true_counter) {
      if (x.second >= epsilon * len) {
        frequent_items.push_back(x.first);
      }
    }
    // check that we did indeed find all the items with count >= epsilon * N
    auto ret = ss.frequent_items();
    std::sort(ret.begin(), ret.end());
    std::set<graphlab::flexible_type> ss_returned_values;
    for(auto x : ret) {
      ss_returned_values.insert(x.first);
    }

    for(auto x: frequent_items) {
      TS_ASSERT(ss_returned_values.count(x) != 0);
    }
    return rt;
  }

  template <typename SketchType> 
  double parallel_combine_test(size_t len, 
                               size_t random_range,
                               double epsilon) {
    std::vector<SketchType> ssarr(16, SketchType(epsilon));

    std::vector<size_t> v(len);
    std::unordered_map<size_t, size_t> true_counter;
    for (size_t i = 0;i < len; ++i) {
      v[i] = graphlab::random::fast_uniform<size_t>(0, random_range - 1);
      ++true_counter[v[i]];
    }
    graphlab::timer ti;
    for (size_t i = 0;i < len; ++i) {
      ssarr[i % ssarr.size()].add(v[i]);
    }
    // merge
    SketchType ss;
    for (size_t i = 0;i < ssarr.size(); ++i) {
      ss.combine(ssarr[i]);
    }
    double rt = ti.current_time();
    // check that we did indeed find all the items with count >= epsilon * N
    std::vector<size_t> frequent_items;
    for(auto x : true_counter) {
      if (x.second >= epsilon * len) {
        frequent_items.push_back(x.first);
      }
    }
    auto ret = ss.frequent_items();
    std::set<graphlab::flexible_type> ss_returned_values;
    for(auto x : ret) {
      ss_returned_values.insert(x.first);
    }
    for(auto x: frequent_items) {
      TS_ASSERT(ss_returned_values.count(x) != 0);
    }
    return rt;
  }
 public:
  void test_perf() {
    graphlab::sketches::space_saving_flextype ss(0.0001);
    graphlab::timer ti;
    for (size_t i = 0;i < 10*1024*1024; ++i) {
      ss.add(i);
    }
    std::cout << "\n Time: " << ti.current_time() << "\n";
  }
  
  void test_stuff() {
    graphlab::random::seed(1001);
    std::vector<size_t> lens{1024, 65536, 256*1024};
    std::vector<size_t> ranges{128, 1024, 65536, 256*1024};
    std::vector<double> epsilon{0.1,0.01,0.005};

    size_t n_idx = lens.size() * ranges.size() * epsilon.size(); 
    size_t thread_idx = 0;
    size_t n_threads = 1;
        for(size_t run_idx = thread_idx; run_idx < n_idx; run_idx += n_threads) {
            
          auto len   = lens[ run_idx / (epsilon.size() * ranges.size()) ];
          auto range = ranges[ (run_idx / epsilon.size()) % ranges.size()];
          auto eps   = epsilon[run_idx % epsilon.size()];
        
          std::cout << "integer:   Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << random_integer_length_test<space_saving<flex_int> >(len, range, eps)
                    << std::endl;

          std::cout << "flex type: Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << random_integer_length_test<space_saving<flexible_type> >(len, range, eps)
                    << std::endl;
          
          std::cout << "_flextype: Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << random_integer_length_test<space_saving_flextype>(len, range, eps)
                    << std::endl;
        }
        
    std::cout << "\n\nReset random seed and repeating with \'parallel\' test\n";
    graphlab::random::seed(1001);
      
        for(size_t run_idx = thread_idx; run_idx < n_idx; run_idx += n_threads) {
          
          auto len   = lens[ run_idx / (epsilon.size() * ranges.size()) ];
          auto range = ranges[ (run_idx / epsilon.size()) % ranges.size()];
          auto eps   = epsilon[run_idx % epsilon.size()];
        
          std::cout << "integer:   Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << parallel_combine_test<space_saving<flex_int> >(len, range, eps)
                    << std::endl;

          std::cout << "flex type: Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << parallel_combine_test<space_saving<flexible_type> >(len, range, eps)
                    << std::endl;
          
          std::cout << "_flextype: Array length: " << len << "\t" 
                    << "Numeric Range: " << range << "\t"
                    << "Epsilon:   " << eps << "  \t"
                    << parallel_combine_test<space_saving_flextype>(len, range, eps)
                    << std::endl;
        }
      
  }

  
};
