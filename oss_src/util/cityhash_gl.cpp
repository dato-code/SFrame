/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <util/cityhash_gl.hpp>
#include <flexible_type/flexible_type.hpp> 
#include <logger/assertions.hpp>
#include <vector>

namespace graphlab {

uint128_t hash128(const flexible_type& v) {
  return v.hash128(); 
}

uint64_t hash64(const flexible_type& v) {
  return v.hash(); 
}

uint128_t hash128(const std::vector<flexible_type>& v) {
  uint128_t h = hash128(v.size());

  for(const flexible_type& x : v)
    h = hash128_combine(h, x.hash128());

  return h;
}

uint64_t hash64(const std::vector<flexible_type>& v) {
  return hash64(hash128(v));
}

/**
 *  When hash64 is used as a random number function, it is nice to be
 *  able to do the following to get a proportion:
 *
 *  uint64_t threshold = hash64_proportion_cutoff(proportion);
 *  // ...
 *  if(hash64(...) < threshold) {
 *     // do something that happens `proportion` of the time.
 *  }
 *
 *  Unfortunately, this computation of the proportion is prone to
 *  numerical issues due to the 48 bits of precision of the double,
 *  which this function gets around.
 */
uint64_t hash64_proportion_cutoff(double proportion) {

  DASSERT_GE(proportion, 0.0);
  DASSERT_LE(proportion, 1.0);

  uint64_t x_half = uint64_t(proportion * double(uint64_t(1) << 63));

  const uint64_t clip_0 = uint64_t(1) << 63;
  const uint64_t clip_1 = std::numeric_limits<uint64_t>::max() - clip_0;

  return std::min(clip_0, x_half) + std::min(clip_1, x_half);
}

}; 
