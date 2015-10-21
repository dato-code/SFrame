/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <unity/lib/gl_sarray.hpp>
#include <unity/lib/gl_sframe.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>

namespace graphlab { 

/**************************************************************************/
/*                                                                        */
/*             Conversion functions                                       */
/*                                                                        */
/**************************************************************************/

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 */
std::shared_ptr<unity_sarray> to_unity_sarray(const gl_sarray& g) {
  std::shared_ptr<unity_sarray> u = g;
  return u;
}

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 *  \overload
 */
std::shared_ptr<unity_sarray> to_unity_sarray(const std::shared_ptr<unity_sarray>& u) {
  return u;
}

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 *  \overload
 */
std::shared_ptr<unity_sarray> to_unity_sarray(
    const std::shared_ptr<sarray<flexible_type> >& sa) {
  std::shared_ptr<unity_sarray> u;
  u->construct_from_sarray(sa);
  return u;
}

////////////////////////////////////////
// To sarray

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(const gl_sarray& g) {
  return to_sarray(to_unity_sarray(g));
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(const std::shared_ptr<unity_sarray>& g) {
  return g->get_underlying_sarray();
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(
    const std::shared_ptr<unity_sarray_base>& g) {
  return to_sarray(gl_sarray(g));
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(
    const std::shared_ptr<sarray<flexible_type> >& sa) {
  return sa;
}

////////////////////////////////////////
// To gl_sarray

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 */
gl_sarray to_gl_sarray(const std::shared_ptr<sarray<flexible_type> >& sa) {
  return gl_sarray(to_unity_sarray(sa));
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const std::shared_ptr<unity_sarray>& g) {
  return gl_sarray(g);
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const std::shared_ptr<unity_sarray_base>& g) {
  return gl_sarray(g);
}

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const gl_sarray& g) { 
  return g;
}

////////////////////////////////////////////////////////////////////////////////
// Sframe stuff

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const gl_sframe& g) {
  std::shared_ptr<unity_sframe> u = g;
  return u;
}

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 *  \overload
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const std::shared_ptr<unity_sframe>& u) {
  return u;
}

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 *  \overload
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const sframe& sf) {
  std::shared_ptr<unity_sframe> u;
  u->construct_from_sframe(sf);
  return u;
}

////////////////////////////////////////
// To sframe

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 */
sframe to_sframe(const gl_sframe& g) {
  return to_sframe(to_unity_sframe(g));
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const std::shared_ptr<unity_sframe>& g) {
  return *(g->get_underlying_sframe());
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const std::shared_ptr<unity_sframe_base>& g) {
  return to_sframe(gl_sframe(g));
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const sframe& sf) {
  return sf;
}

////////////////////////////////////////
// To gl_sframe

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 */
gl_sframe to_gl_sframe(const sframe& sf) {
  return gl_sframe(to_unity_sframe(sf));
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const std::shared_ptr<unity_sframe>& g) {
  return gl_sframe(g);
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const std::shared_ptr<unity_sframe_base>& g) {
  return gl_sframe(g);
}

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const gl_sframe& g) { 
  return g;
}


}
