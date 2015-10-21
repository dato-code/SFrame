/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_SFRAME_SARRAY_TYPE_CONVERSIONS_H_
#define GRAPHLAB_UNITY_SFRAME_SARRAY_TYPE_CONVERSIONS_H_

#include <memory>

namespace graphlab { 

class sframe;
class unity_sframe;
class unity_sframe_base;
class gl_sframe;

template <typename T> class sarray;
class unity_sarray;
class unity_sarray_base;
class gl_sarray;

////////////////////////////////////////////////////////////////////////////////
// Utility functions for conversion to other types.

////////////////////////////////////////
// To unity_sarray

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 */
std::shared_ptr<unity_sarray> to_unity_sarray(const gl_sarray& g);

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 *  \overload
 */
std::shared_ptr<unity_sarray> to_unity_sarray(const std::shared_ptr<unity_sarray>& u);

/** Converts a gl_sarray, unity_sarray, or
 *  sarray<flexible_type> to a unity_sarray.
 *  \overload
 */
std::shared_ptr<unity_sarray> to_unity_sarray(
    const std::shared_ptr<sarray<flexible_type> >& sa);

////////////////////////////////////////
// To sarray

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(const gl_sarray& g);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(const std::shared_ptr<unity_sarray>& g);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(
    const std::shared_ptr<unity_sarray_base>& g);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to an sarray<flexible_type>.
 * \overload
 */
std::shared_ptr<sarray<flexible_type> > to_sarray(
    const std::shared_ptr<sarray<flexible_type> >& sa);

////////////////////////////////////////
// To gl_sarray

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 */
gl_sarray to_gl_sarray(const std::shared_ptr<sarray<flexible_type> >& sa);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const std::shared_ptr<unity_sarray>& g);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const std::shared_ptr<unity_sarray_base>& g);

/** Converts a gl_sarray, unity_sarray, unity_sarray_base, or
 *  sarray<flexible_type> to a gl_sarray
 * \overload
 */
gl_sarray to_gl_sarray(const gl_sarray& g);

////////////////////////////////////////////////////////////////////////////////
// Sframe stuff

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const gl_sframe& g);

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 *  \overload
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const std::shared_ptr<unity_sframe>& u);

/** Converts a gl_sframe, unity_sframe, or
 *  sframe<flexible_type> to a unity_sframe.
 *  \overload
 */
std::shared_ptr<unity_sframe> to_unity_sframe(const sframe& sf);

////////////////////////////////////////
// To sframe

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 */
sframe to_sframe(const gl_sframe& g);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const std::shared_ptr<unity_sframe>& g);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const std::shared_ptr<unity_sframe_base>& g);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to an sframe.
 * \overload
 */
sframe to_sframe(const sframe& sf);

////////////////////////////////////////
// To gl_sframe

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 */
gl_sframe to_gl_sframe(const sframe& sf);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const std::shared_ptr<unity_sframe>& g);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const std::shared_ptr<unity_sframe_base>& g);

/** Converts a gl_sframe, unity_sframe, unity_sframe_base, or
 *  sframe to a gl_sframe
 * \overload
 */
gl_sframe to_gl_sframe(const gl_sframe& g);

}

#endif /* GRAPHLAB_UNITY_SFRAME_SARRAY_TYPE_CONVERSIONS_H_ */
