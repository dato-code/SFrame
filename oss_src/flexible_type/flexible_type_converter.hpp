/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_FLEXIBLE_TYPE_CONVERTER_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_FLEXIBLE_TYPE_CONVERTER_HPP
#include <vector>
#include <map>
#include <unordered_map>
#include <tuple>
#include <type_traits>
#include <util/code_optimization.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/type_traits.hpp>
#include <flexible_type/flexible_type_conversion_utilities.hpp>

namespace graphlab {

/** The primary functions/structs available for doing the flexible
 *  type conversion.
 *
 */
template <typename T> struct is_flexible_type_convertible;
template <typename T> static void convert_from_flexible_type(T& t, const flexible_type& f);
template <typename T> static void convert_to_flexible_type(flexible_type& f, const T& t);
template <typename T> static flexible_type convert_to_flexible_type(const T& t);

namespace flexible_type_internals {

/**
 * Type conversion priority is done according to the lowest numbered
 * ft_converter for which matches<T>() returns true.  If no converter
 * correctly converts things, then a static assert is raised if
 * convert is attempted.
 *
 * If matches() is not true, then the get and set methods are never
 * instantiated.
 */
template <int> struct ft_converter {
  template <typename T> static constexpr bool matches() { return false; }

  template <typename T> static inline void get(T& t, const flexible_type& v) {
    static_assert(swallow_to_false<T>::value, "get with bad match.");
  }

  template <typename T>
  static inline void set(flexible_type& t, const T& v) {
    static_assert(swallow_to_false<T>::value, "set with bad match.");
  }
};

////////////////////////////////////////////////////////////////////////////////
// These are declared in terms of priority.  The resolver will use the
// first one in this list such that matches() is true.

/** straight flexible_type.  Always given priority.
 */
template <> struct ft_converter<1> {

  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flexible_type>::value;
  }

  template <typename T>
  static void get(T& dest, const flexible_type& src) {
    dest = src;
  }

  template <typename T>
  static void set(flexible_type& dest, const T& src) {
    dest = src;
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Any floating point values.
 */
template <> struct ft_converter<2> {

  template <typename T> static constexpr bool matches() {
    return std::is_floating_point<T>::value;
  }

  template <typename Float>
  static void get(Float& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::FLOAT) {
      dest = static_cast<Float>(src.get<flex_float>());
    } else if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Float>(src.get<flex_int>());
    } else {
      throw_type_conversion_error(src, "numeric");
    }
  }

  template <typename Float>
  static void set(Float& dest, const flexible_type& src) {
    dest = flex_float(src);
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Integer values.
 */
template <> struct ft_converter<3> {

  template <typename T> static constexpr bool matches() {
    return std::is_integral<T>::value;
  }

  template <typename Integer>
  static void get(Integer& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::FLOAT) {
      flex_float v = src.get<flex_float>();
      if(static_cast<Integer>(v) != v) {
        throw_type_conversion_error(src, "integer / convertable float");
      }
      dest = static_cast<Integer>(v);
    } else if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Integer>(src.get<flex_int>());
    } else {
      throw_type_conversion_error(src, "integer");
    }
  }

  template <typename Integer>
  static void set(Integer& dest, const flexible_type& src) {
    dest = Integer(flex_int(src));
  }
};


////////////////////////////////////////////////////////////////////////////////

/** Any sequence containers (vectors, lists, deques, etc.) of floating
 *  point values.
 */
template <> struct ft_converter<4> {

  template <typename V> static constexpr bool matches() {
    return (is_sequence_container<V>::value
            && std::is_floating_point<typename first_nested_type<V>::type>::value);
  }

  template <typename Vector>
  static void get(Vector& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::VECTOR) {
      const flex_vec& v = src.get<flex_vec>();
      dest.assign(v.begin(), v.end());
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& f = src.get<flex_list>();
      dest.assign(f.begin(), f.end());
    } else {
      throw_type_conversion_error(src, "flex_vec");
    }
  }

  template <typename Vector>
  static void set(flexible_type& dest, const Vector& src) {
    dest = flex_vec(src.begin(), src.end());
  }
};

////////////////////////////////////////////////////////////////////////////////

/** flex_date_time
 */
template <> struct ft_converter<5> {

  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flex_date_time>::value;
  }

  static void get(flex_date_time& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::DATETIME) {
      dest = src.get<flex_date_time>();
    } else {
      throw_type_conversion_error(src, "flex_date_time");
    }
  }

  static void set(flexible_type& dest, const flex_date_time& src) {
    dest = src;
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Any sequence containers of pairs of flexible type convertable
 *  values (may be numeric).  Put into a dictionary.
 */
template <> struct ft_converter<6> {

  template <typename T> static constexpr bool matches() {
    typedef typename first_nested_type<T>::type pair_type;

    return conditional_test<is_sequence_container<T>::value && is_std_pair<pair_type>::value,
                            is_flexible_type_convertible, pair_type>::value;
  }

  template <typename T, typename U, template <typename...> class C>
  static void get(C<std::pair<T, U> >& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::DICT) {
      const flex_dict& fd = src.get<flex_dict>();
      dest.resize(fd.size());
      for(size_t i = 0; i < fd.size(); ++i) {
        convert_from_flexible_type(dest[i].first, fd[i].first);
        convert_from_flexible_type(dest[i].second, fd[i].second);
      }
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& fl = src.get<flex_list>();
      dest.resize(fl.size());
      for(size_t i = 0; i < fl.size(); ++i) {
        convert_from_flexible_type(dest[i], fl[i]);
      }
    } else {
      throw_type_conversion_error(src, "flex_dict or flex_list of 2-element list/vectors");
    }
  }

  template <typename T, typename U, template <typename...> class C>
  static void set(flexible_type& dest, const C<std::pair<T, U> >& src) {
   flex_dict d(src.size());
    for(size_t i = 0; i < src.size();++i) {
      d[i] = std::make_pair(convert_to_flexible_type(src[i].first), convert_to_flexible_type(src[i].second));
    }
    dest = std::move(d);
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Any pairs of numeric values. This is different from the case below
 *  since this allows the the std::pair to convert to and from a
 *  2-element flex_vec if the types in the std::pair are numeric.
 *  Internally, the logic is the same as the case below if they are
 *  not strictly doubles.
 */
template <> struct ft_converter<7> {

  template <typename T> static constexpr bool matches() {
    typedef typename first_nested_type<T>::type  U1;
    typedef typename second_nested_type<T>::type U2;

    return (is_std_pair<T>::value
            && std::is_convertible<double, U1>::value
            && std::is_convertible<U1, double>::value
            && std::is_convertible<double, U2>::value
            && std::is_convertible<U2, double>::value);
  }

  template <typename T, typename U>
  static void get(std::pair<T, U>& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& l = src.get<flex_list>();
      if(l.size() != 2) {
        throw_type_conversion_error(src, "2-element flex_list/flex_vec (list size != 2)");
      }
      convert_from_flexible_type(dest.first, l[0]);
      convert_from_flexible_type(dest.second, l[1]);
    } else if(src.get_type() == flex_type_enum::VECTOR) {
      const flex_vec& v = src.get<flex_vec>();
      if(v.size() != 2){
        throw_type_conversion_error(src, "2-element flex_list/flex_vec (vector size != 2)");
      }
      dest.first = static_cast<T>(v[0]);
      dest.second = static_cast<U>(v[1]);
    } else {
      throw_type_conversion_error(src, "2-element flex_list/flex_vec");
    }
  }

  template <typename T, typename U>
  static void set(flexible_type& dest, const std::pair<T,U>& src) {
    if(std::is_floating_point<T>::value && std::is_floating_point<U>::value) {
      dest = flex_vec{flex_float(src.first), flex_float(src.second)};
    } else {
      dest = flex_list{convert_to_flexible_type(src.first), convert_to_flexible_type(src.second)};
    }
  }
};

////////////////////////////////////////////////////////////////////////////////

/** std::pair of flexible_type convertable stuff.
 */
template <> struct ft_converter<8> {

  template <typename T> static constexpr bool matches() {
    typedef typename first_nested_type<T>::type  U1;
    typedef typename second_nested_type<T>::type U2;

    return (is_std_pair<T>::value
            && conditional_test<is_std_pair<T>::value, is_flexible_type_convertible, U1>::value
            && conditional_test<is_std_pair<T>::value, is_flexible_type_convertible, U2>::value);
  }

  template <typename T, typename U>
  static void get(std::pair<T, U>& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& l = src.get<flex_list>();
      if(l.size() != 2) {
        throw_type_conversion_error(src, "2-element flex_list/flex_vec (list size != 2)");
      }
      convert_from_flexible_type(dest.first, l[0]);
      convert_from_flexible_type(dest.second, l[1]);
    } else {
      throw_type_conversion_error(src, "2-element flex_list/flex_vec");
    }
  }

  template <typename T, typename U>
  static void set(flexible_type& dest, const std::pair<T,U>& src) {
    dest = flex_list{convert_to_flexible_type(src.first), convert_to_flexible_type(src.second)};
  }
};

////////////////////////////////////////////////////////////////////////////////

/** std::map<T, U>, std::unordered_map<T, U>, or boost::unordered_map,
 *  with T and U convertable to flexible_type.
 */
template <> struct ft_converter<9> {

  template <typename T> static constexpr bool matches() {

    typedef typename first_nested_type<T>::type U1;
    typedef typename second_nested_type<T>::type U2;

    return (is_map<T>::value
            && conditional_test<is_map<T>::value, is_flexible_type_convertible, U1>::value
            && conditional_test<is_map<T>::value, is_flexible_type_convertible, U2>::value);
  }

  template <typename T>
  static void get(T& dest, const flexible_type& src) {
    std::pair<typename T::key_type, typename T::mapped_type> p;

    if(src.get_type() == flex_type_enum::DICT) {

      const flex_dict& fd = src.get<flex_dict>();

      for(size_t i = 0; i < fd.size(); ++i) {
        convert_from_flexible_type(p.first, fd[i].first);
        convert_from_flexible_type(p.second, fd[i].second);
        dest.insert(std::move(p));
      }
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& l = src.get<flex_list>();
      for(size_t i = 0; i < l.size(); ++i) {
        convert_from_flexible_type(p, l[i]);
        dest.insert(std::move(p));
      }
    } else {
      throw_type_conversion_error(src, "flex_dict / list of 2-element flex_lists/flex_vec");
    }
  }

  template <typename T>
  static void set(flexible_type& dest, const T& src) {
    flex_dict fd;
    fd.reserve(src.size());
    for(const auto& p : src) {
      fd.push_back({convert_to_flexible_type(p.first), convert_to_flexible_type(p.second)});
    }
    dest = std::move(fd);
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Any string types.
 */
template <> struct ft_converter<10> {

  template <typename T> static constexpr bool matches() {
    return is_string<T>::value;
  }

  template <typename String>
  static void get(String& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::STRING) {
      const flex_string& s = src.get<flex_string>();
      dest.assign(s.begin(), s.end());
    } else {
      flex_string s = src.to<flex_string>();
      dest.assign(s.begin(), s.end());
    }
  }

  template <typename String>
  static void set(flexible_type& dest, const String& src) {
    dest = flex_string(src.begin(), src.end());
  }
};

////////////////////////////////////////////////////////////////////////////////

/** Any sequence container of values that are convertable to a
 * flexible type, and are not covered by cases 1-9.
 */
template <> struct ft_converter<11> {

  template <typename T> static constexpr bool matches() {
    return (conditional_test<
               is_sequence_container<T>::value,
               is_flexible_type_convertible, typename first_nested_type<T>::type>::value);
  }

  template <typename FlexContainer>
  static void get(FlexContainer& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& fl = src.get<flex_list>();
      dest.resize(fl.size());
      auto it = dest.begin();
      for(size_t i = 0; i < fl.size(); ++i, ++it) {
        typename FlexContainer::value_type t;
        convert_from_flexible_type(t, fl[i]);
        *it = std::move(t);
      }
    } else {
      throw_type_conversion_error(src, "flex_list");
    }
  }

  template <typename FlexContainer>
  static void set(flexible_type& dest, const FlexContainer& src) {
    flex_list fl(src.size());

    auto it = src.begin();
    for(size_t i = 0; i < fl.size(); ++i, ++it) {
      fl[i] = convert_to_flexible_type(*it);
    }

    dest = std::move(fl);
  }
};

////////////////////////////////////////////////////////////////////////////////

/** std::tuple of numeric values.
 */
template <> struct ft_converter<12> {

  template <typename T> static constexpr bool matches() {
    return (is_tuple<T>::value && all_nested_true<std::is_arithmetic, T>::value);
  }

  template <typename... Args>
  static void get(std::tuple<Args...>& dest, const flexible_type& src) {
    switch(src.get_type()) {
      case flex_type_enum::LIST: {
        const flex_list& d = src.get<flex_list>();

        if (d.size() != sizeof...(Args)) {
          std::string errormsg =
              std::string("Expecting a list or vector of length ")
              + std::to_string(sizeof...(Args)) + ", but we got a list of length "
              + std::to_string(d.size());
          throw(errormsg);
        }
        pack_tuple(dest, d);
        break;
      }

      case flex_type_enum::VECTOR: {
        const flex_vec& d = src.get<flex_vec>();
        if (d.size() != sizeof...(Args)) {
          std::string errormsg =
              std::string("Expecting a list or vector of length ")
              + std::to_string(sizeof...(Args)) + ", but we got a vector of length "
              + std::to_string(d.size());
          throw(errormsg);
        }
        pack_tuple(dest, d);
        break;
      }

      default: {
        std::string errormsg =
            std::string("Expecting a list or vector of length ")
            + std::to_string(sizeof...(Args)) + ", but we got a "
            + flex_type_enum_to_name(src.get_type());
        throw(errormsg);
      }
    }
  }

  template <typename... Args>
  static void set(flexible_type& dest, const std::tuple<Args...> & src) {
    flex_vec v(sizeof...(Args));
    unpack_tuple(v, src);
    dest = std::move(v);
  }
};

////////////////////////////////////////////////////////////////////////////////

/** std::tuple of flexible_type-convertable values.  (Note that tuples
 * of arithmetic values are taken care of by the previous case).
 */
template <> struct ft_converter<13> {

  template <typename T> static constexpr bool matches() {
    return (is_tuple<T>::value && all_nested_true<is_flexible_type_convertible, T>::value);
  }

  template <typename... Args>
  static void get(std::tuple<Args...>& dest, const flexible_type& src) {
    switch(src.get_type()) {
      case flex_type_enum::LIST: {
        const flex_list& d = src.get<flex_list>();

        if (d.size() != sizeof...(Args)) {
          std::string errormsg =
              std::string("Expecting a list or vector of length ")
              + std::to_string(sizeof...(Args)) + ", but we got a list of length "
              + std::to_string(d.size());
          throw(errormsg);
        }
        pack_tuple(dest, d);
        break;
      }

      default: {
        std::string errormsg =
            std::string("Expecting a list or vector of length ")
            + std::to_string(sizeof...(Args)) + ", but we got a "
            + flex_type_enum_to_name(src.get_type());
        throw(errormsg);
      }
    }
  }

  template <typename... Args>
  static void set(flexible_type& dest, const std::tuple<Args...> & src) {
    flex_list v(sizeof...(Args));
    unpack_tuple(v, src);
    dest = std::move(v);
  }
};

////////////////////////////////////////////////////////////////////////////////

/**  Enum types.
 */
template <> struct ft_converter<14> {

  template <typename T> static constexpr bool matches() {
    return std::is_enum<T>::value;
  }

  template <typename Enum>
  static void get(Enum& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Enum>(src.get<flex_int>());
    } else {
      throw_type_conversion_error(src, "integer / enum.");
    }
  }

  template <typename Enum>
  static void set(flexible_type& dest, const Enum& val) {
    dest = static_cast<flex_int>(val);
  }
};

static constexpr int last_converter_number = 14;


////////////////////////////////////////////////////////////////////////////////
//
//  All the boilerplate code to make the above work well.
//
////////////////////////////////////////////////////////////////////////////////

template <int idx> struct ft_resolver {

  ////////////////////////////////////////////////////////////////////////////////
  // Do any of the converters of this idx or lower match on this one?

  template <typename T> static constexpr bool any_match(
      typename std::enable_if<ft_converter<idx>::template matches<T>()>::type* = NULL) {
    return true;
  }

  template <typename T> static constexpr bool any_match(
      typename std::enable_if<!ft_converter<idx>::template matches<T>()>::type* = NULL) {
    return ft_resolver<idx - 1>::template any_match<T>();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Does this idx, and none before this one, match?

  template <typename T> static constexpr bool matches() {
      return (ft_converter<idx>::template matches<T>()
              && !ft_resolver<idx - 1>::template any_match<T>());
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Does this idx, and none before this one, match?

  template <typename T>
  static void get(T& t, const flexible_type& v,
                  typename std::enable_if<matches<T>()>::type* = NULL) {
    ft_converter<idx>::get(t, v);
  }

  template <typename T>
  static void get(T& t, const flexible_type& v,
                  typename std::enable_if<!matches<T>()>::type* = NULL) {
    ft_resolver<idx - 1>::get(t, v);
  }

  template <typename T>
  static void set(flexible_type& t, const T& v,
                  typename std::enable_if<matches<T>()>::type* = NULL) {
    ft_converter<idx>::set(t, v);
  }

  template <typename T>
  static void set(flexible_type& t, const T& v,
                  typename std::enable_if<!matches<T>()>::type* = NULL) {
    ft_resolver<idx - 1>::set(t, v);
  }
};

template <> struct ft_resolver<0> {
  template <typename T> static constexpr bool any_match() { return false; }
  template <typename T> static constexpr bool matches() { return false; }
  template <typename... T> static void set(T...) {}
  template <typename... T> static void get(T...) {}
};

}

/**
 * is_flexible_type_convertible<T>::value is true if T can be converted
 * to and from a flexible_type via flexible_type_converter<T>.
 */
template <typename T>
struct is_flexible_type_convertible {
  static constexpr bool value = flexible_type_internals::ft_resolver<
    flexible_type_internals::last_converter_number >::template any_match<T>();
};

template <typename T> GL_HOT_INLINE_FLATTEN
static void convert_from_flexible_type(T& t, const flexible_type& f) {
  static_assert(is_flexible_type_convertible<T>::value, "Type not convertable from flexble_type.");

  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::get(t, f);
};

template <typename T> GL_HOT_INLINE_FLATTEN
static void convert_to_flexible_type(flexible_type& f, const T& t) {
  static_assert(is_flexible_type_convertible<T>::value, "Type not convertable to flexble_type.");

  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::set(f, t);
};

template <typename T> GL_HOT_INLINE_FLATTEN
static flexible_type convert_to_flexible_type(const T& t) {
  static_assert(is_flexible_type_convertible<T>::value, "Type not convertable to flexble_type.");

  flexible_type f;
  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::set(f, t);
  return f;
};

/** A class that wraps the above functions in a convenient way for testing.
 */
template <typename T>
struct flexible_type_converter {

  static constexpr bool value = is_flexible_type_convertible<T>::value;

  flexible_type set(const T& t) const { return convert_to_flexible_type(t); }
  T get(const flexible_type& f) const {
    T t;
    convert_from_flexible_type(t, f);
    return t;
  }
};

/** A convenience class that is true if all arguments are flexible type convertable.
 */
template <typename... Args>
struct all_flexible_type_convertible {
  static constexpr bool value = all_true<is_flexible_type_convertible, Args...>::value;
};

} // namespace graphlab 
#endif
