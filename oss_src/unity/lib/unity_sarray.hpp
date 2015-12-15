/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SARRAY_HPP
#define GRAPHLAB_UNITY_SARRAY_HPP

#include <vector>
#include <memory>
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/api/unity_sarray_interface.hpp>

namespace graphlab {

// forward declarations
template <typename T>
class sarray;
template <typename T>
class sarray_reader;
template <typename T>
class sarray_iterator;

namespace query_eval {
class planner_node;
} // query_eval

/**
 * This is the SArray object exposed to Python. Abstractly, it stores a
 * single column of a flexible_type. An Sarray represents a single immutable
 * column: i.e. once created, it cannot be modified.
 *
 * Internally, it is represented as a single shared_ptr to an
 * \ref "sarray<flexible_type>" sarray object. We delay construction of the
 * internal sarray object until a "construct" call is made. This allows the
 * class to be used in the following way:
 *
 * \code
 * unity_sarray array;
 * // creates the array
 * array.construct(...);
 * // now the array is immutable.
 * \endcode
 *
 * Multiple different construct functions can then be used to create sarrays
 * from different sources: some sources may require the sarray to download
 * files, etc.
 *
 * The SArray may require temporary on disk storage which will be deleted when
 * the SArray is deleted. The temporary file names are obtained from
 * \ref graphlab::get_temp_name
 */
class unity_sarray: public unity_sarray_base {

 public:
  /** Default Constructor.
   * Does nothing basically. Use one of the construct_from_* functions to
   * construct the contents of the SArray.
   */
  unity_sarray();

  /// Destructor. Deletes all temporary sarray files created.
  ~unity_sarray();

  unity_sarray(const unity_sarray& other);

  unity_sarray& operator=(const unity_sarray& other);

  /**
   * Constructs an Sarray from an in memory vector.
   * If the current object is already storing an array, it is cleared
   * (\ref clear()).May throw an exception on failure. If an exception occurs,
   * the contents of SArray is empty.
   */
  void construct_from_vector(const std::vector<flexible_type>& vec, flex_type_enum type);

  /**
   * Constructs a unity_sarray from an existing sarray.
   * This simply sets this class's shared_ptr to the one given by the parameter.
   */
  void construct_from_sarray(std::shared_ptr<sarray<flexible_type>> s_ptr);

  /**
   * Constructs a unity_sarray from a const value.
   */
  void construct_from_const(const flexible_type& value, size_t size);

  /**
   * Constructs a unity_sarray from a parallel iterator generator.
   */
   void construct_from_planner_node(std::shared_ptr<query_eval::planner_node> node); 
                                    

  /**
   * Constructs an Sarray from an existing directory on disk saved with
   * save_array() or a on disk sarray prefix (saved with
   * save_array_by_index_file()). This function will automatically detect if
   * the location is a directory, or a file. The files will not be deleted on
   * destruction.  If the current object is already storing an array, it is
   * cleared (\ref clear()). May throw an exception on failure. If an exception
   * occurs, the contents of SArray is empty.
   */
  void construct_from_sarray_index(std::string location);

  /**
   * Constructs an Sarray from a url. Each line of the file will be a row in the
   * resultant SArray, and each row will be of string type.
   * If the current object is already storing an array, it is cleared
   * (\ref clear()).May throw an exception on failure. If an exception occurs,
   * the contents of SArray is empty.
   */
  void construct_from_files(std::string url, flex_type_enum type);

  /**
   * Given a URL, this function attempts to autodetect if it should
   *  - treat it as a .sidx file and load an SArray from it (construct_from_sarray_index)
   *  - treat it as a file to read line by line (construct_from_files)
   *  - treat it as a directory and load an SArray from it (construct_from_sarray_index)
   */
  void construct_from_autodetect(std::string url, flex_type_enum type);

  /**
   * Given a URL to an Avro (http://avro.apache.org/) file, read each record into SArray
   * of type corresponding to the Avro schema type.
   */
  void construct_from_avro(std::string url);

  /**
   * Saves a copy of the current sarray into a directory.
   * Does not modify the current sarray
   */
  void save_array(std::string target_directory);


  /**
   * Saves a copy of the current sarray into a target location defined by
   * an index file. DOes not modify the current sarray.
   */
  void save_array_by_index_file(std::string index_file);

  /**
   * Clears the contents of the SArray, deleting all temporary files if any.
   */
  void clear();

  /**
   * Returns the number of rows in the SArray. Or 0 if the SArray is empty.
   */
  size_t size();

  /**
   * Returns true if size is obtainable efficiently. 
   */
  bool has_size();

  /**
   * Obtains the underlying sarray pointer.
   * TODO: will slowly move away all users of this function to get_lazy_sarray
   */
  std::shared_ptr<sarray<flexible_type> > get_underlying_sarray();

  /**
   * Returns the underlying planner pointer
   */
  std::shared_ptr<query_eval::planner_node> get_planner_node();

  /**
   * Returns some number of rows of the SArray
   *
   * NOTE: If there are more elements asked for than can fit into
   * memory, this makes no attempt to stop crashing your computer.
   */
  std::shared_ptr<unity_sarray_base> head(size_t nrows);

  /**
   * Same as head, return vector<flexible_type>, used for testing.
   */
  std::vector<flexible_type> _head(size_t nrows) {
    auto result = head(nrows);
    auto ret = result->to_vector();
    return ret;
  }

  /**
   * Returns the type name of the SArray
   */
  flex_type_enum dtype();

  /**
   * Returns a new sarray which is a transform of this using a Python lambda
   * function pickled into a string.
   */
  std::shared_ptr<unity_sarray_base> transform(const std::string& lambda,
                                               flex_type_enum type,
                                               bool skip_undefined,
                                               int seed);

  /**
   * Returns a new sarray which is a transform of this using a registered
   * toolkit function.
   */
  std::shared_ptr<unity_sarray_base> transform_native(
      const function_closure_info& closure,
      flex_type_enum type,
      bool skip_undefined,
      int seed);

  std::shared_ptr<unity_sarray_base> transform_lambda(std::function<flexible_type(const flexible_type&)> lambda,
                                                      flex_type_enum type,
                                                      bool skip_undefined,
                                                      int seed);

  /**
   * Append all rows from "other" sarray to "this" sarray and returns a new sarray
   * that contains all rows from both sarrays
   */
  std::shared_ptr<unity_sarray_base> append(std::shared_ptr<unity_sarray_base> other);

  /**
   * If this sarray contains vectors, this returns a new sarray comprising of a
   * vertical slice of the vector from position start (inclusive) to position
   * end (exclusive). Throws an exception if the sarray is not an vector.
   *
   * If end==(start+1), the output is an SArray of doubles. if end > start,
   * the output is an SArray of vectors, each of length (end - start).
   * If a vector cannot be sliced (for instance the length of the vector is less
   * than end), the resultant value will be UNDEFINED.
   *
   * End must be greater than start. throws an exception otherwise.
   */
  std::shared_ptr<unity_sarray_base> vector_slice(size_t start, size_t end);


  /**
   * Returns a new SArray which is filtered to by the given lambda function.
   * If the lambda evaluates an element to true, this element is copied to the
   * new SArray.  If not, it isn't.  Throws an exception if the return type
   * of the lambda is not castable to a boolean value.
   */
  std::shared_ptr<unity_sarray_base> filter(const std::string& lambda, bool skip_undefined, int seed);


  /**
   * Returns a new SArray which is filtered by a given logical column.
   * The index array must be the same length as the current array. An output
   * array is returned containing only the elements in the current where are the
   * corresponding element in the index array evaluates to true.
   */
  std::shared_ptr<unity_sarray_base> logical_filter(std::shared_ptr<unity_sarray_base> index);

  /**
   * Returns a new SArray which has the top k elements selected.
   * k should be reasonably small. O(k) memory required.
   *
   * If reverse if true, the bottom k is returned instead
   */
  std::shared_ptr<unity_sarray_base> topk_index(size_t k, bool reverse);


  /**
   * Returns true if all the values in the sarray are non-zero / non-empty. An
   * empty array returns true.
   */
  bool all();

  /**
   * Returns true if any value in the sarray is non-zero / non-empty. An
   * empty array returns false.
   */
  bool any();
  
  /**
   * Creates a new SArray with the datetime values casted to string. 
   *
   * "format" determines the string format for the output SArray. 
   */
  std::shared_ptr<unity_sarray_base> datetime_to_str(std::string format);
 
  /**
   * Creates a new SArray with the string values casted to datetime.
   * 
   * "format" determines the string format for the input SArray. 
   */
  std::shared_ptr<unity_sarray_base> str_to_datetime(std::string format);

  /**
   * Creates a new SArray with the same values as current one, but casted to
   * the given type.
   *
   * If undefined_on_failure is set, cast failures do not cause errors, but
   * become undefined values.
   *
   * \see lazy_astype
   */
  std::shared_ptr<unity_sarray_base> astype(flex_type_enum dtype, bool undefined_on_failure = false);

  /**
   * Creates a new SArray with the same values as current one, but casted to
   * the given type. Performed lazily.
   *
   * If undefined_on_failure is set, cast failures do not cause errors, but
   * become undefined values.
   * \see astype
   */
  std::shared_ptr<unity_sarray_base> lazy_astype(flex_type_enum dtype, 
                                                 bool undefined_on_failure = false);

  /**
   * Creates a new SArray with the same values as the current one, except
   * any values above or below the given bounds are changed to be equal
   * to the bound.
   *
   * If lower or upper are given a flex_undefined(), this is interpreted
   * to mean that there is no bound there.  For example,
   * clip(flex_undefined(), 25) clips with no lower bound and an upper bound
   * of 25.
   */
  std::shared_ptr<unity_sarray_base> clip(flexible_type lower = flex_undefined(),
                                          flexible_type upper = flex_undefined());

  /**
   * Returns the largest element in the sarray. An empty array returns
   * flex_undefined, which in python is numpy.nan.  Only works for INTEGER
   * and FLOAT.  Throws an exception if invoked on an sarray of any other type.
   * Undefined values in the array are skipped.
   */
  flexible_type max();

  /**
   * Returns the smallest element in the sarray. An empty array returns
   * flex_undefined, which in python is numpy.nan.  Only works for INTEGER
   * and FLOAT.  Throws an exception if invoked on an sarray of any other type.
   * Undefined values in the array are skipped.
   */
  flexible_type min();

  /**
   * Returns the sum of all elements in the sarray. An empty returns
   * flex_undefined, which in python is numpy.nan.  Only works for INTEGER
   * and FLOAT.  Throws an exception if invoked on an sarray of any other type.
   * Overflows without shame.
   * Undefined values in the array are skipped.
   */
  flexible_type sum();

  /**
   * Returns the mean of the elements in sarray as a flex_float.
   *
   * Invoking on an empty sarray returns flex_undefined.
   * Invoking on a non-numeric type throws an exception.
   * Undefined values in the array are skipped.
   */
  flexible_type mean();

  /**
   * Returns the standard deviation of the elements in sarray as a flex_float.
   *
   * \param ddof ...stands for "delta degrees of freedom".  Adjusts the degrees
   * of freedom in the variance calculation.  If ddof=0, there are N degrees
   * of freedom, with N being the number of elements in the sarray.
   *
   * Throws an exception if:
   *  ddof >= sarray size
   *  sarray is of a non-numeric type
   *
   * Returns flex_undefined if executed on empty or non-existent sarray.
   * Undefined values in the array are skipped.
   */
  flexible_type std(size_t ddof=0);

  /**
   * Returns the variance of the elements in sarray as a flex_float.
   *
   * \param ddof ...stands for "delta degrees of freedom".  Adjusts the degrees
   * of freedom in the variance calculation.  If ddof=0, there are N degrees
   * of freedom, with N being the number of elements in the sarray.
   *
   * Throws an exception if:
   *  ddof >= sarray size
   *  sarray is of a non-numeric type
   *
   * Returns flex_undefined if executed on empty or non-existent SArray.
   * Undefined values in the array are skipped.
   */
  flexible_type var(size_t ddof=0);


  /**
   * Returns the number of missing values in the SArray.
   */
  size_t num_missing();

  /**
   * Returns the number of non-zero elements in the array.
   * Functionally equivalent to
   * \code
   * nonzero().length()
   * \endcode
   * But takes much less memory.
   */
  size_t nnz();


  /**
   * Performs the equivalent of array [op] other , where other is a scalar value.
   * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
   * "<=", ">=", "==", "!=",'%','in'. The type of the new array is dependent on the
   * semantics of the operation.
   *  - comparison operators always return integers
   *  - +,-,* of integer against integers always return integers
   *  - / of integer against integer always returns floats
   *  - +,-,*,/ of floats against floats always return floats
   *  - +,-,*,/ of integer against floats or floats against integers
   *            always return floats.
   *  - %. integer against integers -> integers
   *  - "in". must be string against string, and returns a boolean.
   *
   * This function throws a string exception if there is a type mismatch (
   * for instance you cannot add a string value to an integer array), or if
   * the operation is invalid.
   *
   * UNDEFINED values in the array are ignored.
   *
   * On success, a new array is returned. The new array is the same length and
   * has the same segment structure.
   */
  std::shared_ptr<unity_sarray_base> left_scalar_operator(flexible_type other,
                                                          std::string op);

  /**
   * Performs the equivalent of other [op] array, where other is a scalar value.
   * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
   * "<=", ">=", "==", "!=". The type of the new array is dependent on the
   * semantics of the operation.
   *  - comparison operators always return integers
   *  - +,-,* of integer against integers always return integers
   *  - / of integer against integer always returns floats
   *  - +,-,*,/ of floats against floats always return floats
   *  - +,-,*,/ of integer against floats or floats against integers
   *            always return floats.
   *
   * This function throws a string exception if there is a type mismatch (
   * for instance you cannot add a string value to an integer array), or if
   * the operation is invalid.
   *
   * UNDEFINED values in the array are ignored.
   *
   * On success, a new array is returned. The new array is the same length and
   * has the same segment structure.
   */
  std::shared_ptr<unity_sarray_base> right_scalar_operator(flexible_type other, std::string op);



  /**
   * Performs the equivalent of array [op] other, where other is an SArray.
   * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
   * "<=", ">=", "==", "!=". The type of the new array is dependent on the
   * semantics of the operation.
   *  - comparison operators always return integers
   *  - +,-,* of integer against integers always return integers
   *  - / of integer against integer always returns floats
   *  - +,-,*,/ of floats against floats always return floats
   *  - +,-,*,/ of integer against floats or floats against integers
   *            always return floats.
   *
   * This function throws a string exception if there is a type mismatch (
   * for instance you cannot add a string value to an integer array), or if
   * the operation is invalid.
   *
   * UNDEFINED values in the array are ignored.
   *
   * On success, a new array is returned. The new array is the same length and
   * has the same segment structure.
   */
  std::shared_ptr<unity_sarray_base> vector_operator(
      std::shared_ptr<unity_sarray_base> other, std::string op);

  /**
   * Returns a new array with all UNDEFINED values removed.
   * A new array is returned with the same type as the current array, but
   * potentially shorter. If the array has no missing values, the output array
   * has the same length and the same segment structure as this array.
   */
  std::shared_ptr<unity_sarray_base> drop_missing_values();

  /**
   * Returns a new array with all UNDEFINED values replaced with the given value.
   *
   * Throws if the given value is not convertible to the SArray's type.
   */
  std::shared_ptr<unity_sarray_base> fill_missing_values(flexible_type default_value);

  /**
   * Returns some number of rows on the end of the SArray.  The values are
   * returned in the order they were found in the SArray.
   *
   * NOTE: If there are more elements asked for than can fit into
   * memory, this makes no attempt to stop crashing your computer.
   */
  std::shared_ptr<unity_sarray_base> tail(size_t nrows=10);

  std::vector<flexible_type> _tail(size_t nrows=10) {
    auto result = tail(nrows);
    auto ret = result->to_vector();
    return ret;
  }

  /**
   * Returns a uniform random sample of the sarray, that contains percent of
   * the total elements, without replacement, using the random_seed.
   */
  std::shared_ptr<unity_sarray_base> sample(float percent, int random_seed);


  /**
   * Do a word-count for each element in the SArray and return a SArray of dictionary
   **/
  std::shared_ptr<unity_sarray_base> count_bag_of_words(std::map<std::string, flexible_type> options);

   /**
   * Do a character n-gram count for each element in the SArray and return a SArray of dictionary type.
   * Parameter n is the number or charachters in each n-gram
   * options takes: to_lower, which makes words lower case
   *                ignore_space, which ignores spaces in calculating charachter n-grams
   **/
  std::shared_ptr<unity_sarray_base> count_character_ngrams(size_t n, std::map<std::string, flexible_type> options);


   /**
   * Do a character n-gram count for each element in the SArray and return a SArray of dictionary type.
   * Parameter n is the number of words in each n-gram
   * options takes: to_lower, which makes words lower case
   **/
  std::shared_ptr<unity_sarray_base> count_ngrams(size_t n, std::map<std::string, flexible_type> options);

  /**
  * If SArray dtype is dict, filter out each dict by the given keys.
  * If exclude is True, then all keys that are in the input key list are removed
  * If exclude is False, then only keys that are in the input key list are retained
  **/
  std::shared_ptr<unity_sarray_base> dict_trim_by_keys(const std::vector<flexible_type>& keys, bool exclude);

  /**
  * If SArray dtype is dict, filter out each dict by the given value boundary.
  * all items whose value is not in the low/up bound are removed from the dictionary
  * The boundary are included. I.e, if a value is either lower or upper bound, then
  * the key/value pair is included in the result
  * This function will fail if the value is not comparable
  **/
  std::shared_ptr<unity_sarray_base> dict_trim_by_values(const flexible_type& lower, const flexible_type& upper);

  /**
  * If SArray dtype is dict, returns a new SArray which contains keys for input dictionary
  * otherwise throws exception
  **/
  std::shared_ptr<unity_sarray_base> dict_keys();

  /**
  * If SArray dtype is dict, returns a new SArray which contains values for input dictionary
  * otherwise throws exception
  **/
  std::shared_ptr<unity_sarray_base> dict_values();

  /**
  * If SArray dtype is dict, returns a new SArray which contains integer of 1s or 0s with 1
  * means the original array element has at least one key in the param
  * otherwise throws exception
  **/
  std::shared_ptr<unity_sarray_base> dict_has_any_keys(const std::vector<flexible_type>& keys);

  /**
  * If SArray dtype is dict, returns a new SArray which contains integer of 1s or 0s with 1
  * means the original array element has all keys in the param
  * otherwise throws exception
  **/
  std::shared_ptr<unity_sarray_base> dict_has_all_keys(const std::vector<flexible_type>& keys);

  /**
   **  Returns a new SArray that contains elements that are the length of each item
   ** in input SArray. This function only works on SArray of type vector, list and
   ** dict. It is equivalent to the following python work
   **     sa_ret = sa.apply(lambda x: len(x))
  **/
  std::shared_ptr<unity_sarray_base> item_length();
  
  /**
   * Expand an SArray of datetime type to a set of new columns.
   *
   * \param column_name_prefix: prefix for the expanded column name
   * \param expanded_column_elements: a list including the elements to expand 
   *  from the datetime column. Elements could be 'year','month','day'
   *  'hour','minute','second', and 'tzone'.
   * \param expanded_columns_types: list of types for the expanded columns

   * Returns a new SFrame that contains the expanded columns
   **/
  std::shared_ptr<unity_sframe_base> expand(
    const std::string& column_name_prefix,
    const std::vector<flexible_type>& expanded_column_elements,
    const std::vector<flex_type_enum>& expanded_columns_types);
  /**
    * Unpack an SArray of dict/list/vector type to a set of new columns.
    * For dictionary type, each unique key is a new column
    * For vector/list type, each sub column in the vector is a new column
    *
    * \param column_name_prefix: prefix for the unpacked column name
    * \param unpacked_keys: list of keys to unpack, this is list of string for
    *  dictionary type, and list of integers for list/array type. This list is
    *  used to limit the subset of values to unpack
    * \param unpacked_column_types: list of types for the unpacked columns
    * \param na_value: if not undefined, replace all na_value with missing values

    * Returns a new SFrame that contains the unpacked columns
  **/
  std::shared_ptr<unity_sframe_base> unpack(
    const std::string& column_name_prefix,
    const std::vector<flexible_type>& unpacked_keys,
    const std::vector<flex_type_enum>& unpacked_columns_types,
    const flexible_type& na_value);

  /**
    * Unpack a dict SArray to a set of new columns by extracting each key from dict
    * and creating new column for each unique key. The key name becomes column name

    * \param column_name_prefix: prefix for the unpacked column name
    * \param limit: limited keys for the unpack
    * \param na_value: if not undefined, replace all na_value with missing values

    * Returns a new SFrame that contains the unpacked columns
  **/
  std::shared_ptr<unity_sframe_base> unpack_dict(
  const std::string& column_name_prefix,
  const std::vector<flexible_type>& limit,
  const flexible_type& na_value);

  /**
   * Begin iteration through the SArray.
   *
   * Works together with \ref iterator_get_next(). The usage pattern
   * is as follows:
   * \code
   * array.begin_iterator();
   * while(1) {
   *   auto ret = array.iterator_get_next(64);
   *   // do stuff
   *   if (ret.size() < 64) {
   *     // we are done
   *     break;
   *   }
   * }
   * \endcode
   *
   * Note that use of pretty much any of the other data-dependent SArray
   * functions will invalidate the iterator.
   */
  void begin_iterator();

  /**
   * Obtains the next block of elements of size len from the SFrame.
   * Works together with \ref begin_iterator(). See the code example
   * in \ref begin_iterator() for details.
   *
   * This function will always return a vector of length 'len' unless
   * at the end of the array, or if an error has occured.
   *
   * \param len The number of elements to return
   * \returns The next collection of elements in the array. Returns less then
   * len elements on end of file or failure.
   */
  std::vector<flexible_type> iterator_get_next(size_t len);

  /**
   * Return the content as a vector. Convenience function.
   */
  std::vector<flexible_type> to_vector() {
    begin_iterator();
    return iterator_get_next(size());
  }

  /**
   * materialize the sarray, this is different from save() as this is a temporary persist of
   * this sarray to disk to speed up some computation (for example, lambda)
   * this will NOT create a new uity_sarray.
  **/
  void materialize();

  /**
   * test hook to check if the array is materialized
   **/
   bool is_materialized();

   /**
    * Returns an integer which attempts to uniquely identifies the contents of
    * the SArray.
    *
    * This is not generally guaranteed to be actually a unique identifier for
    * the data contents. It certainly tries to be, but both false positives
    * and false negatives can be possible. It tries *really* hard to avoid
    * false positives though.
    *
    * If the array is lazy, it returns a random number.
    * If the array is materialized, it returns a hash of the file names and
    * row sizes that make up the array.
    */
   size_t get_content_identifier();

   /**
    * Extracts a range of rows from an SArray as a new SArray.
    * This will extract rows beginning at start (inclusive) and ending at
    * end(exclusive) in steps of "step".
    * step must be at least 1.
    */
   std::shared_ptr<unity_sarray_base> copy_range(size_t start, size_t step, size_t end);

   static std::shared_ptr<unity_sarray_base> 
       create_sequential_sarray(ssize_t size, ssize_t start, bool reverse);

   static std::shared_ptr<unity_sarray_base> make_uniform_boolean_array(size_t size,
                                                                 float percent,
                                                                 int random_seed);

  std::shared_ptr<unity_sarray_base> rolling_apply(const std::string &fn_name,
                                                   ssize_t before,
                                                   ssize_t after,
                                                   size_t min_observations);

  void save(oarchive& oarc) const;

  void load(iarchive& iarc);
 private:
  /**
   * Pointer to the lazy evaluator logical operator node.
   * This can never be NULL.
   */
  std::shared_ptr<query_eval::planner_node> m_planner_node;

  /**
   * Supports \ref begin_iterator() and \ref iterator_get_next().
   * The next segment I will read. (i.e. the current segment I am reading
   * is iterator_next_segment_id - 1)
   */
  size_t iterator_next_segment_id = 0;


  /**
   * A copy of the current SArray. This allows iteration, and other
   * SAarray operations to operate together safely in harmony without collisions.
   */
  std::unique_ptr<sarray_reader<flexible_type> > iterator_sarray_ptr;

  /**
   * Supports \ref begin_iterator() and \ref iterator_get_next().
   * The begin iterator of the current segment I am reading.
   */
  std::unique_ptr<sarray_iterator<flexible_type>> iterator_current_segment_iter;

  /**
   * Supports \ref begin_iterator() and \ref iterator_get_next().
   * The end iterator of the current segment I am reading.
   */
  std::unique_ptr<sarray_iterator<flexible_type>> iterator_current_segment_enditer;


  /**
   * Performs either of "array [op] other" or "other [op] array",
   * where other is a scalar value.
   * The operation must be one of the following: "+", "-", "*", "/", "<", ">",
   * "<=", ">=", "==", "!=". The type of the new array is dependent on the
   * semantics of the operation.
   *  - comparison operators always return integers
   *  - +,-,* of integer against integers always return integers
   *  - / of integer against integer always returns floats
   *  - +,-,*,/ of floats against floats always return floats
   *  - +,-,*,/ of integer against floats or floats against integers
   *            always return floats.
   *
   * This function throws a string exception if there is a type mismatch (
   * for instance you cannot add a string value to an integer array), or if
   * the operation is invalid.
   *
   * If right_operator is false, array [op] other is performed.
   * Otherwise is right_operator is true, other [op] array is performed.
   *
   * UNDEFINED values in the array are ignored.
   *
   * On success, a new array is returned. The new array
   * is the same length and has the same segment structure.
   */
  std::shared_ptr<unity_sarray_base> scalar_operator(flexible_type other,
                                                     std::string op,
                                                     bool right_operator);


  void construct_from_unity_sarray(const unity_sarray& other);
  
};

} // namespace graphlab

#endif
