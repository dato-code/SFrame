'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import array
import datetime



#/**************************************************************************/
#/*                                                                        */
#/*             Check python type as one of the flexible_type              */
#/*                                                                        */
#/**************************************************************************/

cdef bint py_check_flex_string(type t):
  """Return true if python type t matches flex_string"""
  return t is str or is_string_type(t)

cdef bint py_check_flex_int(type t):
  """Return true if python type t matches flex_int"""
  return t is int or is_integral_type(t)

cdef bint py_check_flex_date_time(type t):
  """Return true if python type t matches pflex_date_time"""
  return t is datetime.datetime;

cdef bint py_check_flex_float(type t):
  """Return true if python type t matches flex_float"""
  return t is float or is_float_type(t)

cdef bint py_check_flex_vec(type t, object v = None):
  """Return true if python type t matches flex_vec"""
  return is_array_type(t) or (t is list and v and len(v) > 0 and all_numeric(v))

cdef bint py_check_flex_list(type t):
  """Return true if python type t matches flex_list"""
  return t is list or is_array_type(t)

cdef bint py_check_flex_dict(type t):
  """Return true if python type t matches flex_dict"""
  return t is dict

"""Return true if python type t matches flex_image"""
cdef bint py_check_flex_image(type t):
  from ..data_structures import image
  return t is image.Image





#/**************************************************************************/
#/*                                                                        */
#/*               python type and flex_type_enum conversion                */
#/*                                                                        */
#/**************************************************************************/
cdef flex_type_enum pytype_to_flex_type_enum(type t) except *:
  """Convert the python type to flex_type_enum"""
  if py_check_flex_int(t):
      return <flex_type_enum>(INTEGER)
  elif py_check_flex_date_time(t):
      return <flex_type_enum>(DATETIME)
  elif py_check_flex_float(t):
      return <flex_type_enum>(FLOAT)
  elif py_check_flex_string(t):
      return <flex_type_enum>(STRING)
  elif py_check_flex_dict(t):
      return <flex_type_enum>(DICT)
  elif py_check_flex_vec(t):
      return <flex_type_enum>(VECTOR)
  elif py_check_flex_list(t):
      return <flex_type_enum>(LIST)
  elif py_check_flex_image(t):
      return <flex_type_enum>(IMAGE)
  elif (t == type(None)):
      return <flex_type_enum>(UNDEFINED)
  else:
      raise TypeError("Cannot convert python type %s to flexible type" % str(t))

cdef type pytype_from_flex_type_enum(flex_type_enum t):
  """Convert flex_type_enum to python type"""
  from ..data_structures import image
  cdef char c = <char>(t)
  if (c == INTEGER):
      return int
  elif (c == FLOAT):
      return float
  elif (c == DATETIME):
      return datetime.datetime
  elif (c == STRING):
      return str
  elif c == LIST:
      return list
  elif c == VECTOR:
      return array.array
  elif (c == DICT):
      return dict
  elif (c == IMAGE):
      return image.Image
  elif (c == UNDEFINED):
      return type(None)
  else:
      raise TypeError("Cannot convert flexible type %s to python type" % str(flex_type_enum_to_name(t)))

cpdef pytype_from_dtype(object dt):
    """Convert the numpy dtype to python type"""
    if 'int' in dt.name or 'long' in dt.name or 'bool' in dt.name:
        return int
    elif 'datetime' in dt.name:
        return datetime.datetime
    elif 'float' in dt.name or 'double' in dt.name:
        return float
    elif 'object' in dt.name:
        return object	
    else:
        raise TypeError('Numpy type %s not supported' % dt.name)

INTEGRAL_TYPE_NAMES = set(['int', 'long', 'bool', 'int8', 'int16', 'int32', 'int64', 'uint', 'uint8', 'uint16', 'uint32', 'uint64', 'short', 'bool_'])
FLOAT_TYPE_NAMES = set(['float', 'double', 'float16', 'float32', 'float64'])
STRING_TYPE_NAMES = set(['string', 'string_', 'unicode', 'unicode_'])
ARRAY_TYPE_NAMES = set(['array', 'ndarray'])

def is_integral_type(type t):
    return t.__name__ in INTEGRAL_TYPE_NAMES

def is_float_type(type t):
    return t.__name__ in FLOAT_TYPE_NAMES

def is_string_type(type t):
    return t.__name__ in STRING_TYPE_NAMES

def is_array_type(type t):
    return t.__name__ in ARRAY_TYPE_NAMES

def is_numeric_type(type t):
    return is_integral_type(t) or is_float_type(t)

def infer_type_of_list(data):
    # default is float
    if len(data) == 0:
        return float
    unique_types = set([type(x) for x in data if x is not None])
    # convert unicode to str
    if unicode in unique_types:
      unique_types = set([x if x is not unicode else str for x in unique_types])

    if len(unique_types) == 0:
        return float
    elif len(unique_types) == 1:
        ret = unique_types.pop()
        # only 1 type. check against basic types,
        # and cast it back to int or float
        if is_integral_type(ret):
            return int
        elif py_check_flex_date_time(ret):
            return datetime.datetime
        elif is_float_type(ret):
            return float
        elif ret != list:
            # none of the above. its not a list of lists, so its fine.
            # We can handle the other cases.
            return ret

        # if it is a list of lists, we need to look harder at the contents
        # of each list.
        # if all contents of the list is numeric, then use vector
        # otherwise use list
        # empty list is treated as list
        value_types = set([])
        for l in data:
            if l is None:
                continue
            value_types = value_types.union(set([type(x) for x in l]))

        if len(value_types) > 0 and all(is_numeric_type(t) for t in value_types):
            return array.array
        else:
            return list

    elif len(unique_types) == 2:
        # we can do both ints, longs, floats as a float
        if all(is_numeric_type(t) for t in unique_types):
            return float
        elif all(py_check_flex_list(t) for t in unique_types):
            return list 
        else:
            raise TypeError("Cannot infer Array type. Not all elements of array are the same type.")
    else:
        raise TypeError("Cannot infer Array type. Not all elements of array are the same type.")

cdef bint is_int_memory_view(object v):
    cdef int[:] int_template
    try:
        int_template = v
        return True
    except:
        return False

cdef bint is_long_memory_view(object v):
    cdef long[:] long_template
    try:
        long_template = v
        return True
    except:
        return False

cdef bint is_float_memory_view(object v):
    cdef float[:] float_template
    try:
        float_template = v
        return True
    except:
        return False

cdef bint is_double_memory_view(object v):
    cdef double[:] double_template
    try:
        double_template = v
        return True
    except:
        return False

cdef bint is_numeric_memory_view(object v):
    return is_int_memory_view(v) or is_long_memory_view(v) \
           or is_float_memory_view(v) or is_double_memory_view(v)

cdef bint all_numeric(object v):
    return all([is_numeric_type(type(ele)) for ele in v])
