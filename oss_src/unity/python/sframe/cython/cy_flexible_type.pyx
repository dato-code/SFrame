'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from cython.operator cimport dereference as deref
from cython.operator cimport preincrement as inc
import array
import calendar,datetime

from ..util import timezone 
from datetime import tzinfo
from datetime import timedelta

cdef flexible_type flexible_type_from_pyobject(object v) except *:
    """
    Converting python object into flexible_type
    """
    cdef type t = type(v)
    cdef flex_vec array_value
    cdef flex_list recursive_value
    cdef flex_dict dict_value
    cdef flex_image image_value
    cdef flexible_type ret
    if py_check_flex_int(t):
        ret.set_int(v)
    elif py_check_flex_date_time(t):
         if(v.year < 1400 or v.year > 10000):
             raise TypeError('Year is out of valid range: 1400..10000')
         if(v.tzinfo != None):
             offset = int(v.tzinfo.utcoffset(v).total_seconds() / TIMEZONE_RESOLUTION_IN_SECONDS) # store timezone offset at the granularity of half an hour 
             ret.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),offset), v.microsecond)
         else:
             ret.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),EMPTY_TIMEZONE), v.microsecond)
    elif py_check_flex_float(t):
        ret.set_double(v)
    elif py_check_flex_string(t):
        if t is unicode:
            ret.set_string(v.encode('utf-8'))
        else:
            ret.set_string(v)
    elif py_check_flex_vec(t, v):
        array_value = make_flex_vec(v)
        ret.set_vec(array_value)
    elif py_check_flex_list(t):
        recursive_value.resize(len(v))
        for i in range(len(v)):
            recursive_value[i] = flexible_type_from_pyobject(v[i])
        ret.set_recursive(recursive_value)
    elif py_check_flex_dict(t):
        iter = v.iteritems()
        dict_value.resize(len(v))
        for i in range(len(v)):
            key, value = iter.next()
            dict_value[i].first = flexible_type_from_pyobject(key)
            dict_value[i].second = flexible_type_from_pyobject(value)
        ret.set_dict(dict_value)
    elif py_check_flex_image(t):
        image_value = make_flex_image(v)
        ret.set_img(image_value)
    elif v is None:
        ret = FLEX_UNDEFINED
    else:
        raise TypeError('Cannot convert python object %s of type %s to flexible_type' % (str(v), type(v)))
    return ret


cdef flexible_type flexible_type_from_pyobject_with_type_force(object v, type force) except *:
    """
    Converting python object into flexible_type, forcing the object to be
    interpreted as a particular type. t must be one of the types returned by
    pytype_from_flex_type_enum(flex_type_enum t).
    This function will still fail if the object cannot be safely converted.
    """
    cdef type t = type(v)
    cdef flex_vec array_value
    cdef flex_list recursive_value
    cdef flex_dict dict_value
    cdef flexible_type ret
    from ..data_structures import image
    if force is int and py_check_flex_int(t):
        ret.set_int(v)
    elif force is datetime.datetime and py_check_flex_date_time(t):
        if(v.year < 1400 or v.year > 10000):
            raise TypeError('Year is out of valid range: 1400..10000')
        if(v.tzinfo != None):
            offset = int(v.tzinfo.utcoffset(v).total_seconds() / TIMEZONE_RESOLUTION_IN_SECONDS) #store timezone offset at the granularity of half an hour.
            ret.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),offset), v.microsecond)
        else:
            ret.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),EMPTY_TIMEZONE), v.microsecond)
    elif force is float and py_check_flex_float(t):
        ret.set_double(v)
    elif force is str and py_check_flex_string(t):
        if t is unicode:
            ret.set_string(v.encode('utf-8'))
        else:
            ret.set_string(v)
    elif force is array.array and py_check_flex_vec(t, v):
        array_value = make_flex_vec(v)
        ret.set_vec(array_value)
    elif force is list and py_check_flex_list(t):
        recursive_value.resize(len(v))
        for i in range(len(v)):
            recursive_value[i] = flexible_type_from_pyobject(v[i])
        ret.set_recursive(recursive_value)
    elif force is dict and py_check_flex_dict(t):
        iter = v.iteritems()
        dict_value.resize(len(v))
        for i in range(len(v)):
            key, value = iter.next()
            dict_value[i].first = flexible_type_from_pyobject(key)
            dict_value[i].second = flexible_type_from_pyobject(value)
        ret.set_dict(dict_value)
    elif force is image.Image and py_check_flex_image(t):
        image_value = make_flex_image(v)
        ret.set_img(image_value)
    elif force is type(None) and v is None:
        ret = FLEX_UNDEFINED
    else:
        raise TypeError('Cannot convert python object %s of type %s to flexible_type' % (str(v), type(v)))
    return ret

cdef pyobject_from_flexible_type(flexible_type& v):
    """
    Converting flexible_type into python object
    """
    cdef type t = pytype_from_flex_type_enum(<flex_type_enum>v.get_type())
    cdef vector[double] array_value
    cdef vector[flexible_type] recursive_value
    cdef vector[pair[flexible_type, flexible_type]] dict_value
    cdef pair[flexible_type, flexible_type] key_value_pair
    if py_check_flex_int(t):
        return v.get_int()
    elif py_check_flex_date_time(t):
        dt = v.get_date_time()
        us = v.get_microsecond()
        utc = datetime.datetime(1970,1,1) + datetime.timedelta(seconds=dt.first, microseconds=us)
        if dt.second != EMPTY_TIMEZONE:
            to_zone = timezone.GMT(dt.second * TIMEZONE_RESOLUTION_IN_HOURS)
            utc = utc.replace(tzinfo=timezone.GMT(0))
            return utc.astimezone(to_zone)
        else:
            return utc
    elif py_check_flex_float(t):
        return v.get_double()
    elif py_check_flex_string(t):
        return v.get_string()
    elif py_check_flex_vec(t):
        x = array.array('d', v.get_vec())
        return x
    elif py_check_flex_list(t):
        recursive_value = v.get_recursive()
        return [pyobject_from_flexible_type(f) for f in recursive_value]
    elif py_check_flex_dict(t):
        ret = {}
        dict_value = v.get_dict()
        for i in range(dict_value.size()):
            key_value_pair = dict_value[i]
            key = pyobject_from_flexible_type(key_value_pair.first)
            value = pyobject_from_flexible_type(key_value_pair.second)
            ret[key]  = value
        return ret
    elif py_check_flex_image(t):
        return make_python_image(v);
    elif t is type(None):
        return None
    else:
        raise TypeError('Cannot convert python type %s to flexible_type' % str(t))

cdef gl_vec glvec_from_iterable(object values, type t=None, bint ignore_cast_failure=False) except *:
    """ Converting python iterable into vector[flexible_type] of specified type t"""
    assert hasattr(values, '__iter__'), "Cannot convert to vector[flexible_type] from non-iterable type: %s" % str(type(values))
    cdef gl_vec ret
    cdef int cnt = 0
    if t is None:
        """ return vector has no type constraint """
        ret.resize(len(values))
        """Fast path for int[:], double[:] compatible objects"""
        if is_int_memory_view(values) or is_long_memory_view(values):
            for v in values:
                ret[cnt].set_int(<long long>(v))
                cnt = cnt + 1
        elif is_float_memory_view(values) or is_double_memory_view(values):
            for v in values:
                ret[cnt].set_double(<double>(v))
                cnt = cnt + 1
        else:
            for v in values:
                ret[cnt] = flexible_type_from_pyobject(v)
                cnt = cnt + 1
    else:
        """ return vector must be type t """
        if py_check_flex_int(t):
            fill_int_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_date_time(t):
            fill_date_time_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_float(t):
            fill_double_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_string(t):
            fill_string_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_vec(t):
            fill_array_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_list(t):
            fill_recursive_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_dict(t):
            fill_dict_glvec(values, ret, ignore_cast_failure)
        elif py_check_flex_image(t):
            fill_image_glvec(values, ret, ignore_cast_failure)
        else:
          raise TypeError('Cannot convert to vector[flexible_type] of type %s' % str(t))
    return ret


cdef void fill_image_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                out[cnt] = FLEX_UNDEFINED
            else:
                out[cnt].set_img( make_flex_image((v)))
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_image(type(v)):
                f.set_img( make_flex_image((v)))
                out.push_back(f)


cdef void fill_date_time_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                out[cnt] = FLEX_UNDEFINED
            else:
                if(v.year < 1400 or v.year > 10000):
                    raise TypeError('Year is out of valid range: 1400..10000')
                if(v.tzinfo != None):
                    offset = int(v.tzinfo.utcoffset(v).total_seconds() / TIMEZONE_RESOLUTION_IN_SECONDS) # store timezone offset at the granularity of half an hour 
                    out[cnt].set_date_time((<long long>(calendar.timegm(v.utctimetuple())),offset), v.microsecond)
                else:
                    out[cnt].set_date_time((<long long>(calendar.timegm(v.utctimetuple())),EMPTY_TIMEZONE), v.microsecond)
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_date_time(type(v)):
                if(v.year < 1400 or v.year > 10000):
                    raise TypeError('Year is out of valid range: 1400..10000')
                if(v.tzinfo != None):
                    offset = int(v.tzinfo.utcoffset(v).total_seconds() / TIMEZONE_RESOLUTION_IN_SECONDS) # store timezone offset at the granularity of half an hour 
                    f.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),offset), v.microsecond)
                else:
                    f.set_date_time((<long long>(calendar.timegm(v.utctimetuple())),EMPTY_TIMEZONE), v.microsecond)
                out.push_back(f)

cdef void fill_int_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                out[cnt] = FLEX_UNDEFINED
            else:
                out[cnt].set_int(<long long>(v))
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_int(type(v)):
                f.set_int(<long long>(v))
                out.push_back(f)

cdef void fill_double_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                out[cnt] = FLEX_UNDEFINED
            else:
                out[cnt].set_double(<double>(v))
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_float(type(v)):
                f.set_double(<double>(v))
                out.push_back(f)

cdef void fill_string_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                out[cnt] = FLEX_UNDEFINED
            else:
                if type(v) is unicode:
                    out[cnt].set_string(v.encode('utf-8'))
                else:
                    out[cnt].set_string(str(v))

            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_string(type(v)):
                if type(v) is unicode:
                    f.set_string(v.encode('utf-8'))
                else:
                    f.set_string(str(v))
                out.push_back(f)

cdef void fill_array_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    cdef flex_vec empty
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                f = FLEX_UNDEFINED
            elif v == []:
            # empty list will be converted to recursive type
            # make a special exception if we want to fill it in an flex_vec sarray
                f.set_vec(empty)
            else:
                f = flexible_type_from_pyobject(v)
                assert <char>f.get_type() == <char>VECTOR, 'Cannot convert object %s to array.array' % str(v)
            out[cnt] = f
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif v == []:
            # empty list will be converted to recursive type
            # make a special exception if we want to fill it in an flex_vec sarray
                f.set_vec(empty)
                out.push_back(f)
            elif py_check_flex_vec(type(v), v):
                f = flexible_type_from_pyobject(v)
                if <char>f.get_type() == <char>VECTOR:
                    out.push_back(f)

cdef flex_list flex_vec_to_flex_list(flex_vec& vec):
    cdef flex_list ret
    ret.resize(vec.size())
    for i in range(len(vec)):
        ret[i].set_double(vec[i])
    return ret

cdef void fill_recursive_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                f = FLEX_UNDEFINED
            else:
                f = flexible_type_from_pyobject_with_type_force(v, list)
            out[cnt] = f
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_list(type(v)):
                f = flexible_type_from_pyobject_with_type_force(v, list)
                out.push_back(f)

cdef void fill_dict_glvec(object values, gl_vec& out, bint ignore_cast_failure) except *:
    cdef int cnt = 0
    cdef flexible_type f
    out.clear()
    if not ignore_cast_failure:
        out.resize(len(values))
        for v in values:
            if v is None:
                f = FLEX_UNDEFINED
            else:
                f = flexible_type_from_pyobject(v)
                assert <char>f.get_type() == <char>DICT
            out[cnt] = f
            cnt = cnt + 1
    else:
        for v in values:
            if v is None:
                out.push_back(FLEX_UNDEFINED)
            elif py_check_flex_dict(type(v)):
                f = flexible_type_from_pyobject(v)
                if <char>f.get_type() == <char>DICT:
                    out.push_back(f)

cdef gl_options_map gl_options_map_from_pydict(dict d) except *:
    """
    Converting python dict into map[string, flexible_type]
    """
    cdef gl_options_map ret
    cdef pair[string, flexible_type] entry
    for k,v in d.iteritems():
        ret[str(k)] = flexible_type_from_pyobject(v)
    return ret

cdef pylist_from_glvec(vector[flexible_type]& vec):
    """
    Converting vector[flexible_type] to list
    """
    ret = []
    for i in range(vec.size()):
        ret.append(pyobject_from_flexible_type(vec[i]))
    return ret

cdef pydict_from_gl_options_map(gl_options_map& d):
    """
    Converting map[string, flexible_type] to dict
    """
    ret = {}
    it = d.begin()
    cdef pair[string, flexible_type] entry
    while (it != d.end()):
        entry = deref(it)
        ret[entry.first] = pyobject_from_flexible_type(entry.second)
        inc(it)
    return ret

#/**************************************************************************/
#/*                                                                        */
#/*                        Internal Util Functions                         */
#/*                                                                        */
#/**************************************************************************/
cdef flex_vec make_flex_vec(object v) except *:
    """ Convert a python value v to vector[float] """
    cdef flex_vec ret
    if type(v) is list:
        "list of double can be casted to vector[double] in cython"
        ret = v
    elif type(v) is array.array or is_numeric_memory_view(v):
        ret.resize(len(v))
        for i in range(0, len(v)):
            ret[i] = v[i]
    else:
        raise TypeError('Cannot convert object %s to vector[double]' % str(v))
    return ret

cdef flex_image make_flex_image(object v) except *:
    """ Convert a python value v to flex image """
    cdef flex_image ret
    from ..data_structures import image
    if type(v) is image.Image:
        ret = flex_image(v._image_data, v._height, v._width, v._channels, v._image_data_size,<char> v._version, v._format_enum)
    else:
        raise TypeError('Cannot convert object %s to flex_image ' % str(v))
    return ret

cdef make_python_image(flexible_type& v) :
    cdef type t = pytype_from_flex_type_enum(<flex_type_enum>v.get_type())
    cdef flex_image c_image = v.get_img()
    cdef const char* c_image_data = <const char*>c_image.get_image_data()
    from ..data_structures import image
    if py_check_flex_image(t):
        if c_image.m_image_data_size == 0:
          image_data =  <bytearray> ()
        else:
          assert c_image_data != NULL, "image_data is Null"
          image_data =  <bytearray> c_image_data[:c_image.m_image_data_size]
        ret = image.Image( _image_data = image_data, _height = c_image.m_height, _width = c_image.m_width, _channels = c_image.m_channels,
                           _image_data_size = c_image.m_image_data_size, _version = <int>c_image.m_version, _format_enum = <int>c_image.m_format)
    else:
        raise TypeError("Cannot convert flexible_type to py_image")
    return ret


### Test utils ###
def _flexible_type(object v):
    """ convert to flexible_type and back """
    return pyobject_from_flexible_type(flexible_type_from_pyobject(v))

def _gl_vec(object v, type t=None, bint ignore_cast_failure=False):
    """ convert to vector[flexible_type] and back """
    return pylist_from_glvec(glvec_from_iterable(v, t, ignore_cast_failure))
