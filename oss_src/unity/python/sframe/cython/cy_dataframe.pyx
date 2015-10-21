'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .cy_flexible_type cimport flexible_type
from .cy_flexible_type cimport flex_type_enum 
from .cy_flexible_type cimport glvec_from_iterable
from .cy_flexible_type cimport pylist_from_glvec 
from .cy_type_utils cimport pytype_from_dtype
from .cy_type_utils cimport pytype_to_flex_type_enum, pytype_from_flex_type_enum
from .cy_type_utils import infer_type_of_list
from math import isnan

HAS_PANDAS = False
try:
    import pandas as pd
    HAS_PANDAS = True 
except:
    HAS_PANDAS = False


cdef bint is_pandas_dataframe(object v):
    if HAS_PANDAS:
        return isinstance(v, pd.core.frame.DataFrame)
    else:
        return False

cdef gl_dataframe gl_dataframe_from_pd(object df) except *:
    cdef gl_dataframe ret
    assert is_pandas_dataframe(df), 'Cannot convert object of type %s to gl_dataframe' % str(type(df))
    if len(set(df.columns.values)) != len(df.columns.values):
        raise ValueError('Input pandas dataframe column names must be unique')
    for colname in df.columns.values:
        _type = pytype_from_dtype(df[colname].dtype) # convert from dtype to python type
        # convert all NaNs to None
        colcopy = [None if isinstance(x, float) and isnan(x) else x for x in df[colname].values]
        if _type is object:
            _type = infer_type_of_list(colcopy)
        ret.names.push_back(str(colname))
        ret.types[str(colname)] = pytype_to_flex_type_enum(_type)
        ret.values[str(colname)] = glvec_from_iterable(colcopy, _type)
    return ret


cdef gl_dataframe gl_dataframe_from_dict_of_arrays(object df) except *:
    cdef gl_dataframe ret
    keys = sorted(df.keys())
    for key in keys:
        value = df[key]
        _type = infer_type_of_list(value)
        colcopy = [None if isinstance(x, float) and isnan(x) else x for x in value]
        ret.names.push_back(key)
        ret.types[str(key)] = pytype_to_flex_type_enum(_type)
        ret.values[str(key)] = glvec_from_iterable(colcopy, _type)
    return ret

cdef pd_from_gl_dataframe(gl_dataframe& df):
    assert HAS_PANDAS, 'Cannot find pandas library'
    ret = pd.DataFrame()
    for _name in df.names:
        _type = pytype_from_flex_type_enum(df.types[_name])
        ret[_name] = pylist_from_glvec(df.values[_name])
        if len(ret[_name]) == 0:
            """ special handling of empty list, we need to force the type information """
            ret[_name] = ret[_name].astype(_type)
    return ret 


### Test Util: convert dataframe to gl_dataframe and back ###
def _dataframe(object df):
    return pd_from_gl_dataframe(gl_dataframe_from_pd(df))
