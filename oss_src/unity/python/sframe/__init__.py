"""
@package graphlab
...
GraphLab Create is a machine learning platform that enables data scientists and
app developers to easily create intelligent applications at scale. Building an
intelligent, predictive application involves iterating over multiple steps:
cleaning the data, developing features, training a model, and creating and
maintaining a predictive service. GraphLab Create does all of this in one
platform. It is easy to use, fast, and powerful.

For more details on the GraphLab Create see http://dato.com, including
documentation, tutorial, etc.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

import util
from .util import set_runtime_config
from .util import get_runtime_config

from .connect import _get_metric_tracker
import visualization

from .data_structures.sgraph import Vertex, Edge
from .data_structures.sgraph import SGraph
from .data_structures.sframe import SFrame
from .data_structures.sarray import SArray
from .data_structures.sketch import Sketch
from .data_structures.image import Image

from .data_structures.sgraph import load_sgraph, load_graph

from .toolkits._model import Model, CustomModel, load_model

from . import aggregate
from . import toolkits

from .toolkits.image_analysis import image_analysis

from .data_structures.sframe import load_sframe
from .data_structures.DBConnection import connect_odbc, get_libodbc_path, set_libodbc_path

# internal util
from .connect.main import launch as _launch
from .connect.main import stop as _stop
from .connect import main as glconnect

# python egg version
__VERSION__ = '{{VERSION_STRING}}'
version = '{{VERSION_STRING}}'

from .util import get_environment_config
from .util import get_graphlab_object_type
from .util import get_log_location, get_client_log_location, get_server_log_location

from .version_info import version
from .version_info import __VERSION__


class DeprecationHelper(object):
    def __init__(self, new_target):
        self.new_target = new_target

    def _warn(self):
        import warnings
        import logging
        warnings.warn("Graph has been renamed to SGraph. The Graph class will be removed in the next release.", PendingDeprecationWarning)
        logging.warning("Graph has been renamed to SGraph. The Graph class will be removed in the next release.")

    def __call__(self, *args, **kwargs):
        self._warn()
        return self.new_target(*args, **kwargs)

    def __getattr__(self, attr):
        self._warn()
        return getattr(self.new_target, attr)

Graph = DeprecationHelper(SGraph)

################### Extension Importing ########################
from . import extensions
from .extensions import ext_import

extensions._add_meta_path()

# rewrite the extensions module
class _extensions_wrapper(object):
  def __init__(self, wrapped):
    self._wrapped = wrapped
    self.__doc__ = wrapped.__doc__

  def __getattr__(self, name):
    try:
        return getattr(self._wrapped, name)
    except:
        pass
    from .connect.main import get_unity
    get_unity()
    return getattr(self._wrapped, name)

import sys as _sys

_sys.modules[__name__ + ".extensions"] = _extensions_wrapper(_sys.modules[__name__ + ".extensions"])
# rewrite the import
extensions = _sys.modules[__name__ + ".extensions"]
