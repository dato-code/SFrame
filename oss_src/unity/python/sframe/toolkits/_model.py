"""@package graphlab.toolkits

Defines a basic interface for a model object.
"""
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .. import connect as _mt
from ..connect import main as glconnect
from ..data_structures.sframe import SFrame as _SFrame
from ..toolkits._internal_utils import _map_unity_proxy_to_object, \
                                        _toolkit_serialize_summary_struct, \
                                        _toolkit_summary_to_json
from ..util import _make_internal_url
from .. import _gl_pickle

from pickle import PicklingError
import shutil as _shutil
import os as _os
import tempfile
import json


def load_model(location):
    """
    Load any GraphLab Create model that was previously saved.

    This function assumes the model (can be any model) was previously saved in
    GraphLab Create model format with model.save(filename).

    Parameters
    ----------
    location : string
        Location of the model to load. Can be a local path or a remote URL.
        Because models are saved as directories, there is no file extension.

    Examples
    ----------
    >>> model.save('my_model_file')
    >>> loaded_model = gl.load_model('my_model_file')
    """
    _mt._get_metric_tracker().track('toolkit.model.load_model')

    _internal_url = _make_internal_url(location)
    try:
        return glconnect.get_unity().load_model(_internal_url)
    except IOError as e:
        unpickler = _gl_pickle.GLUnpickler(_internal_url)

        # Get the version
        version = unpickler.load()

        # Load the class name.
        cls_name = unpickler.load()
        cls = _get_class_from_name(cls_name)

        # Load the object with the right version.
        model = cls._load_version(unpickler, version)

        unpickler.close()

        # Return the model
        return model



def _get_default_options_wrapper(unity_server_model_name,
                                module_name='',
                                python_class_name='',
                                sdk_model = False):
    """
    Internal function to return a get_default_options function.

    Parameters
    ----------
    unity_server_model_name: str
        Name of the class/toolkit as registered with the unity server

    module_name: str, optional
        Name of the module.

    python_class_name: str, optional
        Name of the Python class.

    sdk_model : bool, optional (default False)
        True if the SDK interface was used for the model. False otherwise.

    Examples
    ----------
    get_default_options = _get_default_options_wrapper('classifier_svm',
                                                       'svm', 'SVMClassifier')
    """
    def get_default_options_for_model(output_type = 'sframe'):
        """
        Get the default options for the toolkit
        :class:`~graphlab.{module_name}.{python_class_name}`.

        Parameters
        ----------
        output_type : str, optional

            The output can be of the following types.

            - `sframe`: A table description each option used in the model.
            - `json`: A list of option dictionaries.

            | Each dictionary/row in the JSON/SFrame object describes the
              following parameters of the given model.

            +------------------+-------------------------------------------------------+
            |      Name        |                  Description                          |
            +==================+=======================================================+
            | name             | Name of the option used in the model.                 |
            +------------------+---------+---------------------------------------------+
            | description      | A detailed description of the option used.            |
            +------------------+-------------------------------------------------------+
            | type             | Option type (REAL, BOOL, INTEGER or CATEGORICAL)      |
            +------------------+-------------------------------------------------------+
            | default_value    | The default value for the option.                     |
            +------------------+-------------------------------------------------------+
            | possible_values  | List of acceptable values (CATEGORICAL only)          |
            +------------------+-------------------------------------------------------+
            | lower_bound      | Smallest acceptable value for this option (REAL only) |
            +------------------+-------------------------------------------------------+
            | upper_bound      | Largest acceptable value for this option (REAL only)  |
            +------------------+-------------------------------------------------------+

        Returns
        -------
        out : JSON/SFrame

        See Also
        --------
        graphlab.{module_name}.{python_class_name}.get_current_options

        Examples
        --------
        .. sourcecode:: python

          >>> import graphlab

          # SFrame formatted output.
          >>> out_sframe = graphlab.{module_name}.get_default_options()

          # JSON formatted output.
          >>> out_sframe = graphlab.{module_name}.get_default_options('json')
        """
        _mt._get_metric_tracker().track('toolkit.%s.get_default_options' % module_name)
        from .. import extensions as _extensions
        if sdk_model:
            response = _extensions._toolkits_sdk_get_default_options(
                                                          unity_server_model_name)
        else:
            response = _extensions._toolkits_get_default_options(
                                                          unity_server_model_name)

        for k in response.keys():
            response[k] = json.loads(response[k],
               parse_int = lambda x: float(x) if type(int(x)) is long else int(x))

        if output_type == 'json':
          return response
        else:
          json_list = [{'name': k, '': v} for k,v in response.items()]
          return _SFrame(json_list).unpack('X1', column_name_prefix='') \
                                   .unpack('X1', column_name_prefix='')

    # Change the doc string before returning.
    get_default_options_for_model.__doc__ = get_default_options_for_model.\
            __doc__.format(python_class_name = python_class_name,
                  module_name = module_name)
    return get_default_options_for_model


def _get_class_full_name(obj):
    """
    Returns a full class name from the module name and the class name.

    Parameters
    ----------
    cls : type | object

    Returns
    -------
    A full name with all the imports.
    """
    cls_name = obj.__name__ if type(obj) == type else obj.__class__.__name__
    return "%s.%s" % (obj.__module__ , cls_name)


def _get_class_from_name(class_name, *arg, **kwarg):
    """
    Create a class instance given the name of the class. This will ensure all
    the required modules are imported.

    For example. The class graphlab.my_model will first import graphlab
    and then load my_model.

    """
    module_path = class_name.split('.')
    import_path = module_path[0:-1]
    module = __import__('.'.join(import_path), fromlist=[module_path[-1]])
    class_ = getattr(module, module_path[-1])
    return class_

class CustomModel(object):
    """
    This class defines the minimal interface of a model that can be interfaced
    with GraphLab objects. This class contains serialization routines that make
    it compatible with GraphLab objects.

    Examples
    ----------
    # Define the model
    class MyModel(CustomModel):
        def __init__(self, sf, classifier):
            self.sframe = sf
            self.classifier = classifier

        def my_func(self):
            return self.classifier.predict(self.sframe)

    # Construct the model
    >>> custom_model = MyModel(sf, glc_model)

    ## The model can be saved and loaded like any GraphLab Create model.
    >>> model.save('my_model_file')
    >>> loaded_model = gl.load_model('my_model_file')
    """

    _PYTHON_MODEL_VERSION = 0

    def __init__(self, proxy = None):
        """
        Construct a dummy object from its proxy object and class name. The
        proxy contains a location to the pickle-file where the real object
        is stored.

        Parameters
        ----------
        proxy  : object

        Returns
        -------
        Returns an object of type _class with a path to the pickle archive
        saved in proxy.temp_file.

        """
        self.__proxy__ = None

    @classmethod
    def _is_gl_pickle_safe(cls):
        """
        Return True if the model is GLPickle safe i.e if the model does not
        contain elements that are written using Python + GraphLab objects.
        """
        return True

    def save(self, location):
        """
        Save the model. The model is saved as a directory which can then be
        loaded using the :py:func:`~graphlab.load_model` method.

        Parameters
        ----------
        location : string
            Target destination for the model. Can be a local path or remote URL.

        See Also
        ----------
        graphlab.load_model

        Examples
        ----------
        >>> model.save('my_model_file')
        >>> loaded_model = gl.load_model('my_model_file')

        """
        # Save to a temoporary pickle file.
        try:
            self._save_to_pickle(_make_internal_url(location))
        except IOError as err:
            raise IOError("Unable to save model. Trace (%s)" % err)

    def _save_to_pickle(self, filename):
        """
        Save the object to a pickle file.

        Parameters
        ----------
        filename : Filename to save.

        Notes
        -----
        The file is saved to a GLPickle archive. The following three attributes
        are saved:

        - The version of the object (obtained from get_version())
        - Class name of the object.
        - The object is pickled as directed in _save_impl

        """
        if not self._is_gl_pickle_safe():
            raise RuntimeError("Cannot pickle object. Use the 'save' method.")

        # Setup the pickler.
        pickler = _gl_pickle.GLPickler(filename)

        # Save version and class-name.
        pickler.dump(self._get_version())
        pickler.dump(_get_class_full_name(self))

        # Save the object.
        self._save_impl(pickler)
        pickler.close()

    def _save_impl(self, pickler):
        """
        An function to implement save for the object in consideration.
        The default implementation will dump self to the pickler.

        WARNING: This implementation is very simple.
                 Overwrite for smarter implementations.

        Parameters
        ----------
        pickler : An opened GLPickle archive (Do not close the archive.)
        """
        if not self._is_gl_pickle_safe():
            raise RuntimeError("Cannot pickle object. Use the 'save' method.")

        # Pickle will hate the proxy
        self.__proxy__ = None
        pickler.dump(self)

    def summary(self, output=None):
        """
        Print a summary of the model.
        The summary includes a description of training
        data, options, hyper-parameters, and statistics measured during model
        creation.

        Examples
        --------
        >>> m.summary()

        Parameters
        ----------
        output : string, None
            The type of summary to return.
            None or 'stdout' : prints directly to stdout
            'str' : string of summary
            'dict' : a dict with 'sections' and 'section_titles' ordered lists
                        The entries in the 'sections' list are tuples of the form ('label', 'value')
        """
        if output is None or output == 'stdout':
            pass
        elif (output == 'str'):
            return self.__repr__()
        elif output == 'dict':
            return _toolkit_serialize_summary_struct( self, \
                                            *self._get_summary_struct() )
        _mt._get_metric_tracker().track(self.__class__.__module__ + '.summary')
        try:
            print(self.__repr__())
        except:
            return self.__class__.__name__

    def _get_summary_struct(self):
        raise NotImplementedError

    def _get_wrapper(self):
        """
        Return a function: UnityModel -> M, for constructing model
        class M from a UnityModel proxy.

        Only used in GLC-1.3.

        """
        _class = self.__class__
        proxy_wrapper = self.__proxy__._get_wrapper()

        # Define the function
        def model_wrapper(unity_proxy):

            # Load the proxy object. This returns a proxy object with
            # 'temp_file' set to where the object is pickled.
            model_proxy = proxy_wrapper(unity_proxy)
            temp_file = model_proxy.temp_file

            # Setup the unpickler.
            unpickler = _gl_pickle.GLUnpickler(temp_file)

            # Get the version
            version = unpickler.load()

            # Load the class name.
            cls_name = unpickler.load()
            cls = _get_class_from_name(cls_name)

            # Load the object with the right version.
            obj = cls._load_version(unpickler, version)

            unpickler.close()

            # Return the object
            return obj

        return model_wrapper

    def _get_version(self):
        return self._PYTHON_MODEL_VERSION

    def show(self, view=None, model_type='base'):
        """
        show(view=None)
        Visualize with GraphLab Canvas :mod:`~graphlab.canvas`.
        This function starts Canvas if it is not already running.
        If the Model has already been plotted, this function will update the plot.

        Parameters
        ----------
        view : str, optional
            The name of the Model view to show. Can be one of:

            - Summary: Shows the statistics of the training process such as size of the data and time cost. The summary also shows the parameters and settings for the model training process if available.
            - Evaluation: Shows precision recall plot as line chart. Tooltip is provided for pointwise analysis. Precision recall values are shown in the tooltip at any given cutoff value the mouse points to.


        Returns
        -------
        view : graphlab.canvas.view.View
            An object representing the GraphLab Canvas view

        See Also
        --------
        canvas

        Examples
        --------
        Suppose 'm' is a Model, we can view it in GraphLab Canvas using:

        >>> m.show()
        """
        from ..visualization.show import show
        show(self, view=view, model_type=model_type)

    @classmethod
    def _load_version(cls, unpickler, version):
        """
        An function to load an object with a specific version of the class.

        WARNING: This implementation is very simple.
                 Overwrite for smarter implementations.

        Parameters
        ----------
        unpickler : GLUnpickler
            A GLUnpickler file handle.

        version : int
            A version number as maintained by the class writer.
        """
        # Warning: This implementation is by default not version safe.
        # For version safe implementations, please write the logic
        # that is suitable for your model.
        return unpickler.load()


class Model(CustomModel):
    """
    This class defines the minimal interface of a model object
    storing the results from a toolkit function.

    User can query the model object via the `get` function, and
    the list of the queriable fields can be obtained via `list_fields`.

    Model object also supports dictionary type access, e.g. m['pagerank']
    is equivalent to m.get('pagerank').
    """

    """The UnityModel Proxy Object"""
    __proxy__ = None

    def name(self):
        """
        Returns the name of the model.

        Returns
        -------
        out : str
            The name of the model object.

        Examples
        --------
        >>> model_name = m.name()
        """
        return self.__class__.__name__

    def list_fields(self):
        """
        List of fields stored in the model. Each of these fields can be queried
        using the ``get(field)`` function or ``m[field]``.

        Returns
        -------
        out : list[str]
            A list of fields that can be queried using the ``get`` method.

        Examples
        --------
        >>> fields = m.list_fields()
        """
        _mt._get_metric_tracker().track(self.__class__.__module__ + '.list_fields')
        return self.__proxy__.list_fields()

    def get(self, field):
        """Return the value for the queried field.

        Each of these fields can be queried in one of two ways:

        >>> out = m['field']
        >>> out = m.get('field')  # equivalent to previous line

        Parameters
        ----------
        field : string
            Name of the field to be retrieved.

        Returns
        -------
        out : value
            The current value of the requested field.

        """
        if field in self.list_fields():
            return self.__proxy__.get(field)
        else:
            raise KeyError('Field \"%s\" not in model. Available fields are'
                         '%s.') % (field, ', '.join(self.list_fields()))

    def __getitem__(self, key):
        return self.get(key)

    def _get_wrapper(self):
        """Return a lambda function: UnityModel -> M, for constructing model
        class M from a UnityModel proxy."""
        raise NotImplementedError

    def save(self, location):
        """
        Save the model. The model is saved as a directory which can then be
        loaded using the :py:func:`~graphlab.load_model` method.

        Parameters
        ----------
        location : string
            Target destination for the model. Can be a local path or remote URL.

        See Also
        ----------
        graphlab.load_model

        Examples
        ----------
        >>> model.save('my_model_file')
        >>> loaded_model = graphlab.load_model('my_model_file')

        """
        _mt._get_metric_tracker().track('toolkit.model.save')
        return glconnect.get_unity().save_model(self, _make_internal_url(location))

    def __repr__(self):
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()

    @classmethod
    def _is_gl_pickle_safe(cls):
        """
        Return True if the model is GLPickle safe i.e if the model does not
        contain elements that are written using Python + GraphLab objects.
        """
        return False

    def __gl_pickle_save__(self, filename):
        self.save(filename)

    @staticmethod
    def __gl_pickle_load__(filename):
        return load_model(filename)
