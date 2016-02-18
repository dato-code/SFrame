import boto
import moto
import unittest
import os
import uuid
import shutil
import sys
from nose.tools import nottest

import pickle
from ..util import cloudpickle

from ..data_structures.sframe import SFrame
from ..data_structures.sarray import SArray
from ..data_structures.sgraph import SGraph, Vertex, Edge

from .. import _gl_pickle as gl_pickle
from ..util import _assert_sframe_equal as assert_sframe_equal

class GLPicklingTest(unittest.TestCase):

    def setUp(self):
        self.filename = str(uuid.uuid4())

    def tearDown(self):
        if os.path.isdir(self.filename):
            shutil.rmtree(self.filename)
        elif os.path.exists(self.filename):
            os.remove(self.filename)

    def _dump(self, obj):
        pickler = gl_pickle.GLPickler(self.filename)
        pickler.dump(obj)
        pickler.close()

    def _load(self):
        unpickler = gl_pickle.GLUnpickler(self.filename)
        obj_ret = unpickler.load()
        unpickler.close()
        return obj_ret

    def test_pickling_simple_types(self):

        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            self._dump(obj)
            obj_ret = self._load()
            self.assertEqual(obj, obj_ret)

    @unittest.skipIf(sys.platform == 'win32' and sys.version_info.major == 3,
                     'See: https://github.com/dato-code/SFrame/issues/176')
    def test_pickling_sarray_types(self):

        sarray_list = [
            SArray([1,2,3]),
            SArray([1.0,2.0,3.5]),
            SArray(["foo", "bar"]),
        ]
        for obj in sarray_list:
            pickler = self._dump(obj)
            obj_ret = self._load()
            self.assertEqual(list(obj), list(obj_ret))

    def test_pickling_sframe_types(self):

        sarray_list = [
            SFrame([1,2,3]),
            SFrame([1.0,2.0,3.5]),
            SFrame(["foo", "bar"]),
        ]
        for obj in sarray_list:
            self._dump(obj)
            obj_ret = self._load()
            assert_sframe_equal(obj, obj_ret)

    def test_pickling_sgraph_types(self):

        sg_test_1 = SGraph().add_vertices([
                                Vertex(0, {'fluffy': 1}),
                                Vertex(1, {'fluffy': 1, 'woof': 1}),
                                Vertex(2, {})])

        sg_test_2 = SGraph()
        sg_test_2 = sg_test_2.add_vertices([
                            Vertex(x) for x in [0, 1, 2]])
        sg_test_2 = sg_test_2.add_edges([
                        Edge(0, 1, attr={'relationship': 'dislikes'}),
                        Edge(1, 2, attr={'relationship': 'likes'}),
                        Edge(1, 0, attr={'relationship': 'likes'})])

        sarray_list = [
            sg_test_1,
            sg_test_2
        ]
        for obj in sarray_list:
            self._dump(obj)
            obj_ret = self._load()
            assert_sframe_equal(obj.get_vertices(), obj_ret.get_vertices())
            assert_sframe_equal(obj.get_edges(), obj_ret.get_edges())

    @unittest.skipIf(sys.platform == 'win32' and sys.version_info.major == 3,
                     'See: https://github.com/dato-code/SFrame/issues/176')
    def test_combination_gl_python_types(self):

        sg_test_1 = SGraph().add_vertices([
                                Vertex(1, {'fluffy': 1}),
                                Vertex(2, {'fluffy': 1, 'woof': 1}),
                                Vertex(3, {})])
        sarray_test_1 = SArray([1,2,3])
        sframe_test_1 = SFrame([1,2,3])

        obj_list = [
            [sg_test_1, sframe_test_1, sarray_test_1],
            {0:sg_test_1, 1:sframe_test_1, 2:sarray_test_1}
        ]

        for obj in obj_list:
            self._dump(obj)
            obj_ret = self._load()
            assert_sframe_equal(obj[0].get_vertices(), obj_ret[0].get_vertices())
            assert_sframe_equal(obj[0].get_edges(), obj_ret[0].get_edges())
            assert_sframe_equal(obj[1], obj_ret[1])
            self.assertEqual(list(obj[2]), list(obj_ret[2]))

    def test_pickle_compatibility(self):
        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            self._dump(obj)
            obj_ret = self._load()
            self.assertTrue(obj, obj_ret)

    def test_cloud_pickle_compatibility(self):
        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            self._dump(obj)
            obj_ret = self._load()
            self.assertTrue(obj, obj_ret)

    def test_relative_path(self):
        # Arrange
        sf1 = SFrame(range(10))
        relative_path = 'tmp/%s' % self.filename

        # Act
        pickler = gl_pickle.GLPickler(relative_path)
        pickler.dump(sf1)
        pickler.close()
        unpickler = gl_pickle.GLUnpickler(relative_path)
        sf2 = unpickler.load()
        unpickler.close()

        # Assert
        assert_sframe_equal(sf1, sf2)

        # Clean up
        shutil.rmtree(relative_path)

    def test_user_path(self):
        # Arrange
        sf1 = SFrame(range(10))
        relative_path = '~/tmp/%s' % self.filename


        # Act
        pickler = gl_pickle.GLPickler(relative_path)
        pickler.dump(sf1)
        pickler.close()
        unpickler = gl_pickle.GLUnpickler(relative_path)
        sf2 = unpickler.load()
        unpickler.close()

        # Assert
        abs_path = os.path.expanduser(relative_path)
        self.assertTrue(os.path.exists(abs_path))
        assert_sframe_equal(sf1, sf2)

        # Clean up
        shutil.rmtree(abs_path)

    def test_env_path(self):
        # Arrange
        sf1 = SFrame(range(10))
        os.environ["FOO"] = '~/bar'
        relative_path = '${FOO}/tmp/%s' % self.filename

        # Act
        pickler = gl_pickle.GLPickler(relative_path)
        pickler.dump(sf1)
        pickler.close()
        unpickler = gl_pickle.GLUnpickler(relative_path)
        sf2 = unpickler.load()
        unpickler.close()

        # Assert
        abs_path = os.path.expanduser(os.path.expandvars(relative_path))
        self.assertTrue(os.path.exists(abs_path))
        assert_sframe_equal(sf1, sf2)

        # Clean up
        shutil.rmtree(abs_path)
        del(os.environ["FOO"])

    @moto.mock_s3
    def test_save_to_s3(self):
        # Arrange
        os.environ['GRAPHLAB_UNIT_TEST'] = 'PredictiveService'
        os.environ['AWS_ACCESS_KEY_ID'] = ''
        os.environ['AWS_SECRET_ACCESS_KEY'] = ''
        BUCKET_NAME = 'foo'
        boto.connect_s3().create_bucket(BUCKET_NAME)
        sf1 = SFrame(range(10))
        S3_PATH = "s3://%s/foobar" % BUCKET_NAME

        # Act
        pickler = gl_pickle.GLPickler(S3_PATH)
        pickler.dump(sf1)
        pickler.close()
        unpickler = gl_pickle.GLUnpickler(S3_PATH)
        sf2 = unpickler.load()
        unpickler.close()

        # Assert
        assert_sframe_equal(sf1, sf2)

        del os.environ['GRAPHLAB_UNIT_TEST']

    @nottest
    def test_backward_compatibility(self):

        # Arrange
        file_name = 's3://gl-internal-datasets/models/1.3/gl-pickle.gl'
        obj = {'foo': SFrame([1,2,3]),
               'bar': SFrame(),
               'foo-bar': ['foo-and-bar', SFrame([1])]}

        # Act
        unpickler = gl_pickle.GLUnpickler(file_name)
        obj_ret = unpickler.load()

        # Assert
        assert_sframe_equal(obj['foo'], obj_ret['foo'])
        assert_sframe_equal(obj['bar'], obj_ret['bar'])
        assert_sframe_equal(obj['foo-bar'][1], obj_ret['foo-bar'][1])
        self.assertEqual(obj['foo-bar'][0], obj_ret['foo-bar'][0])

    @nottest
    def test_backward_compatibility_v1(self):

        # Arrange
        file_name = 's3://gl-internal-datasets/archives/gl-pickle-1.0.gl'
        obj = {'foo': SFrame([1,2,3]),
               'bar': SFrame(),
               'foo-bar': ['foo-and-bar', SFrame([1])]}

        # Act
        unpickler = gl_pickle.GLUnpickler(file_name)
        obj_ret = unpickler.load()

        # Assert
        assert_sframe_equal(obj['foo'], obj_ret['foo'])
        assert_sframe_equal(obj['bar'], obj_ret['bar'])
        assert_sframe_equal(obj['foo-bar'][1], obj_ret['foo-bar'][1])
        self.assertEqual(obj['foo-bar'][0], obj_ret['foo-bar'][0])


    def test_save_over_previous(self):

        sarray_list = [
            SFrame([1,2,3]),
            SFrame([1.0,2.0,3.5]),
            SFrame(["foo", "bar"]),
        ]
        for obj in sarray_list:
            self._dump(obj)
            obj_ret = self._load()

            assert_sframe_equal(obj, obj_ret)

            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()

    def test_extensibility(self):

        class SampleClass:
            def __init__(self, member):
               self.member = member

            def __gl_pickle_save__(self, filename):
                with open(filename, 'w') as f:
                    f.write(self.member)

            @staticmethod
            def __gl_pickle_save__(filename):
                with open(filename, 'r') as f:
                    member = f.read().split()
                return SampleClass(member)

            def __eq__(self, other):
                return self.member == other.member

        test_list = [
            1,
            SFrame([1,2,3]),
            SampleClass("Obj-1"),
            [SampleClass("Obj-1"), SampleClass("Obj-22")],
            {'one': SampleClass("Obj-1"), 'two': SampleClass("Obj-22")}
        ]
        for obj in test_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()

            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            if type(obj) == SFrame:
                assert_sframe_equal(obj, obj_ret)
            else:
                self.assertEqual(obj, obj_ret)

            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
