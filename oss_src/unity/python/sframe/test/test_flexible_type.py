'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import unittest
import numpy as np
import array
import datetime as dt
from ..data_structures import image
from .. import SArray
import os
from ..cython.cy_flexible_type import _flexible_type, _gl_vec
from ..cython.cy_type_utils import infer_type_of_list
from ..util.timezone import GMT

current_file_dir = os.path.dirname(os.path.realpath(__file__))


def from_lambda(v):
    from ..connect import main as glconnect
    u = glconnect.get_unity()
    return u.eval_lambda(lambda x: x, v)


class FlexibleTypeTest(unittest.TestCase):

    # On lambda return, if the return value is a non-empty of list of
    # all numerical values, we try hard to use array.array
    def numeric_list_to_array(self, v):
        if (type(v) is list) and (len(v) > 0) and all((type(x) is int) or (type(x) is float) for x in v):
            return array.array('d', v)
        elif (type(v) is list):
            return [self.numeric_list_to_array(x) for x in v]
        else:
            return v

    def assert_equal_with_lambda_check(self, expected, actual):
        self.assertEqual(expected, actual)
        self.assertEqual(from_lambda(expected), self.numeric_list_to_array(actual))

    def test_none(self):
        self.assert_equal_with_lambda_check(_flexible_type(None), None)
    def test_date_time(self):
        d = dt.datetime(2010, 10, 10, 10, 10, 10);
        self.assert_equal_with_lambda_check(_flexible_type(d),d)
    def test_int(self):
        self.assert_equal_with_lambda_check(_flexible_type(1), 1)
        self.assert_equal_with_lambda_check(_flexible_type(1L), 1)
        self.assert_equal_with_lambda_check(_flexible_type(True), 1)
        self.assert_equal_with_lambda_check(_flexible_type(False), 0)
        # numpy types
        self.assert_equal_with_lambda_check(_flexible_type(np.int_(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.int64(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.int32(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.int16(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.uint64(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.uint32(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.uint16(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool(0)), 0)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool_(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool_(0)), 0)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool8(1)), 1)
        self.assert_equal_with_lambda_check(_flexible_type(np.bool8(0)), 0)

    def test_float(self):
        self.assert_equal_with_lambda_check(_flexible_type(0.1), 0.1)
        # numpy types
        self.assert_equal_with_lambda_check(_flexible_type(np.float(0.1)), 0.1)
        self.assert_equal_with_lambda_check(_flexible_type(np.float_(0.1)), 0.1)
        self.assertAlmostEqual(_flexible_type(np.float16(0.1)), 0.1, delta=0.001)  # not exact
        self.assertAlmostEqual(_flexible_type(np.float32(0.1)), 0.1)  # not exact
        self.assertAlmostEqual(_flexible_type(np.float64(0.1)), 0.1)  # not exact

    def test_string(self):
        self.assert_equal_with_lambda_check(_flexible_type("a"), "a")
        self.assert_equal_with_lambda_check(_flexible_type(unicode("a")), "a")
        # numpy types
        self.assert_equal_with_lambda_check(_flexible_type(np.string_("a")), "a")
        self.assert_equal_with_lambda_check(_flexible_type(np.unicode_("a")), "a")

    def test_array(self):
        # float array
        expected = array.array('d', [.1, .2, .3])
        self.assert_equal_with_lambda_check(_flexible_type(expected), expected)

        # int array
        expected = array.array('d', [1, 2, 3])
        self.assert_equal_with_lambda_check(_flexible_type([1, 2, 3]), expected)
        self.assert_equal_with_lambda_check(_flexible_type([1, 2, 3.0]), expected)

        # numpy ndarray
        expected = np.asarray([1, 2, 3])
        self.assertSequenceEqual(_flexible_type(expected), list(expected))
        self.assertEquals(from_lambda(expected), array.array('d', expected))

        expected = np.asarray([.1, .2, .3])
        self.assertSequenceEqual(_flexible_type(expected), list(expected))
        self.assertEquals(from_lambda(expected), array.array('d', expected))

    def test_dict(self):
        d = dt.datetime(2010, 10, 10, 10, 10, 10)
        img = image.Image(current_file_dir + "/images/nested/sample_grey.jpg","JPG")
        expected = {'int': 0, 'float': 0.1, 'str': 'str',
                    'list': ['a', 'b', 'c'], 'array': array.array('d', [1, 2, 3]),'datetime':[d],
                     'image': img ,'none': None}
        self.assert_equal_with_lambda_check(_flexible_type(expected), expected)
        self.assert_equal_with_lambda_check(_flexible_type({}), {})

        expected = [{'a': 1, 'b': 20, 'c': None}, {"b": 4, None: 5}, None, {'a': 0}]
        self.assert_equal_with_lambda_check(_flexible_type(expected), expected)

    def test_recursive(self):
        d = dt.datetime(2010, 10, 10, 10, 10, 10)
        img = image.Image(current_file_dir + "/images/nested/sample_grey.jpg","JPG")
        expected = [None, img, 1, 0.1, '1',d,array.array('d', [1, 2, 3]), {'foo': array.array('d', [1, 2,3])}]
        self.assert_equal_with_lambda_check(_flexible_type(expected), expected)
        self.assert_equal_with_lambda_check(_flexible_type([]), [])
        self.assert_equal_with_lambda_check(_flexible_type([[], []]), [[], []])

    def test_image(self):
        img_gray_jpg = image.Image(current_file_dir + "/images/nested/sample_grey.jpg","JPG")
        img_gray_png = image.Image(current_file_dir + "/images/nested/sample_grey.png","PNG")
        img_gray_auto_jpg = image.Image(current_file_dir + "/images/nested/sample_grey.jpg")
        img_gray_auto_png = image.Image(current_file_dir + "/images/nested/sample_grey.png")
        img_color_jpg = image.Image(current_file_dir + "/images/sample.jpg","JPG")
        img_color_png = image.Image(current_file_dir + "/images/sample.png","PNG")
        img_color_auto_jpg = image.Image(current_file_dir + "/images/sample.jpg")
        img_color_auto_png = image.Image(current_file_dir + "/images/sample.png")


        self.assert_equal_with_lambda_check(_flexible_type(img_gray_jpg),img_gray_jpg)
        self.assert_equal_with_lambda_check(_flexible_type(img_gray_png),img_gray_png)
        self.assert_equal_with_lambda_check(_flexible_type(img_gray_auto_jpg),img_gray_auto_jpg)
        self.assert_equal_with_lambda_check(_flexible_type(img_gray_auto_png),img_gray_png)
        self.assert_equal_with_lambda_check(_flexible_type(img_color_jpg),img_color_jpg)
        self.assert_equal_with_lambda_check(_flexible_type(img_color_png),img_color_png)
        self.assert_equal_with_lambda_check(_flexible_type(img_color_auto_jpg),img_color_auto_jpg)
        self.assert_equal_with_lambda_check(_flexible_type(img_color_auto_png),img_color_auto_png)

    def test_gl_vec(self):
        expected = []
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)

        # test int list
        expected = [1, 2, 3, 4, 5, None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, int), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, int, ignore_cast_failure=True), expected)

        # test datetime list
        from_zone = GMT(0)
        to_zone = GMT(4.5)
        d1 = dt.datetime(2010, 10, 10, 10, 10, 10).replace(tzinfo=from_zone)
        d2 = d1.astimezone(to_zone)
        expected = [d1,d2, None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, dt.datetime), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, dt.datetime, ignore_cast_failure=True), expected)
        # test image list
        img_gray_auto_png = image.Image(current_file_dir + "/images/nested/sample_grey.png")
        img_color_jpg = image.Image(current_file_dir + "/images/sample.jpg","JPG")
        expected = [img_gray_auto_png, img_color_jpg, None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, image.Image), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, image.Image, ignore_cast_failure=True), expected)
        # test str list
        expected = ['a', 'b', 'c', None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, str), expected)

        # test array list
        expected = [array.array('d', range(5)), array.array('d', range(5)), None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), expected)
        self.assert_equal_with_lambda_check(_gl_vec(expected, array.array), expected)
        expected = [[float(i) for i in range(5)], range(5), None]
        self.assert_equal_with_lambda_check(_gl_vec(expected), [array.array('d', range(5)), array.array('d', range(5)), None])

        # test int array
        expected = array.array('d', range(5))
        self.assert_equal_with_lambda_check(_gl_vec(expected), range(5))

        expected = [1, 1.0, '1', [1., 1., 1.], ['a', 'b', 'c'], {}, {'a': 1}, None]
        self.assert_equal_with_lambda_check(_gl_vec(expected, int, ignore_cast_failure=True), [1, None])
        self.assert_equal_with_lambda_check(_gl_vec(expected, float, ignore_cast_failure=True), [1.0, None])
        self.assert_equal_with_lambda_check(_gl_vec(expected, str, ignore_cast_failure=True), ['1', None])
        self.assert_equal_with_lambda_check(_gl_vec(expected, array.array, ignore_cast_failure=True), [array.array('d', [1., 1., 1.]), None])
        self.assert_equal_with_lambda_check(_gl_vec(expected, list, ignore_cast_failure=True), [[1., 1., 1.], ['a', 'b', 'c'], None])
        self.assert_equal_with_lambda_check(_gl_vec(expected, dict, ignore_cast_failure=True), [{}, {'a': 1}, None])

    def test_infer_list_type(self):
        self.assertEquals(infer_type_of_list([image.Image(current_file_dir + "/images/nested/sample_grey.png"), image.Image(current_file_dir + "/images/sample.jpg","JPG"), image.Image(current_file_dir + "/images/sample.png")
]), image.Image)
        self.assertEquals(infer_type_of_list([dt.datetime(2010, 10, 10, 10, 10, 10), dt.datetime(2000, 5, 7, 10, 4, 10),dt.datetime(1845, 5, 7, 4, 4, 10)]), dt.datetime)
        self.assertEquals(infer_type_of_list([0, 1, 2]), int)
        self.assertEquals(infer_type_of_list([0, 1, 2.0]), float)
        self.assertEquals(infer_type_of_list(['foo', u'bar']), str)
        self.assertEquals(infer_type_of_list([array.array('d', [1, 2, 3]), array.array('d', [1, 2, 3])]), array.array)
        self.assertEquals(infer_type_of_list([[], [1, 2, 3], array.array('d', [1, 2, 3])]), list)
        self.assertEquals(infer_type_of_list([{'a': 1}, {'b': 2}]), dict)

    def test_datetime_lambda(self):
        d = dt.datetime.now()
        sa = SArray([d])

        # Lambda returning self
        sa_self = sa.apply(lambda x: x)
        for i in range(len(sa_self)):
            self.assertEqual(sa[i], sa_self[i])

        # Lambda returning year
        sa_year = sa.apply(lambda x: x.year)
        for i in range(len(sa_year)):
            self.assertEqual(sa[i].year, sa_year[i])

        # Lambda returning second
        sa_sec = sa.apply(lambda x: x.second)
        for i in range(len(sa_sec)):
            self.assertEqual(sa[i].second, sa_sec[i])
