'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from ..data_structures.sarray_builder import SArrayBuilder
import unittest
import array
import datetime as dt
from ..util.timezone import GMT

class SArrayBuilderTest(unittest.TestCase):
    def __test_equal(self, _sarray, _data, _type):
        self.assertEqual(_sarray.dtype(), _type)
        self.assertEqual(len(_sarray), len(_data))
        self.assertSequenceEqual(list(_sarray.head(_sarray.size())), _data)

    def __test_append(self, sb, data, dtype):
        for i in data:
            sb.append(i)
        self.assertEquals(sb.get_type(), dtype)
        sa = sb.close()
        self.__test_equal(sa, data, dtype)

    def __test_append_multiple(self, sb, data, dtype):
        sb.append_multiple(data)
        self.assertEquals(sb.get_type(), dtype)
        sa = sb.close()
        self.__test_equal(sa, data, dtype)

    def test_basic(self):
        data_to_test = [([1,-1,None,2],int),
                        ([i for i in range(20000)], int),
                        ([None, 1.0, -1.0, 2.3],float),
                        (["hi", None, "hello", "None"],str),
                        ([dt.datetime(2013, 5, 7, 10, 4, 10),
                          dt.datetime(1902, 10, 21, 10, 34, 10).replace(tzinfo=GMT(0.0)),None],dt.datetime),
                        ([["hi",1],None,["hi",2,3],["hello"]],list),
                        ([array.array('d',[1.0,2.0]),array.array('d',[3.0,4.0]),None],array.array),
                        ([{'a':1,'b':2},{'c':3,'d':4},None],dict),
                        ]
        for i in data_to_test:
            sb = SArrayBuilder()
            self.__test_append(sb, i[0], i[1])

            sb = SArrayBuilder()
            self.__test_append_multiple(sb, i[0], i[1])

            sb = SArrayBuilder(dtype=i[1])
            self.__test_append(sb, i[0], i[1])

            sb = SArrayBuilder(dtype=i[1])
            self.__test_append_multiple(sb, i[0], i[1])

    def test_history(self):
        sb = SArrayBuilder(history_size=10)
        sb.append_multiple((i for i in xrange(8)))
        hist = sb.read_history(3)
        self.assertEquals(hist,[5,6,7])

        hist = sb.read_history(20)
        self.assertEquals(hist, [i for i in range(8)])
        hist = sb.read_history()
        self.assertEquals(hist, [i for i in range(8)])

        sb.append_multiple((i for i in xrange(5)))
        hist = sb.read_history(10)
        self.assertEquals(hist, [3,4,5,6,7,0,1,2,3,4])

        sb.append(50)
        hist = sb.read_history(10)
        self.assertEquals(hist, [4,5,6,7,0,1,2,3,4,50])

        hist = sb.read_history(-1)
        self.assertEquals(hist, [])
        hist = sb.read_history(0)
        self.assertEquals(hist, [])

        sa = sb.close()
        self.__test_equal(sa,[i for i in range(8)] + [i for i in range(5)] + [50],int)

    def test_segments(self):
        sb = SArrayBuilder(num_segments=4)

        sb.append_multiple((i for i in xrange(20,30)), segment=2)
        sb.append_multiple((i for i in xrange(10,20)), segment=1)
        sb.append_multiple((i for i in xrange(30,40)), segment=3)
        sb.append_multiple((i for i in xrange(0,10)), segment=0)

        sa = sb.close()
        self.__test_equal(sa, range(40), int)
