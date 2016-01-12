'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

class dbapi2_mock(object):
    def __init__(self):
        # Mandated globals
        self.apilevel = "2.0 "
        self.threadsafety = 3
        self.paramstyle = 'qmark'
        self.STRING = 41
        self.BINARY = 42
        self.DATETIME = 43
        self.NUMBER = 44
        self.ROWID = 45
