#!/usr/bin/env python
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

import os
import sys
import glob
import subprocess
from setuptools import setup, find_packages
from setuptools.dist import Distribution
from setuptools.command.install import install

# Prevent distutils from thinking we are a pure python package
class BinaryDistribution(Distribution):
    def is_pure(self):
        return False

class InstallEngine(install):
    """Helper class to hook the python setup.py install path to download client libraries and engine"""

    def run(self):
        import platform

        # start by running base class implementation of run
        install.run(self)

        # Check correct version of architecture (64-bit only)
        arch = platform.architecture()[0]
        if arch != '64bit':
            msg = ("SFrame currently supports only 64-bit operating systems, and only recent Linux/OSX " +
                   "architectures. Please install using a supported version. Your architecture is currently: %s" % arch)

            sys.stderr.write(msg)
            sys.exit(1)

        # Check correct version of Python
        if sys.version_info.major == 2 and sys.version_info[:2] < (2, 7):
            msg = ("SFrame requires at least Python 2.7, please install using a supported version."
                   + " Your current Python version is: %s" % sys.version)
            sys.stderr.write(msg)
            sys.exit(1)

        # if OSX, verify > 10.7
        from distutils.util import get_platform
        from pkg_resources import parse_version
        cur_platform = get_platform()
        py_shobj_ext = 'so'

        if cur_platform.startswith("macosx"):

            mac_ver = platform.mac_ver()[0]
            if parse_version(mac_ver) < parse_version('10.8.0'):
                msg = (
                "SFrame currently does not support versions of OSX prior to 10.8. Please upgrade your Mac OSX "
                "installation to a supported version. Your current OSX version is: %s" % mac_ver)
                sys.stderr.write(msg)
                sys.exit(1)
        elif cur_platform.startswith('linux'):
            pass
        elif cur_platform.startswith('win'):
            py_shobj_ext = 'pyd'
            win_ver = platform.version()
            # Verify this is Vista or above
            if parse_version(win_ver) < parse_version('6.0'):
                msg = (
                "SFrame currently does not support versions of Windows"
                " prior to Vista, or versions of Windows Server prior to 2008."
                "Your current version of Windows is: %s" % platform.release())
                sys.stderr.write(msg)
                sys.exit(1)
        else:
            msg = (
                "Unsupported Platform: '%s'. SFrame is only supported on Windows, Mac OSX, and Linux." % cur_platform
            )
            sys.stderr.write(msg)
            sys.exit(1)

        print ("")
        print ("")
        print ("")
        print ("Thank you for downloading and trying SFrame.")
        print ("")
        print ("")
        print ("")

        from distutils import sysconfig
        import stat
        import glob

        root_path = os.path.join(self.install_lib, 'sframe')


if __name__ == '__main__':
    from distutils.util import get_platform
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Other Audience",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ]
    cur_platform = get_platform()
    if cur_platform.startswith("macosx"):
        classifiers.append("Operating System :: MacOS :: MacOS X")
    elif cur_platform.startswith('linux'):
        classifiers +=  ["Operating System :: POSIX :: Linux",
                         "Operating System :: POSIX :: BSD",
                         "Operating System :: Unix"]
    elif cur_platform.startswith('win'):
        classifiers += ["Operating System :: Microsoft :: Windows",
                        "Operating System :: Microsoft :: Windows 7",
                        "Operating System :: Microsoft :: Windows Server 2008",
                        "Operating System :: Microsoft :: Windows Vista"]
    else:
        msg = (
            "Unsupported Platform: '%s'. SFrame is only supported on Windows, Mac OSX, and Linux." % cur_platform
            )
        sys.stderr.write(msg)
        sys.exit(1)

    version_number='1.8.4'#{{VERSION_STRING}}
    setup(
        name="SFrame",
        version=version_number,
        author='Dato, Inc.',
        author_email='contact@dato.com',
        cmdclass=dict(install=InstallEngine),
        distclass=BinaryDistribution,
        package_data={
        'sframe': ['cython/*.so', 'cython/*.pyd', 'cython/*.dll',
                     '*.so', '*.so.1', '*.dylib',
                     '*.dll', '*.def', 'spark_unity.jar',
                     'deploy/*.jar', '*.exe', 'libminipsutil.*'
                     ]},
        packages=find_packages(
            exclude=["*.tests", "*.tests.*", "tests.*", "tests", "*.test", "*.test.*", "test.*", "test"]),
        url='https://dato.com',
        license='LICENSE.txt',
        description='SFrame is an scalable, out-of-core dataframe, which allows you to work with datasets that are larger than the amount of RAM on your system.',
        # long_description=open('README.txt').read(),
        classifiers=classifiers,
        install_requires=[
            "boto == 2.33.0",
            "decorator == 3.4.0",
            "tornado == 4.1",
            "prettytable == 0.7.2",
            "requests == 2.9.1",
            "awscli == 1.6.2",
            "multipledispatch>=0.4.7",
            "certifi==2015.04.28" # we need to downgrade certifi to work with S3
        ],
    )
