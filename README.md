# **THIS REPOSITORY IS DEPRECATED.**
# THE FUNCTIONALITY HAS MOVED TO [Turi Create](https://github.com/apple/turicreate). 

____

SFrame
======
<a href="https://pypi.python.org/pypi/SFrame">
   <img src="https://img.shields.io/pypi/v/SFrame.svg" alt="latest release" />
</a>
<a href="https://travis-ci.org/turi-code/SFrame">
   <img src="https://travis-ci.org/turi-code/SFrame.svg?branch=master" alt="travis build status" />
</a>

Scalable tabular (**SFrame**, **SArray**) and graph (**SGraph**) data-structures built for out-of-core data analysis. 

The SFrame package provides the complete implementation of:
 - SFrame
 - SArray
 - SGraph 
 - The C++ SDK surface area (gl_sframe, gl_sarray, gl_sgraph)

Introduction
------------

The SFrame contains the open source components [GraphLab Create](https://turi.com/products/create/) from [Turi](https://turi.com). 

Some documentation to help get started:
- [Getting started with SFrame](https://turi.com/learn/gallery/notebooks/introduction_to_sframes.html)
- [SFrame user guide](https://turi.com/learn/userguide/sframe/tabular-data.html)
- [SGraph user guide](https://turi.com/learn/userguide/sgraph/sgraph.html) 

For more details on GraphLab Create (including documentation and tutorials) see http://turi.com.

Some of the key features of this package are.

- A scalable column compressed disk-backed dataframe optimized for machine learning and data science needs.
- Designed for both **tabular** (SFrame, SArray) as well as **graph** data (SGraph)
- Support for **strictly typed** columns (int, float, str, datetime), **weakly typed** columns (schema free lists, dictionaries) as well as **specialized types** such as Image.
- Uniform support for **missing data**.
- Query optimization and Lazy evaluation.
- A C++ API (gl_sarray, gl_sframe, gl_sgraph) with direct native access via the C++ SDK.
- A Python API (SArray, SFrame, SGraph) with an indirect access via an interprocess layer.

License
-------
The SFrame Package is licensed under a BSD license. See [license](LICENSE) file.

Installation
------------
We release an updated version about once every 1.5 months or so. You can download
the most recent version directly from [pypi](https://pypi.python.org/pypi/SFrame)
or using pip:

    pip install -U sframe

**Requirements**

SFrame requires a 64-bit operating system.

**Operating Systems**
- Mac OS X: 10.8+
- Linux: Any distribution with GLIBC >= 2.11
 - Ubuntu >= 11.04 
 - Debian >= 6 
 - RHEL >= 6 
 - SLES >= 11
- Windows (7, 8, 10, Server 2012 R2)

**Python**
- Python 2.7.x
- Pyhton 3.4.x
- **Note**: Unfortunately, Python 3.5.x is currently not supported. This is coming soon.

Build Dependencies
------------------
To simplify the development process, we use a pre-built dependency toolkit we
call dato-deps which prepackages all compile-time dependencies like boost, curl,
etc, some with patches which we should contribute back.  On Linux, we also
prepackage the entire compiler toolchain. On configuration, dato-deps is
downloaded and unpacked into the deps/ directory

### OS X
At least OS X 10.9. OS X 10.10 preferred.

* On OS X: Apple XCode 6 Command Line Tools [Required]
  +  "clang --version" should report at least
     "Apple LLVM version 6.1.0 (clang-602.0.53) (based on LLVM 3.6.0svn)"

* ccache [Optional]
   + Not required, but highly recommended

* cmake >= 3.2 [Optional]
   + Not required, but highly recommended

### Linux

* *nix build tools: patch, make [Required]
   +  Should come with most Mac/Linux systems by default. Recent Ubuntu versions
   will require installing the build-essential package.

* zlib [Required]
   +   Comes with most Mac/Linux systems by default. Recent Ubuntu version will
   require the zlib1g-dev package.

* ccache [Optional]
   + Not required, but highly recommended

* cmake >= 3.2 [Optional]
   + Not required, but highly recommended

### Windows

This is somewhat more involving. See the wiki.


Compiling
---------
After you have done a git pull, cd into the SFrame directory and run the configure script:

    cd SFrame
    ./configure

Running configure will create two sub-directories, *release* and *debug*.  Select 
either of these modes/directories and navigate to the *oss_src/unity* subdirectory:

    cd <repo root>/debug/oss_src/unity/python
   
   or
   
    cd <repo root>/release/oss_src/unity/python

Running **make** will build the project, according to the mode selected. 

We recommend using make's parallel build feature to accelerate the compilation
process. For instance:

    make -j 4

Will perform up to 4 build tasks in parallel. The number of tasks to run in
parallel should be roughly equal to the number of CPUs you have.

Running in the Build Tree
-------------------------
To avoid complicated interactions with python installed in your system, we
install an entire miniconda toolchain into deps/.

In order to use the dev build that you have just compiled, some environment
variables will need to be set.  This can be done by sourcing a script. You'll
need to pass the script either "debug" or "release" depending on the type of
build you have compiled:
  
    source <repo root>/oss_local_scripts/python_env.sh [debug | release ]

This will activate the miniconda toolchain, and you can run python directly
and import sframe, etc.
 
Running Unit Tests
------------------

### Running Python unit tests

There is a script that makes it easy to run the python unit test. You will just need to call it and pass it
your build type (either "debug" or "release).

    <repo root>/oss_local_scripts/run_python_test.sh [debug | release]

Alternatively, you can run nosetests directly. For instance, if I am using
the debug build directory:

Activate the python environment

    source <repo root>/oss_local_scripts/python_env.sh debug

Switch the build output directory and run nosetests

    cd <repo root>/debug/src/unity/python
    nosetests -s -v sframe/test

### Running C++ Units Tests
 
There are also C++ tests. To compile and run them, do the following:

    cd <repo root>/[debug | release]/oss_test
    make
    ctest
  
Compilation and Execution Summary
---------------------------------
To summarize the above, to just build and run SFrame in the debug build directory:

    ./configure
    cd debug/oss_src/unity
    make -j4

    # where root is the checked out SFrame directory
    source <root>/oss_local_scripts/python_env.sh debug

    # import sframe should work here
    python


Packaging
---------
To build an egg for your platform, run

    <repo root>/oss_local_scripts/make_egg.sh

(See --help for options)

SDK
---
See: https://github.com/turi-code/GraphLab-Create-SDK
