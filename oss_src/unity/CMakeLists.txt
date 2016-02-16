project(unity)

subdirs(
  lib
  toolkits
  )
# -----------------------------------------------------------------------
# On windows, the "unity" meta-library really comprises of 2 libraries
#  - unity_core and unity_toolkits
# On Mac/Linux, the "unity meta-library" comprises of only
#  - unity 
#
# In all cases,
#  - "unity" is a meta-library pointing to the shared library
#  - UNITY_ALL_LIBRARIES is a list of the shared libraries inside the "unity" meta library
#  - UNITY_EXTENSION_LIBRARY is the shared library to link to when compiling an extension.
#    (unity_core in the windows case and unity in the mac/linux case)
# -----------------------------------------------------------------------
if (WIN32)
        # this is the annoying part. For windows we just want to collect
        # a subset 
        make_shared_library_from_static(unity_toolkits_shared
                REQUIRES
                unity_image_export
                unity_core_shared # this will refer to the shared library
                DIRECT)

        make_empty_library(unity REQUIRES unity_toolkits_shared unity_core_shared)
        set(UNITY_ALL_LIBRARIES unity_toolkits_shared unity_core_shared)
        set(UNITY_EXTENSION_LIBRARY unity_core_shared)
else()

        make_shared_library_from_static(unity_shared
                REQUIRES
                unity_core
                unity_toolkits
                cppipc
                )
	      
        make_empty_library(unity REQUIRES unity_shared)
        set(UNITY_ALL_LIBRARIES unity_shared)
        set(UNITY_EXTENSION_LIBRARY unity_shared)
endif()



if(NOT WIN32)
ADD_CUSTOM_TARGET (sdk ALL)
endif()
ADD_CUSTOM_COMMAND(
  COMMENT "build sdk"
  COMMAND
  ${CMAKE_COMMAND} -P
  ReplicateSDKSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  TARGET sdk
)

ADD_CUSTOM_TARGET (clean_sdk)
ADD_CUSTOM_COMMAND(
  COMMENT "clean sdk"
  COMMAND rm
  ARGS    -Rf sdk
  TARGET clean_sdk
)
subdirs(extensions)
subdirs(server)

subdirs(python)

