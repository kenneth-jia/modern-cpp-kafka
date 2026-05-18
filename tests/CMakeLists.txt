#---------------------------
# googletest
#---------------------------
if (DEFINED ENV{GTEST_ROOT})
    set(GTEST_ROOT $ENV{GTEST_ROOT})
else ()
    if (EXISTS "/usr/include/gtest/gtest.h")
        set(GTEST_ROOT /usr)
    elseif (EXISTS "/opt/homebrew/include/gtest/gtest.h")
        set(GTEST_ROOT /opt/homebrew)
    endif ()
endif ()

if (NOT EXISTS "${GTEST_ROOT}/include/gtest/gtest.h")
    message(FATAL_ERROR "Could not find headers for gtest!")
endif ()

if (NOT (EXISTS "${GTEST_ROOT}/lib/libgtest_main.a"
         OR EXISTS "${GTEST_ROOT}/lib/libgtest_main.so"
         OR EXISTS "${GTEST_ROOT}/lib/gtest_main.lib"))
    message(FATAL_ERROR "Could not find library for gtest!")
endif ()

message(STATUS "googletest root directory: ${GTEST_ROOT}")

include_directories(SYSTEM ${GTEST_ROOT}/include)
link_directories(${GTEST_ROOT}/lib ${GTEST_ROOT}/bin)


#---------------------------
# Sub-directories
#---------------------------
enable_testing()

add_subdirectory(unit)
add_subdirectory(integration)
add_subdirectory(robustness)
