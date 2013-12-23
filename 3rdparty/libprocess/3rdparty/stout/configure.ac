# Generated with autoscan, then modified appropriately.
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.61])
AC_INIT([stout], [0.1.0])

AC_LANG([C++])

# Initialize automake.
AM_INIT_AUTOMAKE([1.10])

AC_ARG_WITH([cpp11],
            [AS_HELP_STRING([--with-cpp11],
              [compile using the C++11 standard @<:@default=check@:>@])],
            [],
            [with_cpp11=check])

#check user values on C++11 usage
AS_IF([test "x$with_cpp11" = "xyes"], 
[
  AC_DEFINE(HAVE_CXX11, 1, [Whether the compiler supports ISO C++ 11])
],
[test "x$with_cpp11" = "xcheck"], 
[
  #checking if C++11 is available
  AX_CXX_COMPILE_STDCXX_11([noext], [optional])
])

#checking if the C++ standard library supports TR1 specifics 
AX_CXX_HEADER_STDCXX_TR1()
AS_IF([test "x$ax_cv_cxx_stdcxx_tr1" = "xyes"],
[
  AC_DEFINE(HAVE_CXXTR1, 1, [Whether the C++ standard library supports ISO C++ TR1])
])

#check for BOOST 1.55 or higher
AX_BOOST_BASE(1.55.0, [], [])


AC_CONFIG_HEADERS([include/stout/config.hpp])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT


AC_MSG_RESULT([--------------------------------------])
AC_MSG_RESULT([Compiling C++ with: $CXX])
AC_MSG_RESULT([Using flags: $CXXFLAGS])
AC_MSG_RESULT([Linker flags: $LDFLAGS $LIBS])
AC_MSG_RESULT([--------------------------------------])
AC_MSG_CHECKING([whether ISO C++ 11 is enabled])
AC_EGREP_CPP(yes_have_cxx11, [
#ifdef HAVE_CXX11
yes_have_cxx11
#endif ], [ AC_MSG_RESULT([yes]) ], [ AC_MSG_RESULT([no]) ]) 
AC_MSG_CHECKING([whether ISO C++ TR1 is enabled])
AC_EGREP_CPP(yes_have_cxxtr1, [
#ifdef HAVE_CXXTR1
yes_have_cxxtr1
#endif ], [ AC_MSG_RESULT([yes]) ], [ AC_MSG_RESULT([no]) ]) 
AC_MSG_CHECKING([whether BOOST is enabled])
AC_EGREP_CPP(yes_have_boost, [
#ifdef HAVE_BOOST
yes_have_boost
#endif ], [ AC_MSG_RESULT([yes]) ], [ AC_MSG_RESULT([no]) ]) 
AC_MSG_RESULT([======================================])
