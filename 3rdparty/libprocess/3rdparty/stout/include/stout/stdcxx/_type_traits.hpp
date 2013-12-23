#ifndef __STOUT_TYPE_TRAITS_HPP__ 
#define __STOUT_TYPE_TRAITS_HPP__

#ifdef HAVE_CONFIG_H
	#include "config.h" 
#endif

#ifdef HAVE_CXX11 
	#include <type_traits> 
	#define _is_pod std::is_pod
#elif defined(HAVE_CXXTR1) 
	#include <tr1/type_traits> 
	#define _is_pod std::tr1::is_pod
#elif defined(HAVE_BOOST)
	#include "boost/type_traits.hpp"
	#define _is_pod boost::is_pod
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
