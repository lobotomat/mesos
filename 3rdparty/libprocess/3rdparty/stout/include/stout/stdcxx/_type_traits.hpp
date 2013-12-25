#ifndef __STOUT_TYPE_TRAITS_HPP__ 
#define __STOUT_TYPE_TRAITS_HPP__

#include "config.hpp" 

#ifdef HAVE_CXX11 
	#include <type_traits> 
	#define stout_is_pod std::is_pod
#elif defined(HAVE_CXXTR1) 
	#include <tr1/type_traits> 
	#define stout_is_pod std::tr1::is_pod
#elif defined(HAVE_BOOST)
	#include "boost/type_traits.hpp"
	#define stout_is_pod boost::is_pod
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
