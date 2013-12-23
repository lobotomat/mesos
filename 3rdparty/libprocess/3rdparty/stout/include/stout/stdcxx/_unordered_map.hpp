#ifndef __STOUT_UNORDERED_MAP_HPP__ 
#define __STOUT_UNORDERED_MAP_HPP__

#ifdef HAVE_CONFIG_H
	#include "config.h" 
#endif

#ifdef HAVE_CXX11 
	#include <unordered_map> 
	#define _unordered_map std::unordered_map
#elif defined(HAVE_CXXTR1) 
	#include <tr1/unordered_map> 
	#define _unordered_map std::tr1::unordered_map
#elif defined(HAVE_BOOST)
	#include "boost/unordered_map.hpp"
	#define _unordered_map boost::unordered_map
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
