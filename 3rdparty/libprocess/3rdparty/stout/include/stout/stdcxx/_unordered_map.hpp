#ifndef __STOUT_UNORDERED_MAP_HPP__ 
#define __STOUT_UNORDERED_MAP_HPP__

#include "config.hpp" 

#ifdef HAVE_CXX11 
	#include <unordered_map> 
	#define stout_unordered_map std::unordered_map
#elif defined(HAVE_CXXTR1) 
	#include <tr1/unordered_map> 
	#define stout_unordered_map std::tr1::unordered_map
#elif defined(HAVE_BOOST)
	#include "boost/unordered_map.hpp"
	#define stout_unordered_map boost::unordered_map
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
