#ifndef __STOUT_FUNCTIONAL_HPP__ 
#define __STOUT_FUNCTIONAL_HPP__

#ifdef HAVE_CONFIG_H
	#include "config.h" 
#endif

#ifdef HAVE_CXX11 
	#include <functional> 
	#define _function std::function
	#define _bind std::bind
	#define _placeholders std::placeholders
#elif defined(HAVE_CXXTR1) 
	#include <tr1/functional> 
	#define _function std::tr1::function
	#define _bind std::tr1::bind
	#define _placeholders std::tr1::placeholders
#elif defined(HAVE_BOOST)
	#include "boost/functional.hpp"
	#include "boost/function.hpp"
	#include "boost/bind.hpp"
	#define _function boost::function
	#define _bind boost::bind
	#define _placeholders boost::placeholders
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
