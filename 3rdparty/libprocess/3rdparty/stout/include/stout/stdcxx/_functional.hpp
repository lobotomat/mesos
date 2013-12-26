#ifndef __STOUT_FUNCTIONAL_HPP__ 
#define __STOUT_FUNCTIONAL_HPP__

#include "config.hpp" 

#ifdef HAVE_CXX11 
	#include <functional> 
	#define stout_function std::function
	#define stout_bind std::bind
	#define stout_placeholders std::placeholders
#elif defined(HAVE_CXXTR1) 
	#include <tr1/functional> 
	#define stout_function std::tr1::function
	#define stout_bind std::tr1::bind
	#define stout_placeholders std::tr1::placeholders
#elif defined(HAVE_BOOST)
	#include "boost/functional.hpp"
	#include "boost/function.hpp"
	#include "boost/bind.hpp"
	#define stout_function boost::function
	#define stout_bind boost::bind
	#define stout_placeholders boost::placeholders
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif 
