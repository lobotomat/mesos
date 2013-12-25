#ifndef __STOUT_SHARED_PTR_HPP__
#define __STOUT_SHARED_PTR_HPP__

#include "config.hpp"

#ifdef HAVE_CXX11 
	#include <memory> 
	#define stout_shared_ptr std::shared_ptr
	#define stout_weak_ptr std::weak_ptr
#elif defined(HAVE_CXXTR1) 
	#include <tr1/memory> 
	#define stout_shared_ptr std::tr1::shared_ptr
	#define stout_weak_ptr std::tr1::weak_ptr
#elif defined(HAVE_BOOST)
	#include "boost/shared_ptr.hpp"
	#define stout_shared_ptr boost::shared_ptr
	#define stout_weak_ptr boost::weak_ptr
#else
    #error needs either C++11, C++TR1 or BOOST enabled 
#endif 

#endif // __STOUT_SHARED_PTR_HPP__
