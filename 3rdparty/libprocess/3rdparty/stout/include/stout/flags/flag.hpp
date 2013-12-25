#ifndef __STOUT_FLAGS_FLAG_HPP__
#define __STOUT_FLAGS_FLAG_HPP__

#include <string>

#include "stdcxx/_functional.hpp"

#include <stout/nothing.hpp>
#include <stout/try.hpp>

namespace flags {

// Forward declaration.
class FlagsBase;

struct Flag
{
  std::string name;
  std::string help;
  bool boolean;
  _function<Try<Nothing>(FlagsBase*, const std::string&)> loader;
  _function<Option<std::string>(const FlagsBase&)> stringify;
};

} // namespace flags {

#endif // __STOUT_FLAGS_FLAG_HPP__
