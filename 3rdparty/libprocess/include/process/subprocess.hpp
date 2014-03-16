#ifndef __PROCESS_SUBPROCESS_HPP__
#define __PROCESS_SUBPROCESS_HPP__

#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <string>

#include <glog/logging.h>

#include <process/future.hpp>

#include <stout/lambda.hpp>
#include <stout/memory.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>

namespace process {

// Represents a fork() exec()ed subprocess. Access is provided to
// the input / output of the process, as well as the exit status.
// The input / output file descriptors are only closed after both:
//   1. The subprocess has terminated, and
//   2. There are no longer any references to the associated
//      Subprocess object.
struct Subprocess
{
  // Returns the pid for the subprocess.
  pid_t pid() const { return data->pid; }

  // File descriptor accessors for input / output.
  int in()  const { return data->in;  }
  int out() const { return data->out; }
  int err() const { return data->err; }

  // Returns a future from process::reap of this subprocess.
  // Discarding this future has no effect on the subprocess.
  Future<Option<int> > status() const { return data->status; }

private:
  Subprocess() : data(new Data()) {}
  friend Try<Subprocess> subprocess(
      const std::string& command,
      const std::map<std::string, std::string>& env,
      const lambda::function<void()>& inChild);

  struct Data
  {
    ~Data()
    {
      os::close(in);
      os::close(out);
      os::close(err);
    }

    pid_t pid;

    // NOTE: stdin, stdout, stderr are macros on some systems, hence
    // these names instead.
    int in;
    int out;
    int err;

    Future<Option<int> > status;
  };

  memory::shared_ptr<Data> data;
};


namespace internal {

// Used to build the environment to pass to the subprocess.
class Envp
{
public:
  explicit Envp(const std::map<std::string, std::string>& env);
  ~Envp();

  char** operator () () const { return envp_; }

private:
  Envp();  // = delete
  Envp(const Envp&);  // = delete

  char** envp_;
  size_t size_;
};

}

// Runs the provided command in a subprocess.
// NOTE: Take extra care about the design of the inChild function
// lambda as it must not contain any async unsafe code.
Try<Subprocess> subprocess(
    const std::string& command,
    const std::map<std::string, std::string>& env =
      std::map<std::string, std::string>(),
    const lambda::function<void()>& inChild = NULL);

} // namespace process {

#endif // __PROCESS_SUBPROCESS_HPP__
