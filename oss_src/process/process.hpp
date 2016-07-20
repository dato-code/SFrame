/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef PROCESS_HPP
#define PROCESS_HPP

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1
#endif
#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

#include <string>
#include <vector>
#include <util/syserr_reporting.hpp>

#ifdef _WIN32
#include <cross_platform/windows_wrapper.hpp>
#endif
#ifdef __APPLE__
#include <sys/types.h>
#endif
#ifdef __linux__
#include <sys/types.h>
#endif
namespace graphlab
{

//TODO: Is there a better name for this?? Perhaps it can be grouped with other
//cross-platform stuff?
class process {
 public:
  process() {};
  ~process();

  /**
   * A "generic" process launcher.
   * 
   * Launched the command with the given arguments as a separate child process.
   *
   * This function does not throw.
   */
  bool launch(const std::string &cmd,
              const std::vector<std::string> &args);

  /**
   * A generic implementation of popen in read mode.
   *
   * This means that whatever the child writes on the given file descriptor
   * (child_write_fd) can be read by calling read_from_child. On Unix systems,
   * this could be any file descriptor inherited by the child from the parent.
   * On Windows, we only accept STDOUT_FILENO and STDERR_FILENO.
   *
   * NOTE: The STD*_FILENO constants are Unix specific, but defined in this
   * header for convenience.
   *
   * This function does not throw.
   */
  bool popen(const std::string &cmd,
             const std::vector<std::string> &args,
             int child_write_fd);
  /**
   * If we've set up a way to read from the child, use this to read.
   *
   * Returns -1 on error, otherwise bytes received
   *
   * Throws if a way to read was not set up or if process was not launched.
   */
  ssize_t read_from_child(void *buf, size_t count);

  std::string read_from_child();
  /**
   * Kill the launched process
   *
   * Throws if process was never launched.
   */
  bool kill(bool async=true);

  /**
   * Check if the process launched is running.
   *
   * Throws if process was never launched.
   */
  bool exists();

  /**
   * Return the process's return code if it has exited.
   *
   * Returns INT_MIN if the process is still running.
   * Returns INT_MAX if getting the error code failed for some other reason.
   */
  int get_return_code();

  void close_read_pipe();

  size_t get_pid();

  /**
   * Mark that this process should be automatically reaped.
   * In which case, get_return_code() will not work.
   */
  void autoreap();
 private:
  //***Methods
  process(process const&) = delete;
  process& operator=(process const&) = delete;
  
#ifdef _WIN32
  //***Methods
  //***Variables

  // Handle needed to interact with the process
  HANDLE m_proc_handle = NULL;

  // Handle needed for parent to read from child
  HANDLE m_read_handle = NULL;

  // Handle needed for child to write to parent
  HANDLE m_write_handle = NULL;

  // Duplicate of handle so child can pipe stderr and stdout to console.
  HANDLE m_stderr_handle = NULL;
  HANDLE m_stdout_handle = NULL;

  DWORD m_pid = DWORD(-1);

  BOOL m_launched = FALSE;

  BOOL m_launched_with_popen = FALSE;
#else

  //***Variables
  // Handle needed to write to this process
  int m_read_handle = -1;

  pid_t m_pid = 0;

  bool m_launched = false;

  bool m_launched_with_popen = false;
#endif
};

} // namespace graphlab
#endif //PROCESS_HPP
