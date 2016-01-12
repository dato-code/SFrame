/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <globals/globals.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/s3_api.hpp>
#include <fileio/file_download_cache.hpp>
#include <cppipc/cppipc.hpp>
#include <cppipc/common/authentication_token_method.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sketch.hpp>
#include <unity/lib/unity_sarray_builder.hpp>
#include <unity/lib/unity_sframe_builder.hpp>
#include <unity/lib/version.hpp>
#include <unity/lib/simple_model.hpp>
#include <parallel/pthread_tools.hpp>
#include <logger/logger.hpp>
#include <logger/log_rotate.hpp>
#include <minipsutil/minipsutil.h>

#include <lambda/lambda_master.hpp>
#include <lambda/graph_pylambda_master.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

#include <unity/lib/gl_sarray.hpp>
#include <startup_teardown/startup_teardown.hpp>


#ifdef HAS_TCMALLOC
#include <google/malloc_extension.h>
#endif

#ifndef _WIN32
#include <crash_handler/crash_handler.hpp>
#include <signal.h>
#endif
#include <Eigen/Core>
#include "unity_server_init.hpp"

void exported_symbols();

namespace po = boost::program_options;

#ifdef HAS_TCMALLOC
/**
 *  If TCMalloc is available, we try to release memory back to the
 *  system every 15 seconds or so. TCMalloc is otherwise somewhat...
 *  aggressive about keeping memory around.
 */
static bool stop_memory_release_thread = false;
graphlab::mutex memory_release_lock;
graphlab::conditional memory_release_cond;

void memory_release_loop() {
  memory_release_lock.lock();
  while (!stop_memory_release_thread) {
    memory_release_cond.timedwait(memory_release_lock, 15);
    MallocExtension::instance()->ReleaseFreeMemory();
  }
  memory_release_lock.unlock();
}
#endif

std::string SERVER_LOG_FILE;
REGISTER_GLOBAL(std::string, SERVER_LOG_FILE, false);

// Global variables 
graphlab::toolkit_function_registry* g_toolkit_functions;
graphlab::toolkit_class_registry* g_toolkit_classes;
cppipc::comm_server* server;

void print_help(std::string program_name, po::options_description& desc) {
  std::cerr << "Unity Server version: " << UNITY_VERSION << "\n"
            << desc << "\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020\n"
            << "Example: " << program_name << " tcp://*:10020\n"
            << "Example: " << program_name << " tcp://127.0.0.1:10020 tcp://127.0.0.1:10021\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server --auth_token=auth_token_value\n"
            << "Example: " << program_name << " ipc:///tmp/unity_test_server ipc:///tmp/unity_status auth_token_value\n";
}

void init_extensions(std::string current_binary_name) {
  namespace fs = boost::filesystem;
  using namespace graphlab::fileio;
  auto path = fs::path(current_binary_name).parent_path();
  // look for shared libraries I can load
  auto unity_global = graphlab::get_unity_global_singleton();
  std::vector<decltype(path)> candidate_paths {path / "*.so", 
                                               path / "*.dylib", 
                                               path / "*.dll", 
                                               path / "../extensions/*.so", 
                                               path / "../extensions/*.dylib",
                                               path / "../extensions/*.dll"};
  // we exclude all of our own libraries
  std::vector<decltype(path)> exclude_paths {path / "*libunity*.so", 
                                             path / "*libunity*.dylib", 
                                             path / "*libunity*.dll"};
  std::set<std::string> exclude_files;
  for (auto exclude_candidates: exclude_paths) { 
    auto globres = get_glob_files(exclude_candidates.string());
    for (auto file : globres) exclude_files.insert(file.first);
  }

  for (auto candidates: candidate_paths) { 
    for (auto file : get_glob_files(candidates.string())) {
      // exclude files in the exclusion list
      if (exclude_files.count(file.first)) {
        logstream(LOG_INFO) << "Excluding load of " << file.first << std::endl;
        continue;
      }
      // exclude libhdfs
      if (boost::ends_with(file.first, "libhdfs.so")) continue;
      if (file.second == file_status::REGULAR_FILE) {
        logstream(LOG_INFO) << "Autoloading of " << file.first << std::endl;
        unity_global->load_toolkit(file.first, "..");
      }
    }
  }
}


int main(int argc, char** argv) {
#ifndef NDEBUG
  global_logger().set_log_level(LOG_DEBUG);
#endif

  Eigen::initParallel();
  //TODO: This functionality can be mirrored in Windows. Potentially with SEH,
  //WER, and possibly needing some asm code written? Too complicated without a
  //high payout to be on the critical path for now.  Revisit later.
#ifndef _WIN32
  /// Installing crash handler to print stack trace in case of segfault.
  struct sigaction sigact;
  sigact.sa_sigaction = crit_err_hdlr;
  sigact.sa_flags = SA_RESTART | SA_SIGINFO;
  // the crit_err_hdlr writes to this file, by default stderr. 
  extern std::string BACKTRACE_FNAME; 
  BACKTRACE_FNAME = std::string("/tmp/unity_server_") 
                            + std::to_string(getpid())
                            + ".backtrace";
  if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
    fprintf(stderr, "error setting signal handler for %d (%s)\n",
        SIGSEGV, strsignal(SIGSEGV));
    exit(EXIT_FAILURE);
  }
#endif

#ifdef _WIN32
  // Make sure dialog boxes don't come up for errors (apparently doesn't affect
  // "hard system errors")
  SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);

  // Don't listen to ctrl-c.  On Windows, a ctrl-c is delivered to every
  // application "sharing" the console that is selected with the mouse. This
  // causes unity_server to crash even though the client handles it correctly,
  // unless we disable ctrl-c events.
  SetConsoleCtrlHandler(NULL, true);
#endif

  graphlab::configure_global_environment(argv[0]);

  std::string program_name = argv[0];
  std::string server_address;
  std::string control_address;
  std::string publish_address;
  std::string auth_token;
  std::string secret_key;
  std::string log_file;
  bool daemon = false;
  size_t metric_server_port = 0;
  size_t log_rotation_interval = 0;
  size_t log_rotation_truncate = 0;
  boost::program_options::variables_map vm;

  // define all the command line options 
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Print this help message.")
      ("server_address", 
       po::value<std::string>(&server_address)->implicit_value(server_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens on")
      ("control_address", 
       po::value<std::string>(&control_address)->implicit_value(control_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens for control "
                         "messages on. OPTIONAL")
      ("publish_address", 
       po::value<std::string>(&publish_address)->implicit_value(publish_address),
       "This must be a valid ZeroMQ endpoint and "
                          "is the address on which the server "
                          "publishes status logs. OPTIONAL")
      ("metric_server_port", 
       po::value<size_t>(&metric_server_port)->default_value(metric_server_port),
       "This is the port number the Metrics Server listens on. It will accept "
       "connections to this port on all interfaces. If 0, will listen to a "
       "randomly assigned port. Defaults to 0. [[Deprecated]]")
      ("secret_key",
       po::value<std::string>(&secret_key),
       "Secret key used to secure the communication. Client must know the public "
       "key. Default is not to use secure communication.")
      ("auth_token", 
       po::value<std::string>(&auth_token)->implicit_value(auth_token),
       "This is an arbitrary string which is used to authenticate "
                     "the connection. OPTIONAL")
      ("daemon", po::value<bool>(&daemon)->default_value(false),
       "If set to true, will run the process in back-groundable daemon mode.")
      ("log_file",
       po::value<std::string>(&log_file),
       "The aggregated log output file. Logs will be printed to stderr as well as "
       "written to the log file ")
      ("log_rotation_interval", 
       po::value<size_t>(&log_rotation_interval)->implicit_value(60*60*24)->default_value(0),
       "The log rotation interval in seconds. If set, Log rotation will be performed. "
       "The default log rotation interval is 1 day (60*60*24 seconds). "
       "--log_file must be set for this to be meaningful. The log files will be named "
       "[log_file].0, [log_file].1, etc")
      ("log_rotation_truncate",
       po::value<size_t>(&log_rotation_truncate)->implicit_value(8)->default_value(0),
       "The maximum number of logs to keep around. If set log truncation will be performed. "
       "--log_file and --log_rotation_interval must be set for this to be meaningful.");

  po::positional_options_description positional;
  positional.add("server_address", 1);
  positional.add("control_address", 1);
  positional.add("publish_address", 1);
  positional.add("auth_token", 1);

  // try to parse the command line options
  try {
    po::command_line_parser parser(argc, argv);
    parser.options(desc);
    parser.positional(positional);
    po::parsed_options parsed = parser.run();
    po::store(parsed, vm);
    po::notify(vm);
  } catch(std::exception& error) {
    std::cout << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    print_help(program_name, desc);
    return 1;
  }

  if(vm.count("help")) {
    print_help(program_name, desc);
    return 0;
  }
 
  global_logger().set_log_level(LOG_INFO);

  if (!log_file.empty()) {
    if (log_rotation_interval) {
      graphlab::begin_log_rotation(log_file, log_rotation_interval, log_rotation_truncate);
    } else {
      global_logger().set_log_file(log_file);
    }
  }
  SERVER_LOG_FILE = log_file;

  graphlab::reap_unused_temp_files();

  logstream(LOG_EMPH) << "Unity server listening on: " <<  server_address<< std::endl;
  logstream(LOG_EMPH) << "Total System Memory Detected: " << total_mem() << std::endl;

  // Prevent multiple server listen on the same ipc device.
  namespace fs = boost::filesystem;
  if (boost::starts_with(server_address, "ipc://") && fs::exists(fs::path(server_address.substr(6)))) {
    logstream(LOG_FATAL) << "Cannot start unity server at " << server_address<< ". File already exists" << "\n";
    exit(-1);
  }

  // Use process_id to construct a default server address
  if (server_address == "default") {
    std::string path = "/tmp/graphlab_server-" + std::to_string(getpid());
    if (fs::exists(fs::path(path))) {
      // It could be a leftover of a previous crashed process, try to delete the file
      if (remove(path.c_str()) != 0) {
        logstream(LOG_FATAL) << "Cannot start unity server at " << server_address<< ". File already exists, and cannot be deleted." << "\n";
        exit(-1);
      }
    }
    server_address = "ipc://" + path;
  }

  // construct the server
  server = new cppipc::comm_server(std::vector<std::string>(), "", 
                                   server_address, control_address, publish_address, secret_key);

  // include the authentication method if the auth token is provided
  if (vm.count("auth_token")) {
    logstream(LOG_EMPH) << "Authentication Method: authentication_token Applied" << std::endl;
    server->add_auth_method(std::make_shared<cppipc::authentication_token_method>(auth_token));
  } else {
    logstream(LOG_EMPH) << "No Authentication Method." << std::endl;
  }

  g_toolkit_functions = init_toolkits();

  g_toolkit_classes = init_models();

  /**
   * Set the path to the pylambda_slave binary used for evaluate python lambdas parallel in separate processes.
   * Two possible places are relative path to the server binary in the source build,
   * and relative path to the server binary in the pip install build.
   */
  std::vector<fs::path> candidate_paths;
  #ifdef _WIN32
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("pylambda_worker.exe")));
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("../../lambda/pylambda_worker.exe")));
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("../../../oss_src/lambda/pylambda_worker.exe")));
  #else
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("pylambda_worker")));
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("../../lambda/pylambda_worker")));
  candidate_paths.push_back(fs::system_complete(fs::path(program_name).parent_path() / fs::path("../../../oss_src/lambda/pylambda_worker")));
  #endif

  /**
   * Set the path to the python executable and the pylambda_slave
   * binary script used for evaluate python lambdas parallel in
   * separate processes.  
   */
  const char* python_executable_env = std::getenv("__GL_PYTHON_EXECUTABLE__");
  if (python_executable_env) {
    graphlab::GLOBALS_PYTHON_EXECUTABLE = python_executable_env;
    logstream(LOG_INFO) << "Python executable: " << graphlab::GLOBALS_PYTHON_EXECUTABLE << std::endl;
    ASSERT_MSG(fs::exists(fs::path(graphlab::GLOBALS_PYTHON_EXECUTABLE)), "Python executable is not valid path. Do I exist?");
  } else {
    logstream(LOG_WARNING) << "Python executable not set. Python lambdas may not be available" << std::endl;
  }

  std::string pylambda_worker_script;
  {
    const char* pylambda_worker_script_env = std::getenv("__GL_PYLAMBDA_SCRIPT__");
    if (pylambda_worker_script_env) {
      logstream(LOG_INFO) << "PyLambda worker script: " << pylambda_worker_script_env << std::endl;
      pylambda_worker_script = pylambda_worker_script_env;
      ASSERT_MSG(fs::exists(fs::path(pylambda_worker_script)), "PyLambda worker script not valid.");
    } else {
      logstream(LOG_WARNING) << "Python lambda worker script not set. Python lambdas may not be available" << std::endl;
    }
  }

  // Set the lambda_worker_binary_and_args
  graphlab::lambda::lambda_master::set_lambda_worker_binary(
      std::vector<std::string>{graphlab::GLOBALS_PYTHON_EXECUTABLE, pylambda_worker_script});


  server->register_type<graphlab::unity_sgraph_base>([](){ 
                                            return new graphlab::unity_sgraph();
                                          });

  server->register_type<graphlab::model_base>([](){ 
                                            return new graphlab::simple_model();
                                          });
  server->register_type<graphlab::unity_sframe_base>([](){ 
                                            return new graphlab::unity_sframe();
                                          });
  server->register_type<graphlab::unity_sarray_base>([](){ 
                                            return new graphlab::unity_sarray();
                                          });
  server->register_type<graphlab::unity_sketch_base>([](){ 
                                            return new graphlab::unity_sketch();
                                          });
  server->register_type<graphlab::unity_sarray_builder_base>([]()->graphlab::unity_sarray_builder_base* { 
                                            return new graphlab::unity_sarray_builder();
                                          });
  server->register_type<graphlab::unity_sframe_builder_base>([]()->graphlab::unity_sframe_builder_base* { 
                                            return new graphlab::unity_sframe_builder();
                                          });


  create_unity_global_singleton(g_toolkit_functions, g_toolkit_classes, server);

  server->register_type<graphlab::unity_global_base>(
      []()->std::shared_ptr<graphlab::unity_global_base> {
        auto unity_shared_ptr = graphlab::get_unity_global_singleton();
        return std::dynamic_pointer_cast<graphlab::unity_global_base>(unity_shared_ptr);
      });

  // graphlab::launch_metric_server(metric_server_port);

  init_extensions(program_name);

  server->start();

  // set the progress observer
  global_logger().add_observer(
      LOG_PROGRESS,
      [=](int lineloglevel, const char* buf, size_t len){
        server->report_status("PROGRESS", std::string(buf, len));
      });



#ifdef HAS_TCMALLOC
  graphlab::thread memory_release_thread;
  memory_release_thread.launch(memory_release_loop);
#endif
  // make a copy of the stdin file handle since annoyingly some imported
  // libraries via dlopen might close stdin. (I am looking at you...
  // scipy/optimize/minpack2.so as distributed by anaconda)
  auto stdin_clone_fd = dup(STDIN_FILENO);
  auto stdin_clone_file = fdopen(stdin_clone_fd, "r");
  if (daemon) {
    while(1) {
      sleep(1000000);
    }
  } else {
    // lldb when breaking and continuing may
    // interrupt the getchar syscall  making this return
    // a failure. (It will return -1 (EOF) and set EAGAIN).
    // However, of course EOF is a valid value for getchar() to return really.
    // So we have to double check with feof.
    while(1) {
      int c = fgetc(stdin_clone_file);
      // returned EOF, but not actually EOF. This is an interrupted syscall.
      // continue looping.
      if (c == -1 && !feof(stdin_clone_file)) continue;
      logstream(LOG_EMPH) << "Quiting with received character: "
                          << int(c)
                          << " feof = " << feof(stdin_clone_file) << std::endl;
      // quit in all other cases.
      break;
    }
  }

#ifdef HAS_TCMALLOC
  stop_memory_release_thread = true;
  memory_release_cond.signal();
  memory_release_thread.join();
#endif

  // detach the progress observer
  global_logger().add_observer(LOG_PROGRESS, NULL);
  //graphlab::stop_metric_server();
  delete server;
  delete g_toolkit_functions;

  graphlab::global_teardown::get_instance().perform_teardown();

#ifdef _WIN32
  // windows appears to kill threads then call DLLUnload (which calls the
  // global object destructors). This is problematic since all of our
  // state is in DLLs. We can do more cleanup here or we can just die.
  // (this is going to be a problem when we eliminate the unity_server
  // subprocess)
  TerminateProcess(GetCurrentProcess(), 0);
#endif
}
