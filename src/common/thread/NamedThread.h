/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_THREAD_NAMEDTHREAD_H_
#define COMMON_THREAD_NAMEDTHREAD_H_

#include "common/base/Base.h"
#include "common/base/CommonMacro.h"

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef PLATFORM_MACOS
#include <sys/prctl.h>
#else
#include <pthread.h>
#endif
#include <sys/syscall.h>
#include <unistd.h>

namespace nebula {
namespace thread {

pid_t gettid();

class NamedThread final : public std::thread {
 public:
  NamedThread() = default;
  NamedThread(NamedThread &&) = default;
  template <typename F, typename... Args>
  NamedThread(const std::string &name, F &&f, Args &&...args);
  NamedThread &operator=(NamedThread &&) = default;
  NamedThread(const NamedThread &) = delete;
  NamedThread &operator=(const NamedThread &) = delete;

 public:
  class Nominator {
   public:
    explicit Nominator(const std::string &name) {
      get(prevName_);
      set(name);
    }

    ~Nominator() { set(prevName_); }

    static void set(const std::string &name) {
#ifndef PLATFORM_MACOS
      ::prctl(PR_SET_NAME, name.c_str(), 0, 0, 0);
#endif
    }

    static void get(std::string &name) {
      char buf[64];
#ifndef PLATFORM_MACOS
      ::prctl(PR_GET_NAME, buf, 0, 0, 0);
#endif
      name = buf;
    }

   private:
    std::string prevName_;
  };

 private:
  static void hook(const std::string &name, const std::function<void()> &f) {
    if (!name.empty()) {
#ifndef PLATFORM_MACOS
      Nominator::set(name);
#endif
    }
    f();
  }
};

template <typename F, typename... Args>
NamedThread::NamedThread(const std::string &name, F &&f, Args &&...args)
    : std::thread(hook, name, std::bind(std::forward<F>(f), std::forward<Args>(args)...)) {
#ifdef PLATFORM_MACOS
  pthread_setname_np(name.c_str());
#endif
}

}  // namespace thread
}  // namespace nebula

#endif  // COMMON_THREAD_NAMEDTHREAD_H_
