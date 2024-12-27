/**
 * Wrapper class that makes sure a thread is joined and not destroyed in case an exception is thrown. This is
 * because a call to join() is liable to be skipped uf an exception is thrown after the thread has been started
 * but before the call to join().
 *
 * To avoid the application (and thread) to be terminated when an exception is thrown, we need to also call join() in
 * presence of an exception ti avoid accedental lifetime problems.
 *
 * We can either do it like so:
 *
 * struct func;
 * void f() {
 *    int some_local_state = 0;
 *    func my_func(some_local_state);
 *    std::thread t(my_func);
 *    try {
 *        do_something_in_current_thread();
 *    } catch(...) {
 *        t.join();
 *        throw;
 *    }
 *    t.join();
 * }
 *
 * or as we do in this class, is to use RAII and provide a class the does the join() in its destructor:
 *
 * struct func;
 * void f() {
 *    int some_local_state = 0;
 *    func my_func(some_local_state);
 *    std::thread t(my_func);
 *
 *    ThreadGuard g(t);
 *
 *    do_something_in_current_thread();
 * }
 *
 */
#ifndef THREAD_GUARD_H
#define THREAD_GUARD_H

#include <thread>

class ThreadGuard
{
  std::thread &m_t;

public:
  // explicit: prevent the compiler from using that constructor for implicit
  // conversions
  explicit ThreadGuard(std::thread &t_) : m_t(t_) {}

  ~ThreadGuard()
  {
    if (m_t.joinable())
    {
      m_t.join();
    }
  }

  ThreadGuard(const ThreadGuard &) = delete;
  ThreadGuard &operator=(const ThreadGuard &) = delete;
};

#endif
