#ifndef SAFEQUEUE_H
#define SAFEQUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

template <typename T>
class SafeQueue {
public:
  void push(T value) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_queue.push(std::move(value));
    m_cond.notify_one();
  }

  bool pop(T& value) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cond.wait(lock, [this] { return !m_queue.empty() || m_stop; });
    if (m_queue.empty() && m_stop) {
      return false;
    }

    value = std::move(m_queue.front());
    m_queue.pop();
    return true;
  }

  void stop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_stop = true;
    m_cond.notify_all();
  }

private:
  std::queue<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_cond;
  bool m_stop = false;
};

#endif
