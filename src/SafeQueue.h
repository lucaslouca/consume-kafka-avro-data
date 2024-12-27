#ifndef SAFE_QUEUE_H
#define SAFE_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

// A threadsafe-queue.
template <typename T>
class SafeQueue {
   public:
    SafeQueue(void) : m_queue(), m_mutex(), m_cv() {}

    ~SafeQueue(void) {}

    // Add an element to the queue.
    void enqueue(T t) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push(t);
        m_cv.notify_all();
    }

    // Get the "front"-element.
    // If the queue is empty, wait till a element is avaiable.
    T dequeue(void) {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty()) {
            // release lock as long as the wait and reaquire it afterwards.
            m_cv.wait(lock);
        }
        T val = m_queue.front();
        m_queue.pop();
        return val;
    }

    void dequeue_with_timeout(const int ms, T &val) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait_for(lock, std::chrono::milliseconds(ms), [this]() {
            bool stop_waiting = !m_queue.empty();
            return stop_waiting;
        });

        if (!m_queue.empty()) {
            val = m_queue.front();
            m_queue.pop();
        }
    }

    size_t size() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

   private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
};
#endif
