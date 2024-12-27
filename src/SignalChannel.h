#ifndef SIGNAL_CHANNEL_H
#define SIGNAL_CHANNEL_H

#include <atomic>
#include <condition_variable>

class SignalChannel
{
public:
    std::atomic<bool> m_shutdown_requested = false;
    std::mutex m_cv_mutex;
    std::condition_variable m_cv;
};

#endif