#include "Logging.h"
#include "ThreadGuard.h"

static std::string name = "LogProcessor";
SafeQueue<std::string> log_queue;

Logging::LogProcessor::LogProcessor(std::atomic<size_t> *active_processors, std::condition_variable *log_cv, std::mutex *log_cv_mutex) : m_active_processors(active_processors),
                                                                                                                                         m_log_cv(log_cv),
                                                                                                                                         m_log_cv_mutex(log_cv_mutex)
{
    // Logging::configure({{"type", "file"}, {"file_name", "flycatcher.log"}, {"reopen_interval", "1"}});
    // Logging::configure({{"type", "std_out"}});
    Logging::configure({{"type", "daily"}, {"file_name", "logs/flycatcher.log"}, {"hour", "2"}, {"minute", "30"}});
}

bool Logging::LogProcessor::start()
{
    m_t = std::make_unique<std::thread>(&Logging::LogProcessor::run, this);
    return true;
}

void Logging::LogProcessor::join() const
{
    ThreadGuard g(*m_t);
}

void Logging::LogProcessor::run()
{
    m_should_run = true;
    while (m_should_run)
    {
        // Do not log when we have active processors. Processors have priority over logging.

        {
            /*
            Without blocks log_lock doesn't release until the iteration ends. Right after that iteration ends,
            it starts the next one. This means, we only have time between the destruction of log_lock and then
            the initialization of log_lock in the next iteration. Since that happens as basically the next instruction
            there basically isn't any time for CsvProcessor to acquire a lock on the mutex.
            */

            // Lock first, then check predicate, if false unlock and then wait
            std::unique_lock log_lock(*m_log_cv_mutex);

            /*
            When it wakes up it tries to re-lock the mutex and check the
            predicate.
            */
            m_log_cv->wait_for(log_lock, std::chrono::hours(1),
                               /*
                               When the condition variable is woken up (spurious or through notify_all())
                               and this predicate returns true, the wait is stopped.
                               */
                               [this]()
                               {
                        bool stop_waiting = !m_active_processors->load();
                        return stop_waiting; });

        } // wait() reacquired the lock on exit. So we release it here since there is no reason to hold it while printing.

        /*
        Read and log
        Important: after unlocking as we don't want to block strategies while waiting for dequeue if queue is empty
        */
        std::string message = log_queue.dequeue();
        Logging::log(message);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } // end while

    Logging::log("Shutdown requested. Processing remaining " + std::to_string(log_queue.size()) + " messages...", Logging::Level::INFO, name);
    std::string msg;
    log_queue.dequeue_with_timeout(1000, msg);
    while (!msg.empty())
    {
        Logging::log(msg, Logging::Level::INFO);
        msg.clear();
        log_queue.dequeue_with_timeout(1000, msg);
    }

    Logging::log("Shutting down", Logging::Level::INFO, name);
}

void Logging::LogProcessor::stop()
{
    m_should_run = false;
}

Logging::LogProcessor::~LogProcessor()
{
}