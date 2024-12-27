#include "KafkaPoller.h"
#include "ThreadGuard.h"
#include "logging/Logging.h"

static std::string name = "KafkaPoller";

KafkaPoller::KafkaPoller(RdKafka::Producer *kafka_producer, std::shared_ptr<SignalChannel> sig_channel) : m_kafka_producer(kafka_producer), m_sig_channel(sig_channel)
{
}

bool KafkaPoller::start()
{
    m_t = std::make_unique<std::thread>(&KafkaPoller::run, this);
    Logging::INFO("Started", name);
    return true;
}

void KafkaPoller::join() const
{
    ThreadGuard g(*m_t);
}

void KafkaPoller::run()
{
    while (!m_sig_channel->m_shutdown_requested.load())
    {
        {
            std::unique_lock shutdown_lock(m_sig_channel->m_cv_mutex);
            m_sig_channel->m_cv.wait_for(shutdown_lock, std::chrono::milliseconds(10), [this]()
                                         { bool should_shutdown = m_sig_channel->m_shutdown_requested.load();
                                     return should_shutdown; });
        }

        m_kafka_producer->poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    }

    Logging::INFO("Shutting down", name);
}

KafkaPoller::~KafkaPoller()
{
}