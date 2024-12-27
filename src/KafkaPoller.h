/**
 * A Kafka producer application should continually serve the delivery report queue by calling poll()
 * at frequent intervals.
 *
 * This starts a dedicated poll thread to make sure that poll() is still called during periods
 * where we are not producing any messages to make sure previously produced messages have their
 * delivery report callback served (and any other callbacks we register).
 **/
#ifndef KAFKA_POLLER_H
#define KAFKA_POLLER_H
#include "SignalChannel.h"
#include <thread>
#include <memory>
#include <librdkafka/rdkafkacpp.h>

class KafkaPoller
{
public:
    KafkaPoller(RdKafka::Producer *kafka_producer_, std::shared_ptr<SignalChannel> sig_channel_);
    bool start();
    void join() const;
    ~KafkaPoller();

private:
    RdKafka::Producer *m_kafka_producer;
    std::unique_ptr<std::thread> m_t;
    std::shared_ptr<SignalChannel> m_sig_channel;
    void run();
};

#endif
