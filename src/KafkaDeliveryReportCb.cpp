#include "KafkaDeliveryReportCb.h"
#include "logging/Logging.h"

static std::string name = "KafkaDeliveryReportCb";

void KafkaDeliveryReportCb::dr_cb(RdKafka::Message &message)
{
    if (message.err())
    {
        Logging::ERROR("Message delivery failed: " + message.errstr(), name);
    }
    else
    {
        Logging::INFO("Message delivered to topic " + message.topic_name() + " [" + std::to_string(message.partition()) + "] at offset " + std::to_string(message.offset()), name);
    }
}