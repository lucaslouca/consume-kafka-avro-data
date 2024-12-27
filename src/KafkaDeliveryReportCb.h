
/**
 * Kafka delivery report callback used to signal back to the application when a message
 * has been delivered (or failed permanently after retries).
 *
 **/
#ifndef KAFKA_DELIVERY_REPORT_CB_H
#define KAFKA_DELIVERY_REPORT_CB_H

#include <librdkafka/rdkafkacpp.h>

class KafkaDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message);
};

#endif