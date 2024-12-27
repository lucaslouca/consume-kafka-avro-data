#ifndef KAFKA_CONSUMER_CALLBACK_H
#define KAFKA_CONSUMER_CALLBACK_H

#include <librdkafka/rdkafkacpp.h>

#include <cstring>
#include <iostream>

#include "SchemaRegistry.h"
class KafkaConsumerCallback : public RdKafka::ConsumeCb {
   public:
    KafkaConsumerCallback();
    void consume_cb(RdKafka::Message &msg, void *opaque) override;
    bool consume_message(RdKafka::Message *message);
    ~KafkaConsumerCallback();

   private:
    const std::string m_name = "KafkaConsumerCallback";
    Serdes::Schema *m_schema;
    int avro2json(Serdes::Schema *schema, const avro::GenericDatum *datum, std::string &str, std::string &errstr);
    size_t deserialize(RdKafka::Message *message);
};

#endif