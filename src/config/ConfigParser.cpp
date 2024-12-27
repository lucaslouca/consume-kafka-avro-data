#include "ConfigParser.h"

#include <cpprest/http_client.h>

#include <avro/Compiler.hh>
#include <avro/Schema.hh>
#include <numeric>  // for accumulate()

#include "SchemaRegistry.h"
#include "logging/Logging.h"

static std::string name = "ConfigParser";

ConfigParser::ConfigParser(std::string config_file) : m_config_file(config_file) {
    m_config = YAML::LoadFile(m_config_file);
}

ConfigParser &ConfigParser::instance(std::string c) {
    static ConfigParser i(c);
    return i;
}

bool ConfigParser::has_key(const std::string &k) { return m_config[k].Type(); }

std::map<std::string, std::string> ConfigParser::config_for_key(const std::string &k) {
    if (has_key(k)) {
        YAML::Node node = m_config[k];
        if (node.Type() != YAML::NodeType::Map) {
            std::string err = std::string("Value for key '");
            err += k;
            err += "' is not a map";
            Logging::ERROR(err, name);
            kill(getpid(), SIGINT);
        } else {
            std::map<std::string, std::string> result;
            // We now have a map node, so let's iterate through:
            for (auto it = node.begin(); it != node.end(); ++it) {
                YAML::Node key = it->first;
                YAML::Node value = it->second;
                if (value.Type() == YAML::NodeType::Scalar) {
                    result.insert(std::make_pair(key.as<std::string>(), value.as<std::string>()));
                }
                if (value.Type() == YAML::NodeType::Map) {
                    // This should be true; do something here with the map.
                    std::string s = std::string("Nested map found for key '");
                    s += k;
                    s += "'";
                    Logging::INFO(s, name);
                }
            }
            return result;
        }
    } else {
        std::string err = std::string("No such key '");
        err += k;
        err += "'";
        Logging::ERROR(err, name);
        kill(getpid(), SIGINT);
    }
}

std::map<std::string, std::string> ConfigParser::column_map() { return config_for_key("column_map"); }

std::map<std::string, std::string> ConfigParser::column_type_transforms_map() {
    return config_for_key("column_type_transforms");
}

std::map<std::string, std::string> ConfigParser::kafka() { return config_for_key("kafka"); }

std::map<std::string, SchemaConfig> ConfigParser::schema_configs() {
    std::vector<std::string> err;
    std::map<std::string, SchemaConfig> result;

    std::map<std::string, std::string> kafka_config = kafka();
    SchemaRegistry::init(kafka_config["schema.registry.url"]);

    if (has_key("type_map")) {
        const auto &topic_map = m_config["type_map"];

        for (auto it = topic_map.begin(); it != topic_map.end(); ++it) {
            auto cm = column_map();
            auto ctt = column_type_transforms_map();
            std::string topic;
            std::string key_column;
            std::vector<std::string> columns;

            YAML::Node key = it->first;
            YAML::Node value = it->second;

            topic.assign(key.as<std::string>());

            if (value.Type() == YAML::NodeType::Map) {
                if (value["key_column"]) {
                    key_column.assign(value["key_column"].as<std::string>());
                } else {
                    err.emplace_back("Missing key_column for topic '" + topic + "'");
                }

                auto list = value["columns"];
                for (YAML::const_iterator it = list.begin(); it != list.end(); ++it) {
                    columns.emplace_back(it->as<std::string>());
                }

                if (columns.empty()) {
                    err.emplace_back("No columns found for topic '" + topic + "'");
                }
            } else {
                err.emplace_back("Value for key '" + topic + "' should be of type map!");
            }

            /*
            No need to check for error on current iteration befroe we insert, since we are going to check later for
            errors and throw in case of error. I don't want to return here (and avoid unnecessary parsing)
            since I want to collect all the errors first.
            */

            /*
            TODO: Improve: Move of some args would be better, but I don't want to move topic here as it is going to be
            emptied. and (compiler build) constructors are all-or-nothing
            */
            result.insert(std::make_pair(topic, SchemaConfig{topic, key_column, columns, cm, ctt}));
        }
    } else {
        err.emplace_back("No schema configuration found");
    }

    if (!err.empty()) {
        std::string errstr = std::accumulate(err.begin(), err.end(), std::string(),
                                             [](std::string running_str, const std::string &new_str) {
                                                 return running_str.empty() ? new_str : running_str + "\n" + new_str;
                                             });
        Logging::ERROR(errstr, name);
        kill(getpid(), SIGINT);
    } else {
        return result;
    }
}

avro::ValidSchema ConfigParser::assemble_schema(const SchemaConfig &config) {
    // https://avro.apache.org/docs/1.4.0/api/cpp/html/index.html

    avro::RecordSchema record(config.name + "_msg");
    for (const auto &field : config.columns) {
        std::string field_name = field;
        if (config.column_map.find(field) != config.column_map.end()) {
            field_name.assign(config.column_map.find(field)->second);
        }

        std::string type = "string";
        if (config.column_type_transforms.find(field) != config.column_type_transforms.end()) {
            type.assign(config.column_type_transforms.find(field)->second);
        }

        if (!type.compare("float")) {
            record.addField(field_name, avro::FloatSchema());
        } else if (!type.compare("double")) {
            record.addField(field_name, avro::DoubleSchema());
        } else if (!type.compare("int")) {
            record.addField(field_name, avro::IntSchema());
        } else if (!type.compare("long")) {
            record.addField(field_name, avro::LongSchema());
        } else {
            record.addField(field_name, avro::StringSchema());
        }
    }

    avro::ValidSchema schema(record);
    return schema;
}

avro::ValidSchema ConfigParser::load_schema(const std::string file) {
    std::ifstream is(file);
    avro::ValidSchema schema;
    avro::compileJsonSchema(is, schema);
    return schema;
}

int32_t ConfigParser::fetch_schema_id_rest(const std::string &name, const std::string &registry) {
    std::stringstream ss;
    ss << registry << "/subjects/" << name << "-value/versions/latest";
    web::uri uri(ss.str());
    web::http::client::http_client client(uri);
    web::http::http_request req;

    req.set_method(web::http::methods::GET);
    pplx::task<web::json::value> request_task = client.request(req).then([&](web::http::http_response response) {
        web::json::value json_obj;
        try {
            if (response.status_code() == web::http::status_codes::OK) {
                response.headers().set_content_type("application/json");  // Set headers to receive data as JSON
                json_obj = response.extract_json().get();
            }
        } catch (const web::http::http_exception &e) {
            Logging::ERROR("HTTP exception while requesting '" + ss.str() + "': " + e.error_code().message(), name);
        }
        return json_obj;
    });
    int32_t id = -1;
    try {
        web::json::object data = request_task.get().as_object();
        id = data.at("id").as_integer();
    } catch (const web::json::json_exception &e) {
        Logging::ERROR("JSON exception while requesting '" + ss.str() + "': " + e.what(), name);
        kill(getpid(), SIGINT);
    } catch (const web::http::http_exception &e) {
        Logging::ERROR("HTTP exception while requesting '" + ss.str() + "': " + e.error_code().message(), name);
        kill(getpid(), SIGINT);
    }
    return id;
}

int32_t ConfigParser::fetch_schema_id(const std::string &name) {
    return SchemaRegistry::instance().fetch_value_schema_id(name);
}

std::map<std::string, SchemaConfig> ConfigParser::schemas() {
    std::map<std::string, SchemaConfig> schemas = schema_configs();

    for (auto &[topic, schema_config] : schemas) {
        int schema_id = fetch_schema_id(topic);
        schema_config.schema_id = schema_id;
        schema_config.schema = assemble_schema(schema_config);
        Logging::DEBUG("Created schema\n" + schema_config.schema.toJson() + "\n for topic '" + topic + "'", name);
    }

    return schemas;
}

ConfigParser::~ConfigParser() {};