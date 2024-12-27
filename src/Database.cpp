#include "Database.h"

#include <iostream>

Database::Database(const std::string *url) : m_conn(*url) { prepare_insert(m_conn); }

Database &Database::instance_impl(const std::string *url = nullptr) {
    static Database i{url};

    return i;
}

void Database::init(const std::string &url) { instance_impl(&url); }

Database &Database::instance() {
    Database &i = instance_impl();
    return i;
}

void Database::prepare_insert(pqxx::connection &conn) {
    conn.prepare("insert_object", m_insert_object_stmt);
    conn.prepare("insert_relationship", m_insert_relationship_stmt);
    conn.prepare("select_object_id", m_object_id_for_name_stmt);
}

int Database::get_object_id(const std::string &object_name) {
    if (m_conn.is_open()) {
        pqxx::work transaction{m_conn};
        pqxx::result r = transaction.exec_prepared("select_object_id", object_name);
        transaction.commit();
        for (auto const &row : r) {
            const pqxx::field field = row[0];
            return field.as<int>();
        }
    } else {
        std::cout << "Database connection is not open!" << std::endl;
    }
    return 0;
}

bool Database::insert_object(const std::string &object_name, const std::string &object_type,
                             const std::string &created_at) {
    if (m_conn.is_open()) {
        pqxx::work transaction{m_conn};
        pqxx::result r;
        for (int i = 0; i < 2; i++) {
            r = transaction.exec_prepared("insert_object", object_name, object_type, created_at);
        }
        transaction.commit();

        // for (auto const &row : r) {
        //     const pqxx::field field = row[0];
        //     return field.as<int>();
        // }

    } else {
        std::cout << "Database connection is not open!" << std::endl;
        return false;
    }

    return true;
}

bool Database::insert_relationship(const int source_id, const int target_id, const std::string &relationship_name) {
    if (m_conn.is_open()) {
        pqxx::work transaction{m_conn};
        pqxx::result r;
        for (int i = 0; i < 2; i++) {
            r = transaction.exec_prepared("insert_relationship", source_id, target_id, relationship_name);
        }
        transaction.commit();
    } else {
        std::cout << "Database connection is not open!" << std::endl;
        return false;
    }

    return true;
}
Database::~Database() { m_conn.close(); }
