#ifndef DATABASE_H
#define DATABASE_H

#include <iostream>
#include <pqxx/pqxx>
#include <string>
class Database {
   public:
    ~Database();

    bool insert_object(const std::string &object_name, const std::string &object_type, const std::string &created_at);
    int get_object_id(const std::string &object_name);
    bool insert_relationship(const int source_id, const int target_id, const std::string &relationship_name);
    static Database &instance();
    static void init(const std::string &url);

   private:
    Database(const std::string *url);
    void prepare_insert(pqxx::connection &conn);
    static Database &instance_impl(const std::string *url);
    pqxx::connection m_conn;

    const std::string m_insert_object_stmt{
        "INSERT INTO objects(object_name, object_type, created_at) VALUES ($1, $2, $3::date) ON CONFLICT ON CONSTRAINT "
        "objects_unique_constraint DO NOTHING RETURNING id"};

    const std::string m_object_id_for_name_stmt{"SELECT id FROM objects WHERE object_name = $1"};

    const std::string m_insert_relationship_stmt{
        "INSERT INTO relationships(source_id, target_id, relationship_name) VALUES ($1, $2, $3) ON CONFLICT ON "
        "CONSTRAINT relationships_unique_constraint DO NOTHING"};
};
#endif