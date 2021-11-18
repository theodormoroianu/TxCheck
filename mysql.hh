/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef MYSQL_HH
#define MYSQL_HH

extern "C"  {
#include <mysql/mysql.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

struct mysql_connection {
    MYSQL mysql;
    string test_db;
    unsigned int test_port;
    mysql_connection(string db, unsigned int port);
    ~mysql_connection();
};

struct schema_mysql : schema, mysql_connection {
    schema_mysql(string db, unsigned int port);
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_mysql : dut_base, mysql_connection {
    virtual void test(const std::string &stmt, std::vector<std::string>* output = NULL);
    virtual void reset(void);
    virtual void backup(void);
    virtual void trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output = NULL
                          , int commit_or_not = 1);
    virtual void reset_to_backup(void);
    virtual void get_content(vector<string>& tables_name, map<string, vector<string>>& content);
    dut_mysql(string db, unsigned int port);
};

#endif