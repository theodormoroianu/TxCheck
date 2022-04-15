#ifndef TRANSACTION_TEST_HH
#define TRANSACTION_TEST_HH

#include "config.h"

#include "dbms_info.hh"
#include "general_process.hh"
#include "instrumentor.hh"
#include "dependency_analyzer.hh"

#include <sys/time.h>
#include <sys/wait.h>

using namespace std;

struct transaction {
    shared_ptr<dut_base> dut;
    bool is_blocked;
    
    vector<shared_ptr<prod>> stmts;
    vector<stmt_output> stmt_outputs;
    vector<string> stmt_err_info;

    vector<shared_ptr<prod>> normal_stmts;
    vector<stmt_output> normal_outputs;
    vector<string> normal_err_info;

    transaction() {is_blocked = false; stmt_num = 0; status = NOT_DEFINED;}

    int stmt_num;
    txn_status status;
};

class transaction_test {
public:
    static int record_bug_num;
    static pid_t server_process_id;
    static bool try_to_kill_server();

    transaction* trans_arr;
    string output_path_dir;

    dbms_info test_dbms_info;
    int commit_num;

    int trans_num;
    int stmt_num;

    shared_ptr<schema> db_schema;

    vector<int> tid_queue;
    vector<shared_ptr<prod>> stmt_queue;
    vector<stmt_usage> stmt_use;
    map<string, vector<vector<string>>> init_db_content;

    vector<int> real_tid_queue;
    vector<shared_ptr<prod>> real_stmt_queue;
    vector<stmt_output> real_output_queue;
    vector<stmt_usage> real_stmt_usage;
    map<string, vector<vector<string>>> trans_db_content;

    vector<int> longest_seq_txn_order;

    map<string, vector<vector<string>>> normal_db_content;

    void assign_txn_id();
    void assign_txn_status();
    void gen_txn_stmts();
    void instrument_txn_stmts();

    bool analyze_txn_dependency();
    bool refine_txn_as_txn_order();
    
    bool check_commit_trans_blocked();
    void trans_test();
    void retry_block_stmt(int cur_stmt_num, shared_ptr<int[]> status_queue);
    int trans_test_unit(int stmt_pos, stmt_output& output);

    bool check_txn_normal_result();

    bool fork_if_server_closed();

    void normal_test();

    transaction_test(dbms_info& d_info);
    ~transaction_test();
    
    int test();

private:
    bool check_one_order_result(int order_index);
    void save_test_case(string dir_name);
};

#endif