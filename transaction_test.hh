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

#define SHOW_CHARACTERS 100
#define SPACE_HOLDER_STMT "select 1 from (select 1) as subq_0 where 0 <> 0"

struct transaction
{
    shared_ptr<dut_base> dut;
    bool is_blocked;

    vector<shared_ptr<prod>> stmts;
    vector<stmt_output> stmt_outputs;
    vector<string> stmt_err_info;

    vector<shared_ptr<prod>> normal_stmts;
    vector<stmt_output> normal_outputs;
    vector<string> normal_err_info;

    transaction()
    {
        is_blocked = false;
        stmt_num = 0;
        status = NOT_DEFINED;
    }

    int stmt_num;
    // Speficies if this specific transaction aborts or commits.
    txn_status status;
};

class transaction_test
{
public:
    static int record_bug_num;
    static pid_t server_process_id;
    static bool try_to_kill_server();

    transaction *trans_arr;
    string output_path_dir;

    dbms_info test_dbms_info;
    int commit_num;

    int trans_num;
    int stmt_num;

    shared_ptr<schema> db_schema;

    // TID of the transactions in the order in which we execute them.
    // e.g. { 1, 2, 0, 1 } -> stmt from 1, stmt from 2, stmt from 0, stmt from 1
    vector<int> tid_queue;
    // The statements that we execute in the order in which we execute them.
    // Same order as `tid_queue`.
    vector<shared_ptr<prod>> stmt_queue;
    // The usage of the statements in the order in which we execute them.
    // same as `tid_queue` and `stmt_queue`.
    vector<stmt_usage> stmt_use;
    // Initial database content, indexed by table name.
    map<string, vector<vector<string>>> init_db_content;

    vector<int> real_tid_queue;
    vector<shared_ptr<prod>> real_stmt_queue;
    vector<stmt_output> real_output_queue;
    vector<stmt_usage> real_stmt_usage;
    map<string, vector<vector<string>>> trans_db_content;

    // normal stmt test related
    vector<stmt_output> normal_stmt_output;
    vector<string> normal_stmt_err_info;
    map<string, vector<vector<string>>> normal_stmt_db_content;

    // original stmt test case
    vector<int> original_tid_queue;
    vector<shared_ptr<prod>> original_stmt_queue;
    vector<stmt_usage> original_stmt_use;

    /**
     * Populates the `tid_queue` with transaction IDs, which is the order
     * in which transaction statements should be executed.
     * i.e. for each `x`, a new statement from transaction `x` is executed.
     */
    void assign_txn_id();

    // Set which transactions abort and which commit.
    void assign_txn_status();

    /**
     * Generates the statements for each transaction, with the init / abort / commit statements.
     * It also populates the `stmt_queue` and `stmt_use` vectors.
     */
    void gen_txn_stmts();

    // instrument, and also align the trans_arr[tid] related data
    void instrument_txn_stmts();

    // Deletes all instrumentation statements, while preserving the orignal
    // statements, transaction IDs, and statement usage.
    void clean_instrument();

    // Deletes the statements that are not executed together with their instrumentation.
    void remove_separated_blocks();

    // change stmt_queue, stmt_use, and tid_queue but not change trans[tid] related data.
    // While the statement order is different when ran, replace the order with the new one. Statements not ran are replaced with a placeholder.
    void block_scheduling();

    // Change the status of a transaction.
    // Returns true if the status was updated, false if not needed.
    bool change_txn_status(int tid, txn_status final_status);

    // input da is empty; output the analyzed da
    bool analyze_txn_dependency(shared_ptr<dependency_analyzer> &da);

    // reset trans_arr[tid] related data, clear real_*, clear normal_*, clear init_db_content.
    void clear_execution_status();

    /**
     * Runs a test on the transaction.
     * The trasactions, transaction statements, and the database content are set before calling this function.
     *
     * @return true if the test fails, false if the test passes.
     */
    bool multi_stmt_round_test();

    bool refine_stmt_queue(vector<stmt_id> &stmt_path, shared_ptr<dependency_analyzer> &da);
    void normal_stmt_test(vector<stmt_id> &stmt_path);
    bool check_normal_stmt_result(vector<stmt_id> &stmt_path, bool debug = false);

    void trans_test(bool debug_mode = true);
    void retry_block_stmt(int cur_stmt_num, int *status_queue, bool debug_mode = true);
    int trans_test_unit(int stmt_pos, stmt_output &output, bool debug_mode = true);

    static bool fork_if_server_closed(dbms_info &d_info);

    transaction_test(dbms_info &d_info);
    ~transaction_test();

    int test();

private:
    void save_test_case(string dir_name,
                        string prefix,
                        vector<shared_ptr<prod>> &tar_stmt_queue,
                        vector<int> &tar_tid_queue,
                        vector<stmt_usage> &tar_usage_queue);
};

void print_stmt_path(vector<stmt_id> &stmt_path, map<pair<stmt_id, stmt_id>, set<dependency_type>> &stmt_graph);

#endif