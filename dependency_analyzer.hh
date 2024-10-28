#ifndef DEPENDENCY_ANALYZER_HH
#define DEPENDENCY_ANALYZER_HH

#include <ostream>
#include "relmodel.hh"
#include <memory>
#include "schema.hh"

#include "prod.hh"
#include "expr.hh"
#include "grammar.hh"

#include "instrumentor.hh"
#include <vector>
#include <set>
#include <algorithm>

using namespace std;

// START_DEPEND: the begin is count by first read or write
// STRICT_START_DEPEND: the begin is count by begin statement
enum dependency_type
{
    // a writes a value, b reads it
    WRITE_READ,
    // a writes a value, b overwrites it
    WRITE_WRITE,
    // a reads a value, b overwrites it
    READ_WRITE,
    // a commits before b starts
    START_DEPEND,
    // a commits before b runs a statement (different than start transaction)
    STRICT_START_DEPEND,
    // ordering imposed by the instrumentation
    INSTRUMENT_DEPEND,
    VERSION_SET_DEPEND,
    OVERWRITE_DEPEND,
    // ordering within a transaction
    INNER_DEPEND
}; // for predicate

// a row consists of several field(string) -> vector <string>
typedef vector<string> row_output;
// one output consits of several rows -> vector <vector <string>>
typedef vector<row_output> stmt_output;

/**
 * Seems to be an entry to the history vector??
 */
struct operate_unit
{
    stmt_usage stmt_u;
    // Version of the row.
    int write_op_id;
    int tid;
    int stmt_idx;
    int row_id;
    size_t hash;
    operate_unit(
        stmt_usage use,
        int op_id,
        int tid,
        int stmt_idx,
        int row_id,
        size_t hash) : stmt_u(use), write_op_id(op_id),
                       tid(tid), stmt_idx(stmt_idx),
                       row_id(row_id), hash(hash) {}
};

/**
 * Stores the updates of a specific row of the database (identified with the rowid).
 */
struct row_change_history
{
    int row_id;
    vector<operate_unit> row_op_list;
};

/**
 * Stores the history of the updates done to rows of the database.
 */
struct history
{
    vector<row_change_history> change_history;
    void insert_to_history(operate_unit &oper_unit);
};

struct stmt_id
{
    int txn_id;
    int stmt_idx_in_txn;
    bool operator==(const stmt_id &other_id) const
    {
        return this->txn_id == other_id.txn_id &&
               this->stmt_idx_in_txn == other_id.stmt_idx_in_txn;
    }

    bool operator<(const stmt_id &other_id) const
    {
        if (this->txn_id == other_id.txn_id)
            return this->stmt_idx_in_txn < other_id.stmt_idx_in_txn;
        else
            return this->txn_id < other_id.txn_id;
    }

    stmt_id(vector<int> &final_tid_queue, int stmt_idx);
    stmt_id()
    {
        txn_id = -1;
        stmt_idx_in_txn = -1;
    }
    stmt_id(int tid, int stmt_pos)
    {
        txn_id = tid;
        stmt_idx_in_txn = stmt_pos;
    }
    int transfer_2_stmt_idx(vector<int> &final_tid_queue);
};

struct dependency_analyzer
{
    dependency_analyzer(vector<stmt_output> &init_output,
                        vector<stmt_output> &total_output,
                        vector<int> &final_tid_queue,
                        vector<stmt_usage> &final_stmt_usage,
                        vector<txn_status> &final_txn_status,
                        int t_num,
                        int primary_key_idx,
                        int write_op_key_idx);
    ~dependency_analyzer();

    size_t hash_output(row_output &row);

    // Creates
    void build_predicate_dependency(vector<operate_unit> &op_list, int predicate_idx);

    /**
     * Finds who installed a specific value and adds the WR dependency.
     *
     * @param op_list The list of values of the row.
     * @param op_idx The index of the read operation in the row list.
     */
    void build_directly_read_dependency(vector<operate_unit> &op_list, int op_idx);

    /**
     * Finds who read an overwriten value and adds the RW dependency.
     *
     * @param op_list The list of values of the row.
     * @param op_idx The index of the overwite operation in the row list.
     */
    void build_directly_item_anti_dependency(vector<operate_unit> &op_list, int op_idx);

    /**
     * Finds who installed a specific value and adds the WW dependency.
     *
     * @param op_list The list of values of the row.
     * @param op_idx The index of the write operation in the row list.
     */
    void build_directly_write_dependency(vector<operate_unit> &op_list, int op_idx);

    // for predicate
    void build_VS_dependency();
    void build_OW_dependency();

    /**
     * Returns true if the statement `stmt_idx` is overwriten by
     * the statement `overwrite_stmt`.
     *
     * @param stmt_idx The index of the statement to check. Must contain a PREDICATE_MATCH.
     * @param overwrite_stmt The index of the statement that overwrites the other. Must be an AWR.
     * @param bpm The index of the statement that contains the before predicate match of `stmt_idx` for `overwrite_stmt`.
     * @param apm The index of the statement that contains the after predicate match of `stmt_idx` for `overwrite_stmt`.
     */
    bool check_if_stmt_is_overwriten(int predicate_match, int overwrite_awr_stmt, int bpm_stmt, int apm_stmt);

    /**
     * Returns true if the version `v1` is higher than the version `v2`.
     *
     * @param row_id The id of the row.
     * @param v1 The first version to compare.
     * @param v2 The second version to compare.
     */
    bool check_which_version_is_higher(int row_id, int v1, int v2);

    /**
     * Reads the output of a statement into the primary key and version key.
     *
     * @param stmt_idx The index of the statement to read.
     * @param pk_version_pair The set to store the primary key and version key.
     * @param primary_key_set The set to store the primary keys.
     */
    void read_stmt_output_into_pk_and_version(int stmt_idx, set<pair<int, int>> &pk_version_pair, set<int> &primary_key_set);

    void build_stmt_inner_dependency();
    void build_start_dependency();
    void build_stmt_instrument_dependency();

    // Returns the set of instrumentation statements created for the statement at the given index.
    // Relies on the `stmt_dependency_graph` map to find the dependencies.
    set<int> get_instrumented_stmt_set(int queue_idx);

    void build_stmt_start_dependency(int prev_tid, int later_tid, dependency_type dt);

    void print_dependency_graph();

    // G1a: Aborted Reads. A history H exhibits phenomenon G1a if it contains an aborted
    // transaction Ti and a committed transaction Tj such that Tj has read some object
    // (maybe via a predicate) modified by Ti.
    bool check_G1a();
    // G1b: Intermediate Reads. A history H exhibits phenomenon G1b if it contains a
    // committed transaction Tj that has read a version of object x (maybe via a predicate)
    // written by transaction Ti that was not Ti’s final modification of x.
    bool check_G1b();
    // G1c: Circular Information Flow. A history H exhibits phenomenon G1c if DSG(H)
    // contains a directed cycle consisting entirely of dependency edges.
    bool check_G1c();
    // G2-item: Item Anti-dependency Cycles. A history H exhibits phenomenon G2-item
    // if DSG(H) contains a directed cycle having one or more item-anti-dependency edges.
    bool check_G2_item();

    // Snapshot Isolation:
    // G-SIa: Interference. A history H exhibits phenomenon G-SIa if SSG(H) contains a
    // read/write-dependency edge from Ti to Tj without there also being a start-dependency
    // edge from Ti to Tj.
    bool check_GSIa();
    // G-SIb: Missed Effects. A history H exhibits phenomenon G-SIb if SSG(H) contains
    // a directed cycle with exactly one anti-dependency edge.
    bool check_GSIb();

    /**
     * Checks if the DSG contains a cycle.
     * It first converts the statement dependency graph to a DSG (it
     * creates a graph on transactions instead of statements), and
     * then checks if there is a cycle in the DSG.
     *
     * @return True if there is a cycle, false otherwise.
     */
    bool check_any_transaction_cycle();

    bool check_cycle(set<dependency_type> &edge_types);
    static bool reduce_graph_indegree(int **direct_graph, int length);
    static bool reduce_graph_outdegree(int **direct_graph, int length);

    history h;
    // Number of transactions (including the init transaction).
    int tid_num;
    int stmt_num;
    int *tid_begin_idx;        // idx of first non-start transaction
    int *tid_strict_begin_idx; // idx of start transaction
    int *tid_end_idx;

    // Index of the PK in the output of a statement.
    int primary_key_index;
    // Index if the version key in the output of a statement.
    int version_key_index;

    // Status of the transactions (aborted / committed / undefined).
    vector<txn_status> f_txn_status;
    // The id of the transactions in the order of the appearence of statements.
    vector<int> f_txn_id_queue;
    // Number of statements in each transaction.
    vector<int> f_txn_size;
    // Type of the executed statements.
    vector<stmt_usage> f_stmt_usage;
    // Output of the statements.
    vector<stmt_output> f_stmt_output;
    // Hash of the output of statements.
    map<int, row_output> hash_to_output;

    // dependency_graph[i][j] = set of dependencies of txn i over j
    set<dependency_type> **dependency_graph;
    void check_txn_graph_cycle(set<int> &cycle_nodes, vector<int> &sorted_nodes);

    // Dependencies between statements.
    map<pair<stmt_id, stmt_id>, set<dependency_type>> stmt_dependency_graph;
    void build_stmt_depend_from_stmt_idx(int stmt_idx1, int stmt_idx2, dependency_type dt);
    vector<stmt_id> longest_stmt_path(map<pair<stmt_id, stmt_id>, int> &stmt_dist_graph);
    vector<stmt_id> longest_stmt_path();
    vector<stmt_id> topological_sort_path(set<stmt_id> deleted_nodes, bool *delete_flag = NULL);

    vector<vector<stmt_id>> get_all_topo_sort_path();
    void recur_topo_sort(vector<stmt_id> current_path,
                         set<stmt_id> deleted_nodes,
                         vector<vector<stmt_id>> &total_path,
                         map<pair<stmt_id, stmt_id>, set<dependency_type>> &graph);
};

#endif