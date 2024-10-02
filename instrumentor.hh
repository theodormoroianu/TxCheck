#ifndef INSTRUMENTOR_HH
#define INSTRUMENTOR_HH

#include <ostream>
#include "relmodel.hh"
#include <memory>
#include "schema.hh"

#include "prod.hh"
#include "expr.hh"
#include "grammar.hh"
#include "relmodel.hh"

#include <vector>
#include <map>

using namespace std;

// item-dependency related: BEFORE_WRITE_READ, AFTER_WRITE_READ
// predicate-dependency related: VERSION_SET_READ, BEFORE_OVERWRITE_JUDGE, AFTER_OVERWRITE_JUDGE
enum stmt_basic_type
{
    INIT_TYPE, // replaced str or did not figure out yet
    SELECT_READ,
    UPDATE_WRITE,
    INSERT_WRITE,
    DELETE_WRITE,
    BEFORE_WRITE_READ,
    AFTER_WRITE_READ,
    VERSION_SET_READ,
    BEFORE_PREDICATE_MATCH,
    AFTER_PREDICATE_MATCH,
    PREDICATE_MATCH
};

/**
 * Stores the information of a statement: its type (SELECT / UPDATE / ... / BWR / VSR / AWR),
 * the table it operates on, and whether it is instrumented.
 */
struct stmt_usage
{
    stmt_basic_type stmt_type;
    string target_table; // not used for SELECT_READ which may read row from multiple tables
    bool is_instrumented;

    stmt_usage(const stmt_basic_type &target_st, bool instrument)
    {
        stmt_type = target_st;
        is_instrumented = instrument;
    }
    stmt_usage(const stmt_basic_type &target_st, bool instrument, string target_t)
    {
        stmt_type = target_st;
        target_table = target_t;
        is_instrumented = instrument;
    }

    bool operator==(const stmt_basic_type &target_st) const
    {
        return stmt_type == target_st;
    }
    bool operator==(const stmt_usage &target_s) const
    {
        return stmt_type == target_s.stmt_type && target_table == target_s.target_table;
    }
    bool operator!=(const stmt_basic_type &target_st) const
    {
        return stmt_type != target_st;
    }
    bool operator!=(const stmt_usage &target_s) const
    {
        return stmt_type != target_s.stmt_type || target_table != target_s.target_table;
    }
    void operator=(const stmt_basic_type &target_st)
    {
        stmt_type = target_st;
    }
    friend ostream &operator<<(ostream &output, const stmt_usage &su)
    {
        output << su.stmt_type;
        return output;
    }
};

struct instrumentor
{
    instrumentor(vector<shared_ptr<prod>> &stmt_queue,
                 vector<int> &tid_queue,
                 shared_ptr<schema> db_schema);

    // Used for generating statements.
    scope used_scope;
    shared_ptr<schema> db_schema;

    // The final list of statements to be executed, after instrumenting
    vector<shared_ptr<prod>>
        final_stmt_queue;

    // The final list of transaction IDs, after instrumenting. It represents the tid of each statement in final_stmt_queue.
    vector<int> final_tid_queue;

    // The final list of statement usages, after instrumenting. It represents the type (SELECT / UPDATE / ... / BWR / VSR / AWR) of each statement in final_stmt_queue.
    vector<stmt_usage> final_stmt_usage;

    // Stores the predicates extracted from the statements.
    // The key is the statement index, and the value is the predicate.
    map<int, shared_ptr<query_spec>> predicates;

    // Gives additional information on the BEFORE and AFTER PREDICATE MATCH statements.
    // It maps predicate reads, BEFORE_PREDICATE_MATCH, and AFTER_PREDICATE_MATCH to the same predicate ID from the `predicates` map.
    std::map<int, int> stmt_id_to_predicate_id;

    // Populates the `predicates` map with the predicates extracted from the statements.
    void ExtractPredicates(vector<shared_ptr<prod>> &stmt_queue);

    // Adds the instrumentation from an update statement.
    void HandleUpdateStmt(shared_ptr<update_stmt> update_statement, int tid, int stmt_idx);

    // Adds the instrumentation from a delete statement.
    void HandleDeleteStmt(shared_ptr<delete_stmt> delete_statement, int tid, int stmt_idx);

    // Adds the instrumentation from an insert statement.
    void HandleInsertStmt(shared_ptr<insert_stmt> stmt, int tid, int stmt_idx);

    // Adds the instrumentation from a select statement.
    void HandleSelectStmt(shared_ptr<query_spec> stmt, int tid, int stmt_idx);
};

#endif