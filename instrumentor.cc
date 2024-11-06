#include "instrumentor.hh"

const bool ADD_PREDICATE_INSTRUMENTATION = true;

bool stmt_basic_type_is_instrumentation(stmt_basic_type st)
{
    switch (st)
    {
    case BEFORE_PREDICATE_MATCH:
    case AFTER_PREDICATE_MATCH:
    case PREDICATE_MATCH:
    case VERSION_SET_READ:
    case BEFORE_WRITE_READ:
    case AFTER_WRITE_READ:
    case OLD_INSTRUMENTATION_BEFORE:
    case OLD_INSTRUMENTATION_AFTER:
        return true;

    case DELETE_WRITE:
    case INSERT_WRITE:
    case UPDATE_WRITE:
    case SELECT_READ:
    case INIT_TYPE:
        return false;
    default:
        throw runtime_error("Unknown stmt_basic_type");
    }
}

bool instrumentation_is_before(stmt_basic_type st)
{
    switch (st)
    {
    case BEFORE_PREDICATE_MATCH:
    case BEFORE_WRITE_READ:
    case PREDICATE_MATCH:
    case VERSION_SET_READ:
    case OLD_INSTRUMENTATION_BEFORE:
        return true;
    case AFTER_PREDICATE_MATCH:
    case AFTER_WRITE_READ:
    case OLD_INSTRUMENTATION_AFTER:
        return false;
    default:
        throw runtime_error("Unknown stmt_basic_type");
    }
}

stmt_basic_type transform_to_deleted_stmt(stmt_basic_type st)
{
    assert(st != OLD_INSTRUMENTATION_AFTER && st != OLD_INSTRUMENTATION_BEFORE);

    if (stmt_basic_type_is_instrumentation(st))
    {
        if (instrumentation_is_before(st))
            return OLD_INSTRUMENTATION_BEFORE;
        else
            return OLD_INSTRUMENTATION_AFTER;
    }
    else
        return INIT_TYPE;
}

string stmt_basic_type_to_string(stmt_basic_type st)
{
    switch (st)
    {
    case INIT_TYPE:
        return "INIT_TYPE";
    case SELECT_READ:
        return "SELECT_READ";
    case UPDATE_WRITE:
        return "UPDATE_WRITE";
    case INSERT_WRITE:
        return "INSERT_WRITE";
    case DELETE_WRITE:
        return "DELETE_WRITE";
    case BEFORE_WRITE_READ:
        return "BEFORE_WRITE_READ";
    case AFTER_WRITE_READ:
        return "AFTER_WRITE_READ";
    case VERSION_SET_READ:
        return "VERSION_SET_READ";
    case BEFORE_PREDICATE_MATCH:
        return "BEFORE_PREDICATE_MATCH";
    case AFTER_PREDICATE_MATCH:
        return "AFTER_PREDICATE_MATCH";
    case PREDICATE_MATCH:
        return "PREDICATE_MATCH";
    case OLD_INSTRUMENTATION_BEFORE:
        return "OLD_INSTRUMENTATION_BEFORE";
    case OLD_INSTRUMENTATION_AFTER:
        return "OLD_INSTRUMENTATION_AFTER";
    default:
        return "UNKNOWN";
    }
}

string print_stmt_to_string(shared_ptr<prod> stmt);

set<string> extract_words_begin_with(const string str, const string begin_str)
{
    set<string> words;
    auto pos = str.find(begin_str, 0);
    while (pos != string::npos)
    {
        if (pos >= 1 &&
            str[pos - 1] != '(' &&
            str[pos - 1] != ' ' &&
            str[pos - 1] != '\n')
        { // not begin
            pos = str.find(begin_str, pos + 1);
            continue;
        }

        // find the interval
        auto interval_pos = pos + 1;
        while (interval_pos < str.size() &&
               str[interval_pos] != ')' &&
               str[interval_pos] != '.' &&
               str[interval_pos] != ' ' &&
               str[interval_pos] != '\n' &&
               str[interval_pos] != ';') // not end
            interval_pos++;

        auto word = str.substr(pos, interval_pos - pos);
        words.insert(word);

        if (interval_pos == str.size()) // the last one
            return words;

        pos = str.find(begin_str, interval_pos + 1);
    }
    return words;
}

/**
 * Creates a simple SELECT * FROM ??? WHERE ??? statement.
 */
shared_ptr<query_spec> CreateQueryFromTableAndSearch(
    scope &scope,
    table *table,
    shared_ptr<bool_expr> search)
{
    scope.new_stmt();
    return make_shared<query_spec>((struct prod *)0, &scope, table, search);
}

/**
 * Extract all predicates from the statements.
 */
void instrumentor::ExtractPredicates(vector<shared_ptr<prod>> &stmt_queue)
{
    for (int i = 0; i < (int)stmt_queue.size(); i++)
    {
        if (auto update_s = dynamic_pointer_cast<update_stmt>(stmt_queue[i]))
            predicates[i] = CreateQueryFromTableAndSearch(
                used_scope,
                update_s->victim,
                update_s->search);
        else if (auto insert_s = dynamic_pointer_cast<insert_stmt>(stmt_queue[i]))
            ;
        else if (auto query_s = dynamic_pointer_cast<query_spec>(stmt_queue[i]))
            predicates[i] = query_s;
        else if (auto delete_s = dynamic_pointer_cast<delete_stmt>(stmt_queue[i]))
            predicates[i] = CreateQueryFromTableAndSearch(
                used_scope,
                delete_s->victim,
                delete_s->search);
        else if (auto str_s = dynamic_pointer_cast<txn_string_stmt>(stmt_queue[i]))
        {
            // begin, commit, abort, SELECT 1 WHERE 0 <> 0, but should not include SELECT * FROM t
            if (print_stmt_to_string(str_s).find("SELECT * FROM") != string::npos)
                throw runtime_error("Unexpected SELECT * FROM in txn_string_stmt");
        }
        else
            throw runtime_error("Unknown statement type!");
    }
}

void instrumentor::HandleUpdateStmt(shared_ptr<update_stmt> update_statement, int tid, int __attribute__((unused)) stmt_idx)
{
    used_scope.new_stmt(); // for before_write_select_stmt
    auto before_write_select_stmt = make_shared<query_spec>(
        (struct prod *)0, &used_scope,
        update_statement->victim, update_statement->search);

    used_scope.new_stmt(); // for after_write_select_stmt
    // get wkey idex
    auto table = update_statement->victim;
    int wkey_idx = -1;
    auto &columns = table->columns();
    int t_size = columns.size();
    for (int i = 0; i < t_size; i++)
    {
        if (columns[i].name == "wkey")
        {
            wkey_idx = i;
            break;
        }
    }
    if (wkey_idx == -1)
    {
        cerr << "problem stmt:\n"
             << print_stmt_to_string(update_statement) << endl;
        throw runtime_error("intrument update statement: cannot find wkey");
    }

    // get wkey value
    int wkey_set_idx = -1;
    auto names_size = update_statement->set_list->names.size();
    for (int i = 0; i < names_size; i++)
    {
        if (update_statement->set_list->names[i] == "wkey")
        {
            wkey_set_idx = i;
            break;
        }
    }
    if (wkey_set_idx == -1)
        throw runtime_error("intrument update statement: cannot find wkey = expr");

    auto wkey_value = update_statement->set_list->value_exprs[wkey_set_idx];

    // init compare op
    op *equal_op = NULL;
    for (auto &op : db_schema->operators)
    {
        if (op.name == "=")
        {
            equal_op = &op;
            break;
        }
    }
    if (equal_op == NULL)
        throw runtime_error("intrument update statement: cannot find = operator");

    // init column reference
    auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table->name);
    // init the select
    auto after_write_select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, table, equal_op, wkey_column, wkey_value);
    auto involved_tables = extract_words_begin_with(print_stmt_to_string(update_statement), "t_");

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // BPM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(BEFORE_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }

        // Predicate match
        if (predicates.contains(stmt_idx))
        {
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(predicates[stmt_idx]);
            final_stmt_usage.push_back(stmt_usage(PREDICATE_MATCH, true));
        }
    }

    // VSR
    /*---- version_set select (select * from t where 1=1) ---*/
    for (auto &table_str : involved_tables)
    {
        auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
        final_tid_queue.push_back(tid); // version_set select, build predicate-WW
        final_stmt_queue.push_back(version_set_select_stmt);
        final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
    }

    auto target_table_str = table->ident();

    // BWR
    final_tid_queue.push_back(tid);
    final_stmt_queue.push_back(before_write_select_stmt);
    final_stmt_usage.push_back(stmt_usage(BEFORE_WRITE_READ, true, target_table_str));

    // update
    final_tid_queue.push_back(tid);
    final_stmt_queue.push_back(update_statement);
    final_stmt_usage.push_back(stmt_usage(UPDATE_WRITE, false, target_table_str));

    // AWR
    final_tid_queue.push_back(tid); // get the changed value for later WW and WR dependency
    final_stmt_queue.push_back(after_write_select_stmt);
    final_stmt_usage.push_back(stmt_usage(AFTER_WRITE_READ, true, target_table_str));

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // APM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(AFTER_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }
    }
}

void instrumentor::HandleDeleteStmt(shared_ptr<delete_stmt> delete_statement, int tid, int __attribute__((unused)) stmt_idx)
{
    used_scope.new_stmt(); // for select_stmt
    auto select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope,
                                               delete_statement->victim, delete_statement->search);

    /*---- version_set select (select * from t where 1=1) ---*/
    auto involved_tables = extract_words_begin_with(print_stmt_to_string(delete_statement), "t_");
    auto target_table_str = delete_statement->victim->ident();

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // BPM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(BEFORE_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }

        // Predicate match
        if (predicates.contains(stmt_idx))
        {
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(predicates[stmt_idx]);
            final_stmt_usage.push_back(stmt_usage(PREDICATE_MATCH, true));
        }
    }

    // VSR
    for (auto &table_str : involved_tables)
    {
        auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
        final_tid_queue.push_back(tid); // version_set select, build predicate-WW
        final_stmt_queue.push_back(version_set_select_stmt);
        final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
    }

    // BWR
    final_tid_queue.push_back(tid); // get the ealier value to build RW and WW dependency
    final_stmt_queue.push_back(select_stmt);
    final_stmt_usage.push_back(stmt_usage(BEFORE_WRITE_READ, true, target_table_str));

    // Delete
    // item is deleted, so no need for later dependency
    final_tid_queue.push_back(tid);
    final_stmt_queue.push_back(delete_statement);
    final_stmt_usage.push_back(stmt_usage(DELETE_WRITE, false, target_table_str));

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // APM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(AFTER_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }
    }
}

void instrumentor::HandleInsertStmt(shared_ptr<insert_stmt> insert_statement, int tid, int __attribute__((unused)) stmt_idx)
{
    used_scope.new_stmt(); // for select_stmt
    // get wkey idex
    auto table = insert_statement->victim;
    int wkey_idx = -1;
    auto &columns = table->columns();
    int t_size = columns.size();
    for (int i = 0; i < t_size; i++)
    {
        if (columns[i].name == "wkey")
        {
            wkey_idx = i;
            break;
        }
    }
    if (wkey_idx == -1)
    {
        cerr << "problem stmt:\n"
             << print_stmt_to_string(insert_statement) << endl;
        throw runtime_error("intrument insert statement: cannot find wkey");
    }

    // get wkey value
    auto &items = insert_statement->value_exprs_vector.front();
    auto wkey_value = items[wkey_idx];

    // init compare op
    op *equal_op = NULL;
    for (auto &op : db_schema->operators)
    {
        if (op.name == "=")
        {
            equal_op = &op;
            break;
        }
    }
    if (equal_op == NULL)
        throw runtime_error("intrument insert statement: cannot find = operator");

    // init column reference
    auto wkey_column = make_shared<column_reference>((struct prod *)0, columns[wkey_idx].type, columns[wkey_idx].name, table->name);
    // init the select
    auto select_stmt = make_shared<query_spec>((struct prod *)0, &used_scope, table, equal_op, wkey_column, wkey_value);
    auto involved_tables = extract_words_begin_with(print_stmt_to_string(insert_statement), "t_");

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // BPM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(BEFORE_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }

        // Predicate match
        if (predicates.contains(stmt_idx))
        {
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(predicates[stmt_idx]);
            final_stmt_usage.push_back(stmt_usage(PREDICATE_MATCH, true));
        }
    }

    // VSR
    /*---- version_set select (select * from t where 1=1) ---*/
    // the inserted value may be determined by other table's row.
    auto target_table_str = table->ident();
    involved_tables.erase(table->ident());
    for (auto &table_str : involved_tables)
    {
        auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
        final_tid_queue.push_back(tid); // version_set select, build predicate-WW
        final_stmt_queue.push_back(version_set_select_stmt);
        final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
    }

    // Insert
    // item does not exist, so no need for ealier dependency
    final_tid_queue.push_back(tid);
    final_stmt_queue.push_back(insert_statement);
    final_stmt_usage.push_back(stmt_usage(INSERT_WRITE, false, target_table_str));

    // AWR
    final_tid_queue.push_back(tid); // get the later value to build WR and WW dependency
    final_stmt_queue.push_back(select_stmt);
    final_stmt_usage.push_back(stmt_usage(AFTER_WRITE_READ, true, target_table_str));

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // APM
        for (auto [pred_idx, predicate] : predicates)
        {
            final_tid_queue.push_back(tid);
            final_stmt_usage.push_back(stmt_usage(AFTER_PREDICATE_MATCH, true));
            final_stmt_queue.push_back(predicate);
        }
    }
}

void instrumentor::HandleSelectStmt(shared_ptr<query_spec> query_stmt, int tid, int stmt_idx)
{
    // normal select (with cte) query
    auto involved_tables = extract_words_begin_with(print_stmt_to_string(query_stmt), "t_");

    if (ADD_PREDICATE_INSTRUMENTATION)
    {
        // Predicate match
        if (predicates.contains(stmt_idx))
        {
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(predicates[stmt_idx]);
            final_stmt_usage.push_back(stmt_usage(PREDICATE_MATCH, true));
        }
    }

    // VSR
    for (auto &table_str : involved_tables)
    {
        auto version_set_select_stmt = make_shared<txn_string_stmt>((prod *)0, "SELECT * FROM " + table_str);
        final_tid_queue.push_back(tid); // version_set select, build predicate-WW
        final_stmt_queue.push_back(version_set_select_stmt);
        final_stmt_usage.push_back(stmt_usage(VERSION_SET_READ, true, table_str));
    }

    // SELECT
    final_tid_queue.push_back(tid);
    final_stmt_queue.push_back(query_stmt);
    final_stmt_usage.push_back(stmt_usage(SELECT_READ, false));
}

instrumentor::instrumentor(vector<shared_ptr<prod>> &stmt_queue,
                           vector<int> &tid_queue,
                           shared_ptr<schema> db_schema_) : db_schema(db_schema_)
{
    cerr << "instrumenting the statement ...      ";
    int stmt_num = stmt_queue.size();
    db_schema->fill_scope(used_scope);

    // First need to extract all of the predicates from the statements.
    ExtractPredicates(stmt_queue);

    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt = stmt_queue[i];
        auto tid = tid_queue[i];

        auto update_statement = dynamic_pointer_cast<update_stmt>(stmt);
        if (update_statement)
        { // is a update statement
            HandleUpdateStmt(update_statement, tid, i);
            continue;
        }

        auto delete_statement = dynamic_pointer_cast<delete_stmt>(stmt);
        if (delete_statement)
        {
            HandleDeleteStmt(delete_statement, tid, i);
            continue;
        }

        auto insert_statement = dynamic_pointer_cast<insert_stmt>(stmt);
        if (insert_statement)
        {
            HandleInsertStmt(insert_statement, tid, i);
            continue;
        }

        // begin, commit, abort, SELECT 1 WHERE 0 <> 0, but should not include SELECT * FROM t
        auto string_stmt = dynamic_pointer_cast<txn_string_stmt>(stmt);
        if (string_stmt && print_stmt_to_string(string_stmt).find("SELECT * FROM") == string::npos)
        {
            final_tid_queue.push_back(tid);
            final_stmt_queue.push_back(stmt);
            final_stmt_usage.push_back(stmt_usage(INIT_TYPE, false));
            continue;
        }

        // Should be a query_spec statement.
        auto query_stmt = dynamic_pointer_cast<query_spec>(stmt);
        if (query_stmt)
        {
            HandleSelectStmt(query_stmt, tid, i);
            continue;
        }

        throw runtime_error("Unknown statement type!");
    }

    cerr << "done" << endl;
    return;

    cerr << "Original stmt num: " << stmt_num << endl;
    cerr << "Original statements: " << endl;
    for (int i = 0; i < (int)stmt_queue.size(); i++)
    {
        cerr << i << ":\n";
        cerr << print_stmt_to_string(stmt_queue[i]) << endl;
        cerr << "\n\n";
    }
    cerr << "Found predicates: " << endl;
    for (auto [stmt_idx, predicate] : predicates)
    {
        cerr << "stmt_idx: " << stmt_idx << endl;
        cerr << *predicate << endl;
        cerr << "\n\n";
    }

    cerr << "Final stmt num: " << final_stmt_queue.size() << endl;
    for (int i = 0; i < (int)final_stmt_queue.size(); i++)
    {
        cerr << "\n#" << i << " tid=" << final_tid_queue[i]
             << " usage=" << stmt_basic_type_to_string(final_stmt_usage[i].stmt_type) << " is_instrumented=" << final_stmt_usage[i].is_instrumented << endl;

        cerr << *final_stmt_queue[i] << endl;
    }

    // Wait forever.
    cerr << "Waiting forever..." << endl;
    while (1)
    {
        int x;
        cin >> x;
    }
}