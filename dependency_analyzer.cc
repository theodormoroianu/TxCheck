#include <dependency_analyzer.hh>
#include <functional>
#include <time.h>

#define RESET "\033[0m"
#define BLACK "\033[30m"            /* Black */
#define RED "\033[31m"              /* Red */
#define GREEN "\033[32m"            /* Green */
#define YELLOW "\033[33m"           /* Yellow */
#define BLUE "\033[34m"             /* Blue */
#define MAGENTA "\033[35m"          /* Magenta */
#define CYAN "\033[36m"             /* Cyan */
#define WHITE "\033[37m"            /* White */
#define BOLDBLACK "\033[1m\033[30m" /* Bold Black */

using std::function;

void history::insert_to_history(operate_unit &oper_unit)
{
    auto row_id = oper_unit.row_id;
    auto size = change_history.size();
    bool exist_row_id = false;
    int row_idx;
    for (int i = 0; i < size; i++)
    {
        if (change_history[i].row_id == row_id)
        {
            exist_row_id = true;
            row_idx = i;
            break;
        }
    }

    if (exist_row_id)
        change_history[row_idx].row_op_list.push_back(oper_unit);
    else
    {
        row_change_history rch;
        rch.row_id = row_id;
        rch.row_op_list.push_back(oper_unit);
        change_history.push_back(rch);
    }

    return;
}

stmt_id::stmt_id(vector<int> &final_tid_queue, int stmt_idx)
{
    txn_id = final_tid_queue[stmt_idx];
    stmt_idx_in_txn = -1;
    for (int i = 0; i <= stmt_idx; i++)
    {
        if (final_tid_queue[i] == txn_id)
            stmt_idx_in_txn++;
    }
}

// -1: error, txn_id or stmt_idx_in_txn is -1, or cannot find
int stmt_id::transfer_2_stmt_idx(vector<int> &final_tid_queue)
{
    if (txn_id == -1 || stmt_idx_in_txn == -1)
        return -1;
    int tmp_target_txn_idx = -1;
    auto queue_size = final_tid_queue.size();
    for (int i = 0; i < queue_size; i++)
    {
        if (final_tid_queue[i] != txn_id)
            continue;
        tmp_target_txn_idx++;
        if (tmp_target_txn_idx == stmt_idx_in_txn)
            return i;
    }
    return -1;
}

void dependency_analyzer::build_stmt_depend_from_stmt_idx(int stmt_idx1, int stmt_idx2, dependency_type dt)
{
    auto stmt_id1 = stmt_id(f_txn_id_queue, stmt_idx1);
    auto stmt_id2 = stmt_id(f_txn_id_queue, stmt_idx2);
    auto stmt_pair = make_pair(stmt_id1, stmt_id2);
    if (stmt_dependency_graph.count(stmt_pair) > 0)
        stmt_dependency_graph[stmt_pair].insert(dt);
    else
    {
        set<dependency_type> d_set;
        d_set.insert(dt);
        stmt_dependency_graph[stmt_pair] = d_set;
    }
}

size_t dependency_analyzer::hash_output(row_output &row)
{
    size_t hash = 0;
    for (auto &str : row)
    {
        auto size = str.size();
        for (int i = 0; i < size; i++)
        {
            size_t ch = (size_t)str[i];
            hash = hash * 131 + ch;
        }
    }
    return hash;
}

// for BEFORE_WRITE_READ, VERSION_SET_READ, SELECT_READ
void dependency_analyzer::build_directly_read_dependency(vector<operate_unit> &op_list, int op_idx)
{
    auto &target_op = op_list[op_idx];
    bool find_the_write = false;
    for (int i = op_idx - 1; i >= 0; i--)
    {
        if (op_list[i].stmt_u != AFTER_WRITE_READ) // only search for AFTER_WRITE_READ
            continue;

        // need strict compare to check whether the write is missed
        if (op_list[i].hash != target_op.hash)
            continue;

        find_the_write = true;

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0) // stmts in same transaction should build dependency
            build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, WRITE_READ);

        if (op_list[i].tid != target_op.tid)
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_READ);

        break; // only find the nearest write
    }
    if (find_the_write == false)
    {
        cerr << "Read stmt idx: " << target_op.stmt_idx << endl;
        cerr << "Read stmt tid: " << target_op.tid << endl;

        cerr << "Problem read: ";
        auto &problem_row = hash_to_output[target_op.hash];
        for (int i = 0; i < problem_row.size(); i++)
            cerr << problem_row[i] << " ";
        cerr << endl;

        for (int i = 0; i < op_list.size(); i++)
        {
            if (op_list[i].stmt_u != AFTER_WRITE_READ)
                continue;
            cerr << "AFTER_WRITE_READ " << i << ": ";
            auto &write_row = hash_to_output[op_list[i].hash];
            for (int i = 0; i < write_row.size(); i++)
                cerr << write_row[i] << " ";
            cerr << endl;
        }

        throw runtime_error("BUG: Cannot find the corresponding write");
    }

    return;
}

// for BEFORE_WRITE_READ
void dependency_analyzer::build_directly_item_anti_dependency(vector<operate_unit> &op_list, int op_idx)
{
    auto &target_op = op_list[op_idx];
    if (target_op.stmt_u != BEFORE_WRITE_READ)
        throw runtime_error("something wrong, target_op.stmt_u is not BEFORE_WRITE_READ in build_directly_item_anti_dependency");

    auto list_size = op_list.size();
    for (int i = 0; i < list_size; i++)
    {
        // could not build BWR -> BWR ()
        // if BWR -> BWR is build (RW), then AWR -> BWR is also built (WW), so missing it is fine
        if (op_list[i].stmt_u != SELECT_READ && op_list[i].stmt_u != AFTER_WRITE_READ)
            continue; // only search for SELECT_READ, AFTER_WRITE_READ

        // need eazier compare to build more edge
        if (op_list[i].write_op_id != target_op.write_op_id)
            continue;

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0)
            build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, READ_WRITE);

        if (op_list[i].tid != target_op.tid)
            dependency_graph[op_list[i].tid][target_op.tid].insert(READ_WRITE);

        // do not break, because need to find all the read
    }

    return;
}

// for BEFORE_WRITE_READ
void dependency_analyzer::build_directly_write_dependency(vector<operate_unit> &op_list, int op_idx)
{
    auto &target_op = op_list[op_idx];
    if (target_op.stmt_u != BEFORE_WRITE_READ)
        throw runtime_error("something wrong, target_op.stmt_u is not BEFORE_WRITE_READ in build_directly_write_dependency");

    bool find_the_write = false;
    for (int i = op_idx - 1; i >= 0; i--)
    {
        if (op_list[i].stmt_u != AFTER_WRITE_READ)
            continue;

        // need strict compare to find miss write bug
        if (op_list[i].hash != target_op.hash)
            continue;

        find_the_write = true;

        if (op_list[i].stmt_idx >= 0 && target_op.stmt_idx >= 0)
            build_stmt_depend_from_stmt_idx(op_list[i].stmt_idx, target_op.stmt_idx, WRITE_WRITE);

        if (op_list[i].tid != target_op.tid)
            dependency_graph[op_list[i].tid][target_op.tid].insert(WRITE_WRITE);

        break; // only find the nearest write
    }
    if (find_the_write == false)
        throw runtime_error("BUG: Cannot find the corresponding write");

    return;
}

// should be used after build_start_dependency
void dependency_analyzer::build_VS_dependency()
{
    // if (tid_begin_idx == NULL || tid_strict_begin_idx == NULL || tid_end_idx == NULL) {
    //     cerr << "you should not use build_VS_dependency before build_start_dependency" << endl;
    //     throw runtime_error("you should not use build_VS_dependency before build_start_dependency");
    // }
    for (int i = 0; i < stmt_num; i++)
    {
        auto &i_stmt_u = f_stmt_usage[i];
        if (i_stmt_u != VERSION_SET_READ)
            continue;
        auto &i_tid = f_txn_id_queue[i];
        auto &i_output = f_stmt_output[i];

        set<pair<int, int>> i_pv_pair_set; // primary_key, version_key
        set<int> i_primary_set;            // primary_key
        for (auto &row : i_output)
        {
            auto row_id = stoi(row[primary_key_index]);
            auto version_id = stoi(row[version_key_index]);
            pair<int, int> p(row_id, version_id);
            i_pv_pair_set.insert(p);
            i_primary_set.insert(row_id);
        }

        for (int j = 0; j < stmt_num; j++)
        {
            auto &j_tid = f_txn_id_queue[j];
            // if (i_tid == j_tid)
            //     continue;
            // // skip if they donot interleave
            // if (dependency_graph[i_tid][j_tid].count(STRICT_START_DEPEND) > 0)
            //     continue;
            // if (dependency_graph[j_tid][i_tid].count(STRICT_START_DEPEND) > 0)
            //     continue;

            auto &j_stmt_u = f_stmt_usage[j];
            if (j_stmt_u == UPDATE_WRITE || j_stmt_u == INSERT_WRITE)
            {
                auto after_write_idx = j + 1;
                if (f_stmt_usage[after_write_idx] != AFTER_WRITE_READ)
                {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: after_write_idx is not AFTER_WRITE_READ, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                auto &after_write_output = f_stmt_output[after_write_idx];
                set<pair<int, int>> after_write_pv_pair_set; // primary_key, version_key
                for (auto &row : after_write_output)
                {
                    auto row_id = stoi(row[primary_key_index]);
                    auto version_id = stoi(row[version_key_index]);
                    pair<int, int> p(row_id, version_id);
                    after_write_pv_pair_set.insert(p);
                }

                set<pair<int, int>> res;
                set_intersection(i_pv_pair_set.begin(), i_pv_pair_set.end(),
                                 after_write_pv_pair_set.begin(), after_write_pv_pair_set.end(),
                                 inserter(res, res.begin()));

                if (!res.empty())
                { // if it is not empty, the changed version is seen in version read
                    if (i_tid != j_tid)
                        dependency_graph[j_tid][i_tid].insert(VERSION_SET_DEPEND);
                    // build_stmt_depend_from_stmt_idx(after_write_idx, i, VERSION_SET_DEPEND);
                    // update/insert -> AFTER_WRITE_READ -> VERSION_SET_READ -> target_one
                }
            }
            else if (j_stmt_u == DELETE_WRITE)
            {
                // This is kind of wrong, as DELETE should basically create
                // a hidden version. Probably we need to not accept DELETE statements.
                // TODO
                continue;
                // throw runtime_error("DELETE not supported yet!");
                auto before_write_idx = j - 1;
                if (f_stmt_usage[before_write_idx] != BEFORE_WRITE_READ)
                {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: before_write_idx is not BEFORE_WRITE_READ, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                // skip if they donot handle the same table
                if (f_stmt_usage[before_write_idx].target_table == "" || i_stmt_u.target_table == "")
                {
                    auto err_info = "[INSTRUMENT_ERR] build_VS_dependency: target_table is not initialized, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (f_stmt_usage[before_write_idx].target_table != i_stmt_u.target_table)
                    continue;

                auto &before_write_output = f_stmt_output[before_write_idx];
                if (before_write_output.empty()) // delete nothing, skip
                    continue;

                set<int> before_write_primary_set; // primary_key, version_key
                for (auto &row : before_write_output)
                {
                    auto row_id = stoi(row[primary_key_index]);
                    before_write_primary_set.insert(row_id);
                }

                set<int> res;
                set_intersection(i_primary_set.begin(), i_primary_set.begin(),
                                 before_write_primary_set.begin(), before_write_primary_set.begin(),
                                 inserter(res, res.begin()));
                if (res.empty())
                { // if it is emtpy, the row is deleted
                    if (i_tid != j_tid)
                        dependency_graph[j_tid][i_tid].insert(VERSION_SET_DEPEND);
                    // build_stmt_depend_from_stmt_idx(j, i, VERSION_SET_DEPEND);
                    // BEFORE_WRITE_READ-> delete -> VERSION_SET_READ -> target_one
                }
            }
        }
    }
}

void dependency_analyzer::read_stmt_output_into_pk_and_version(int stmt_idx, set<pair<int, int>> &pk_version_pair, set<int> &primary_key_set)
{
    assert(stmt_idx >= 0 && stmt_idx < stmt_num);

    auto &stmt_output = f_stmt_output[stmt_idx];
    for (auto &row : stmt_output)
    {
        auto row_id = stoi(row[primary_key_index]);
        auto version_id = stoi(row[version_key_index]);

        assert(pk_version_pair.count({row_id, version_id}) == 0);
        pk_version_pair.insert({row_id, version_id});

        assert(primary_key_set.count(row_id) == 0);
        primary_key_set.insert(row_id);
    }
}

bool dependency_analyzer::check_which_version_is_higher(int row_id, int v1, int v2)
{
    // We need to check which version is higher.
    // We can look in the history.

    // Maybe we can just look at the history.
    for (auto &change_history : h.change_history)
    {
        if (change_history.row_id != row_id)
            continue;

        int v1_idx = -1, v2_idx = -1;
        for (int i = 0; i < change_history.row_op_list.size(); i++)
        {
            if (change_history.row_op_list[i].write_op_id == v1)
                v1_idx = i;
            if (change_history.row_op_list[i].write_op_id == v2)
                v2_idx = i;
        }

        if (v1_idx == -1 || v2_idx == -1)
            throw runtime_error("Version not found in history");

        // Idk if right, it also depends on when the version is commited.
        return v1_idx > v2_idx;
    }

    throw runtime_error("Not implemented yet");
}

bool dependency_analyzer::check_if_stmt_is_overwriten(int a_predicate_match, int a_vsr_begin, int a_vsr_end, int b_awr_stmt, int b_bpm_stmt, int b_apm_stmt)
{
    // cerr << "stmt_idx: " << stmt_idx << endl;
    // cerr << "overwrite_awr_stmt: " << overwrite_awr_stmt << endl;
    // cerr << "bpm_stmt: " << bpm_stmt << endl;
    // cerr << "apm_stmt: " << apm_stmt << endl;

    // Sanity checks.
    if (a_predicate_match < 0 || b_awr_stmt < 0 || b_bpm_stmt < 0 || b_apm_stmt < 0)
        throw runtime_error("Negative values!");
    if (b_bpm_stmt > b_awr_stmt || b_apm_stmt < b_awr_stmt)
        throw runtime_error("bpm_stmt > overwrite_awr_stmt || apm_stmt < overwrite_awr_stmt");
    if (a_vsr_begin > a_vsr_end || f_stmt_usage[a_vsr_begin] != VERSION_SET_READ || f_stmt_usage[a_vsr_end] != VERSION_SET_READ)
        throw runtime_error("Invalid version set read statements");
    if (f_txn_id_queue[a_predicate_match] != f_txn_id_queue[a_vsr_begin] || f_txn_id_queue[a_predicate_match] != f_txn_id_queue[a_vsr_end])
        throw runtime_error("Transaction IDs do not match");
    if (f_stmt_usage[a_predicate_match] != PREDICATE_MATCH || f_stmt_usage[b_bpm_stmt] != BEFORE_PREDICATE_MATCH || f_stmt_usage[b_apm_stmt] != AFTER_PREDICATE_MATCH || f_stmt_usage[b_awr_stmt] != AFTER_WRITE_READ)
        throw runtime_error("Invalid statement types");
    if (f_txn_id_queue[b_bpm_stmt] != f_txn_id_queue[b_awr_stmt] || f_txn_id_queue[b_apm_stmt] != f_txn_id_queue[b_awr_stmt])
        throw runtime_error("Transaction IDs do not match");

    // { primary_key, version_key }
    set<pair<int, int>> bpm_pv_pair, apm_pv_pair, predicate_match_pair, awr_pv_pair;
    set<int> bpm_pk, apm_pk, predicate_match_pk, awr_pk;
    read_stmt_output_into_pk_and_version(b_bpm_stmt, bpm_pv_pair, bpm_pk);
    read_stmt_output_into_pk_and_version(b_apm_stmt, apm_pv_pair, apm_pk);
    read_stmt_output_into_pk_and_version(a_predicate_match, predicate_match_pair, predicate_match_pk);
    read_stmt_output_into_pk_and_version(b_awr_stmt, awr_pv_pair, awr_pk);

    // We first compute the version set of the predicate.
    set<pair<int, int>> vsr_pv_pair;
    set<int> vsr_pk;
    for (int vsr_stmt = a_vsr_begin; vsr_stmt <= a_vsr_end; vsr_stmt++)
        if (f_stmt_usage[vsr_stmt] == VERSION_SET_READ)
            read_stmt_output_into_pk_and_version(vsr_stmt, vsr_pv_pair, vsr_pk);

    // We want to check if an element installed by the overwrite statement
    // is a later version of an element part of the VSR.

    // The row ids which have a higher version in the AWR than in the VSR of the predicate match.
    set<int> rows_with_higher_version_in_awr;
    for (auto [pk, v_awr] : awr_pv_pair)
    {
        if (vsr_pk.count(pk) == 0)
            continue;
        int v_vsr = -1;
        for (auto [pk_vsr, v_vsr_] : vsr_pv_pair)
            if (pk_vsr == pk)
                v_vsr = v_vsr_;
        if (v_vsr == -1)
            throw runtime_error("Version not found in VSR");

        // Need to check which version is higher.
        // We can look in the history.
        if (check_which_version_is_higher(pk, v_awr, v_vsr))
            rows_with_higher_version_in_awr.insert(pk);
    }

    // Now we want to check if there is a row in `rows_with_higher_version_in_awr`
    // which is only matched in one of `bpm` and `apm`.
    for (auto pk : rows_with_higher_version_in_awr)
        if (bpm_pk.contains(pk) ^ apm_pk.contains(pk))
            return true;

    return false;
}

// should be used after build_start_dependency
void dependency_analyzer::build_OW_dependency()
{
    // Compute metrics and perform some sanity check.
    // 1. The number of APM and BPM should be equal.
    // 2. The number of APM and BPM should be equal to
    //   the nr of update operations times `total_nr_predicate_matches`.
    int total_nr_predicate_matches = 0;
    int nr_apm = 0, nr_bpm = 0;
    int nr_update_operations = 0;
    for (int i = 0; i < stmt_num; i++)
    {
        if (f_stmt_usage[i] == UPDATE_WRITE || f_stmt_usage[i] == DELETE_WRITE || f_stmt_usage[i] == INSERT_WRITE)
            nr_update_operations++;
        if (f_stmt_usage[i] == AFTER_PREDICATE_MATCH)
            nr_apm++;
        if (f_stmt_usage[i] == BEFORE_PREDICATE_MATCH)
            nr_bpm++;
        if (f_stmt_usage[i] == PREDICATE_MATCH)
            total_nr_predicate_matches++;
    }
    if (nr_apm != nr_bpm || nr_apm != nr_update_operations * total_nr_predicate_matches)
    {
        cerr << "nr_apm: " << nr_apm << endl;
        cerr << "nr_bpm: " << nr_bpm << endl;
        cerr << "nr_update_operations: " << nr_update_operations << endl;
        cerr << "total_nr_predicate_matches: " << total_nr_predicate_matches << endl;
        throw runtime_error("nr_apm != nr_bpm || nr_apm != nr_update_operations * total_nr_predicate_matches");
    }

    // Directly Predicate-Anti-Depends.
    // Part 2 of Directly Predicate-Write-Depends.

    for (int i = 0, processed_predicates = 0; i < stmt_num; i++)
    {
        if (f_stmt_usage[i] != PREDICATE_MATCH)
            continue;
        processed_predicates++;

        int vsr_begin = i + 1, vsr_end = i + 1;
        if (f_stmt_usage[vsr_begin] != VERSION_SET_READ)
            throw runtime_error("vsr_begin is not VERSION_SET_READ");
        while (f_stmt_usage[vsr_end + 1] == VERSION_SET_READ)
            vsr_end++;

        // cerr << "Processing predicate match nr #" << processed_predicates << " at stmt #" << i << endl;

        // We are looking for the `processed_predicates`-th predicate match.
        for (int j = 0, processed_updates = 0; j < stmt_num; j++)
        {
            if (f_stmt_usage[j] != UPDATE_WRITE && f_stmt_usage[j] != DELETE_WRITE && f_stmt_usage[j] != INSERT_WRITE)
                continue;
            processed_updates++;

            // We are looking for the `processed_predicates`-th predicate match.
            int nr_bpm_to_skip = (processed_updates - 1) * total_nr_predicate_matches + processed_predicates;
            int nr_apm_to_skip = nr_bpm_to_skip;
            int bpm_stmt = -1, apm_stmt = -1;

            while (nr_bpm_to_skip)
            {
                bpm_stmt++;
                if (bpm_stmt == stmt_num)
                    throw runtime_error("nr_bpm_to_skip is too large");
                if (f_stmt_usage[bpm_stmt] == BEFORE_PREDICATE_MATCH)
                    nr_bpm_to_skip--;
            }

            while (nr_apm_to_skip)
            {
                apm_stmt++;
                if (apm_stmt == stmt_num)
                    throw runtime_error("nr_apm_to_skip is too large");
                if (f_stmt_usage[apm_stmt] == AFTER_PREDICATE_MATCH)
                    nr_apm_to_skip--;
            }

            if (f_stmt_usage[bpm_stmt] != BEFORE_PREDICATE_MATCH)
                throw runtime_error("bpm_stmt is not BEFORE_PREDICATE_MATCH");
            if (f_stmt_usage[apm_stmt] != AFTER_PREDICATE_MATCH)
                throw runtime_error("apm_stmt is not AFTER_PREDICATE_MATCH");
            if (f_txn_id_queue[bpm_stmt] != f_txn_id_queue[j] || f_txn_id_queue[apm_stmt] != f_txn_id_queue[j])
                throw runtime_error("Transaction IDs do not match");

            int awr = j + 1;
            if (f_stmt_usage[awr] != AFTER_WRITE_READ)
            {
                cerr << "j: " << f_stmt_usage[j] << endl;
                cerr << "awr: " << f_stmt_usage[awr] << endl;
                cerr << "awr+1: " << f_stmt_usage[awr + 1] << endl;

                throw runtime_error("awr is not AFTER_WRITE_READ");
            }
            if (f_txn_id_queue[awr] != f_txn_id_queue[j])
                throw runtime_error("Transaction IDs do not match");

            if (check_if_stmt_is_overwriten(i, vsr_begin, vsr_end, awr, bpm_stmt, apm_stmt))
            {
                build_stmt_depend_from_stmt_idx(i, j, OVERWRITE_DEPEND);
            }
        }
    }

    return;
    // if (tid_begin_idx == NULL || tid_strict_begin_idx == NULL || tid_end_idx == NULL) {
    //     cerr << "you should not use build_VS_dependency before build_start_dependency" << endl;
    //     throw runtime_error("you should not use build_VS_dependency before build_start_dependency");
    // }

    for (int i = 0; i < stmt_num; i++)
    {
        auto &i_stmt_u = f_stmt_usage[i];
        if (i_stmt_u != VERSION_SET_READ)
            continue;
        auto &i_tid = f_txn_id_queue[i];
        auto &i_output = f_stmt_output[i];

        set<pair<int, int>> i_pv_pair_set; // primary_key, version_key
        set<int> i_primary_set;            // primary_key
        for (auto &row : i_output)
        {
            auto row_id = stoi(row[primary_key_index]);
            auto version_id = stoi(row[version_key_index]);
            pair<int, int> p(row_id, version_id);
            i_pv_pair_set.insert(p);
            i_primary_set.insert(row_id);
        }

        int orginal_index = -1;
        for (int j = i + 1; j < stmt_num; j++)
        {
            if (f_stmt_usage[j] == SELECT_READ ||
                f_stmt_usage[j] == UPDATE_WRITE ||
                f_stmt_usage[j] == DELETE_WRITE ||
                f_stmt_usage[j] == INSERT_WRITE)
            {
                orginal_index = j;
                break;
            }
            // Should be same transaction
            if (f_txn_id_queue[j] != i_tid)
                throw std::runtime_error("Expected tid " + to_string(i_tid) + " but found " + to_string(f_txn_id_queue[j]));
        }
        if (orginal_index == -1)
        {
            auto err_info = "[INSTRUMENT_ERR] cannot find the orginal_index in build_OW_dependency";
            cerr << err_info << endl;
            throw runtime_error(err_info);
        }
        if (f_stmt_usage[orginal_index] == UPDATE_WRITE ||
            f_stmt_usage[orginal_index] == INSERT_WRITE)
        {
            orginal_index++; // use after_write_read (SELECT_READ and DELETE_WRITE donot have awr)
            if (f_stmt_usage[orginal_index] != AFTER_WRITE_READ)
            {
                auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: orginal_index + 1 is not AFTER_WRITE_READ, orginal_index = " + to_string(orginal_index);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }
        }

        // We now have the VSR statement i, and the AWR / READ operation in original_index.
        for (int j = 0; j < stmt_num; j++)
        {
            auto &j_tid = f_txn_id_queue[j];
            // if (i_tid == j_tid)
            //     continue;
            // // skip if they donot interleave
            // if (dependency_graph[i_tid][j_tid].count(STRICT_START_DEPEND) > 0)
            //     continue;
            // if (dependency_graph[j_tid][i_tid].count(STRICT_START_DEPEND) > 0)
            //     continue;

            auto &j_stmt_u = f_stmt_usage[j];
            if (j_stmt_u == UPDATE_WRITE || j_stmt_u == DELETE_WRITE)
            {
                auto before_write_idx = j - 1;
                if (f_stmt_usage[before_write_idx] != BEFORE_WRITE_READ)
                {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: before_write_idx is not BEFORE_WRITE_READ, before_write_idx = " + to_string(before_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                auto &before_write_output = f_stmt_output[before_write_idx];
                set<pair<int, int>> before_write_pv_pair_set; // primary_key, version_key
                for (auto &row : before_write_output)
                {
                    auto row_id = stoi(row[primary_key_index]);
                    auto version_id = stoi(row[version_key_index]);
                    pair<int, int> p(row_id, version_id);
                    before_write_pv_pair_set.insert(p);
                }

                set<pair<int, int>> res;
                set_intersection(i_pv_pair_set.begin(), i_pv_pair_set.end(),
                                 before_write_pv_pair_set.begin(), before_write_pv_pair_set.end(),
                                 inserter(res, res.begin()));
                if (!res.empty())
                {
                    if (i_tid != j_tid)
                        dependency_graph[i_tid][j_tid].insert(OVERWRITE_DEPEND);
                    build_stmt_depend_from_stmt_idx(orginal_index, before_write_idx, OVERWRITE_DEPEND);
                    // version_set read -> target_one -> before_read -> update/delete
                }
            }
            else if (j_stmt_u == INSERT_WRITE)
            {
                auto after_write_idx = j + 1;
                if (f_stmt_usage[after_write_idx] != AFTER_WRITE_READ)
                {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: after_write_idx is not AFTER_WRITE_READ, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                // skip if they donot handle the same table
                if (f_stmt_usage[after_write_idx].target_table == "" || i_stmt_u.target_table == "")
                {
                    auto err_info = "[INSTRUMENT_ERR] build_OW_dependency: target_table is not initialized, after_write_idx = " + to_string(after_write_idx);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (f_stmt_usage[after_write_idx].target_table != i_stmt_u.target_table)
                    continue;

                auto &after_write_output = f_stmt_output[after_write_idx];
                if (after_write_output.empty()) // insert nothing, skip
                    continue;
                set<int> after_write_primary_set;
                for (auto &row : after_write_output)
                {
                    auto row_id = stoi(row[primary_key_index]);
                    after_write_primary_set.insert(row_id);
                }
                set<int> res;
                set_intersection(i_primary_set.begin(), i_primary_set.begin(),
                                 after_write_primary_set.begin(), after_write_primary_set.begin(),
                                 inserter(res, res.begin()));
                if (res.empty())
                { // if it is emtpy, the row is not inserted yet
                    if (i_tid != j_tid)
                        dependency_graph[i_tid][j_tid].insert(OVERWRITE_DEPEND);
                    build_stmt_depend_from_stmt_idx(orginal_index, j, OVERWRITE_DEPEND);
                    // version_set read -> target_one -> insert -> after_read
                }
            }
        }
    }
}

void dependency_analyzer::build_start_dependency()
{
    // count the second stmt as begin stmt, because some dbms donot use snapshot unless it read or write something
    auto tid_has_used_begin = new bool[tid_num];
    tid_strict_begin_idx = new int[tid_num];
    tid_begin_idx = new int[tid_num];
    tid_end_idx = new int[tid_num];
    for (int i = 0; i < tid_num; i++)
    {
        tid_strict_begin_idx[i] = -1;
        tid_begin_idx[i] = -1;
        tid_end_idx[i] = -1;
        tid_has_used_begin[i] = false;
    }
    for (int i = 0; i < stmt_num; i++)
    {
        auto tid = f_txn_id_queue[i];
        // skip the first stmt (i.e. start transaction)
        if (tid_has_used_begin[tid] == false)
        {
            tid_has_used_begin[tid] = true;
            tid_strict_begin_idx[tid] = i;
            continue;
        }
        if (tid_begin_idx[tid] == -1)
            tid_begin_idx[tid] = i;
        if (tid_end_idx[tid] < i)
            tid_end_idx[tid] = i;
    }
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            if (i == j)
                continue;
            if (tid_end_idx[i] < tid_begin_idx[j])
            {
                dependency_graph[i][j].insert(START_DEPEND);
                build_stmt_start_dependency(i, j, START_DEPEND);
            }
            if (tid_end_idx[i] < tid_strict_begin_idx[j])
            {
                dependency_graph[i][j].insert(STRICT_START_DEPEND);
                build_stmt_start_dependency(i, j, STRICT_START_DEPEND);
            }
        }
    }
    delete[] tid_has_used_begin;
}

/**
 * Takes BWR, AWR and VSR and adds edges to/from the instrumented statement as a dependency
 * in the `stmt_dependency_graph` map.
 */
void dependency_analyzer::build_stmt_instrument_dependency()
{
    // Given an instrumentation statement, returns the statement before it / after it which it instruments.
    auto find_instrumented_stmt = [&](int instr_stmt_idx, int direction)
    {
        if (!stmt_basic_type_is_instrumentation(f_stmt_usage[instr_stmt_idx].stmt_type))
            throw runtime_error("BUG: stmt is not an instrumentation statement");

        for (int idx = instr_stmt_idx;; idx += direction)
        {
            // Should not be out of range.
            if (idx < 0 || idx >= stmt_num)
                throw runtime_error("BUG: idx out of range, no instrumented stmt found");

            // Should not be from a different transaction.
            if (f_txn_id_queue[idx] != f_txn_id_queue[instr_stmt_idx])
                throw runtime_error("BUG: different transaction id, no instrumented stmt found");

            if (!stmt_basic_type_is_instrumentation(f_stmt_usage[idx].stmt_type))
                return idx;
        }
    };

    for (int i = 0; i < stmt_num; i++)
    {
        auto cur_usage = f_stmt_usage[i];
        auto cur_tid = f_txn_id_queue[i];

        if (cur_usage == BEFORE_WRITE_READ)
        {
            if (i + 1 >= stmt_num)
            {
                auto err_info = "[INSTRUMENT_ERR] i = BEFORE_WRITE_READ, i + 1 >= stmt_num, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            auto next_tid = f_txn_id_queue[i + 1];
            auto next_usage = f_stmt_usage[i + 1];
            if (next_tid != cur_tid)
            {
                auto err_info = "[INSTRUMENT_ERR] BEFORE_WRITE_READ: next_tid != cur_tid, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            if (next_usage != UPDATE_WRITE && next_usage != DELETE_WRITE)
            {
                auto err_info = "[INSTRUMENT_ERR] BEFORE_WRITE_READ: next_usage != UPDATE_WRITE && next_usage != DELETE_WRITE, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            build_stmt_depend_from_stmt_idx(i, i + 1, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == AFTER_WRITE_READ)
        {
            if (i - 1 < 0)
            {
                auto err_info = "[INSTRUMENT_ERR] i = AFTER_WRITE_READ, i - 1 < 0, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            auto prev_tid = f_txn_id_queue[i - 1];
            auto prev_usage = f_stmt_usage[i - 1];
            if (prev_tid != cur_tid)
            {
                auto err_info = "[INSTRUMENT_ERR] AFTER_WRITE_READ: prev_tid != cur_tid, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            if (prev_usage != UPDATE_WRITE && prev_usage != INSERT_WRITE)
            {
                auto err_info = "[INSTRUMENT_ERR] AFTER_WRITE_READ: prev_tid != UPDATE_WRITE && prev_tid != INSERT_WRITE, i = " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            build_stmt_depend_from_stmt_idx(i - 1, i, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == VERSION_SET_READ)
        {
            int normal_pos = i + 1;
            while (normal_pos < stmt_num)
            {
                auto next_tid = f_txn_id_queue[normal_pos];
                auto next_usage = f_stmt_usage[normal_pos];
                if (next_tid != cur_tid)
                {
                    auto err_info = "[INSTRUMENT_ERR] VERSION_SET_READ: next_tid != cur_tid, cur: " + to_string(i) + " next: " + to_string(normal_pos);
                    cerr << err_info << endl;
                    throw runtime_error(err_info);
                }
                if (next_usage == SELECT_READ ||
                    next_usage == UPDATE_WRITE ||
                    next_usage == DELETE_WRITE ||
                    next_usage == INSERT_WRITE)
                    break;
                normal_pos++;
            }

            if (normal_pos == stmt_num)
            {
                auto err_info = "[INSTRUMENT_ERR] VERSION_SET_READ: cannot find the normal one, cur: " + to_string(i);
                cerr << err_info << endl;
                throw runtime_error(err_info);
            }

            // TODO: Check if we really want to do that.
            build_stmt_depend_from_stmt_idx(i, normal_pos, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == BEFORE_PREDICATE_MATCH)
        {
            int instr_stmt_idx = find_instrumented_stmt(i, 1);
            build_stmt_depend_from_stmt_idx(instr_stmt_idx, i, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == AFTER_PREDICATE_MATCH)
        {
            int instr_stmt_idx = find_instrumented_stmt(i, -1);
            build_stmt_depend_from_stmt_idx(instr_stmt_idx, i, INSTRUMENT_DEPEND);
        }
        else if (cur_usage == PREDICATE_MATCH)
        {
            int instr_stmt_idx = find_instrumented_stmt(i, 1);
            build_stmt_depend_from_stmt_idx(instr_stmt_idx, i, INSTRUMENT_DEPEND);
        }
    }
}

set<int> dependency_analyzer::get_instrumented_stmt_set(int queue_idx)
{
    set<int> init_idx_set;
    set<int> processed_idx_set;
    init_idx_set.insert(queue_idx);
    while (!init_idx_set.empty())
    {
        auto select_idx = *init_idx_set.begin();
        init_idx_set.erase(select_idx);
        processed_idx_set.insert(select_idx);

        auto stmt_id1 = stmt_id(f_txn_id_queue, select_idx);
        for (int i = 0; i < stmt_num; i++)
        {
            if (processed_idx_set.count(i) > 0) // has been processed
                continue;
            auto stmt_id2 = stmt_id(f_txn_id_queue, i);
            pair<stmt_id, stmt_id> instrument_pair;
            if (i < select_idx)
                instrument_pair = make_pair<>(stmt_id2, stmt_id1);
            else
                instrument_pair = make_pair<>(stmt_id1, stmt_id2);
            if (stmt_dependency_graph[instrument_pair].count(INSTRUMENT_DEPEND) > 0)
                init_idx_set.insert(i);
        }
    }
    return processed_idx_set;
}

void dependency_analyzer::build_stmt_inner_dependency()
{
    for (int i = 0; i < stmt_num; i++)
    {
        auto tid = f_txn_id_queue[i];
        for (int j = 0; j < i; j++)
        {
            auto prev_tid = f_txn_id_queue[j];
            if (prev_tid == tid)
                build_stmt_depend_from_stmt_idx(j, i, INNER_DEPEND);
        }
    }
}

void dependency_analyzer::build_stmt_start_dependency(int prev_tid, int later_tid, dependency_type dt)
{
    for (int i = 0; i < stmt_num; i++)
    {
        auto i_tid = f_txn_id_queue[i];
        if (i_tid != prev_tid)
            continue;
        for (int j = i + 1; j < stmt_num; j++)
        {
            auto j_tid = f_txn_id_queue[j];
            if (j_tid != later_tid)
                continue;

            build_stmt_depend_from_stmt_idx(i, j, dt);
        }
    }
}

void dependency_analyzer::print_dependency_graph()
{
    cerr << "  ";
    for (int i = 0; i < tid_num; i++)
    {
        if (i < 10)
            cerr << "|     " << i;
        else
            cerr << "|    " << i;
    }
    cerr << "|" << endl;
    for (int i = 0; i < tid_num; i++)
    {
        if (i < 10)
            cerr << " " << i;
        else
            cerr << i;
        for (int j = 0; j < tid_num; j++)
        {
            cerr << "|";
            if (dependency_graph[i][j].count(WRITE_READ))
                cerr << "0";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(WRITE_WRITE))
                cerr << "1";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(READ_WRITE))
                cerr << "2";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(VERSION_SET_DEPEND))
                cerr << "3";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(OVERWRITE_DEPEND))
                cerr << "4";
            else
                cerr << " ";
            if (dependency_graph[i][j].count(STRICT_START_DEPEND))
                cerr << "5";
            else
                cerr << " ";
        }
        cerr << "|" << endl;
    }
}

bool dependency_analyzer::check_any_transaction_cycle()
{
    // stores the edges of the DSG.
    vector<set<pair<int, dependency_type>>> dsg(tid_num);

    // Add the dependency_graph edges to the DSG.
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            // Ignore self-edges.
            if (i == j)
                continue;

            for (auto &dt : dependency_graph[i][j])
            {
                dsg[i].insert({j, dt});
            }
        }
    }

    // Add statement edges to the DSG.
    for (auto &[stmt_pair, dt_set] : stmt_dependency_graph)
    {
        auto [stmt1, stmt2] = stmt_pair;
        int trans1 = stmt1.txn_id, trans2 = stmt2.txn_id;

        // Ignore self-edges.
        if (trans1 == trans2)
            continue;

        for (auto &dt : dt_set)
        {
            dsg[trans1].insert({trans2, dt});
        }
    }

    // Remove all START_DEPENDS and STRICT_START_DEPENDS.
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            dsg[i].erase({j, START_DEPEND});
            dsg[i].erase({j, STRICT_START_DEPEND});
        }
    }

    // Check for any cycles.
    // TODO: Check for cycles of some specific type.

    // If a node was already visited.
    vector<int> visited(tid_num, 0);
    // If a node is currently being visited.
    vector<int> is_active(tid_num, 0);
    // The parent of a node.
    vector<int> node_parent(tid_num, -1);
    vector<int> node_parent_edge_type(tid_num, -1);

    // Stores a cycle if one is found.
    bool cycle_found = false;
    vector<pair<int, int>> cycle;

    // println("There are {} transactions.", tid_num);
    // for (auto i : ranges::iota_view(0, tid_num))
    // {
    //     println("Transaction {} has {} edges.", i, dsg[i].size());
    //     for (auto &[neighbour, dep_type] : dsg[i])
    //     {
    //         println("    -> Transaction {} with dependency type {}", neighbour, (int)dep_type);
    //     }
    // }

    cerr << "Checking for cycles in the dependency graph... ";
    function<void(int, int, int)> Dfs = [&](int node, int parent, int type)
    {
        if (cycle_found)
            return;

        // Found a cycle.
        if (is_active[node])
        {
            assert(cycle_found == false);
            cycle_found = true;
            cycle.push_back({parent, -1});
            for (int cnt = parent; cnt != node; cnt = node_parent[cnt])
            {
                assert(cnt != -1);
                cycle.push_back({node_parent[cnt], node_parent_edge_type[cnt]});
            }
            return;
        }

        // Node is already visited.
        if (visited[node])
            return;

        // Mark the node as active and visited, call recursively.
        node_parent[node] = parent;
        node_parent_edge_type[node] = type;
        is_active[node] = 1;
        visited[node] = 1;

        for (auto [neighbour, dep_type] : dsg[node])
        {
            Dfs(neighbour, node, dep_type);
        }

        // Mark the node as inactive.
        is_active[node] = 0;
    };

    // Start a DFS from all nodes.
    for (int i = 0; i < tid_num; i++)
    {
        Dfs(i, -1, -1);
    }

    if (cycle_found)
    {
        cerr << "There are " << tid_num << " transactions." << endl;
        for (int i = 0; i < tid_num; i++)
        {
            cerr << "Transaction " << i << " has " << dsg[i].size() << " edges." << endl;
            for (auto &[neighbour, dep_type] : dsg[i])
            {
                cerr << "    -> Transaction " << neighbour << " with dependency type " << (int)dep_type << endl;
            }
        }

        cerr << RED << "Cycle found in the dependency graph." << RESET << endl;
        cerr << "Cycle: ";
        cerr << "Txn " << cycle[0].first;
        reverse(cycle.begin(), cycle.end());
        for (auto &node : cycle)
        {
            cerr << " --> Txn " << node.first;
        }
        cerr << endl;

        while (true)
        {
            // wait.
            sleep(1);
        }
        return true;
    }
    cerr << "done." << endl;
    return false;
}

dependency_analyzer::dependency_analyzer(vector<stmt_output> &init_output,
                                         vector<stmt_output> &total_output,
                                         vector<int> &final_tid_queue,
                                         vector<stmt_usage> &final_stmt_usage,
                                         vector<txn_status> &final_txn_status,
                                         int t_num,
                                         int primary_key_idx,
                                         int write_op_key_idx) : tid_num(t_num + 1), // add 1 for init txn
                                                                 tid_begin_idx(NULL),
                                                                 tid_strict_begin_idx(NULL),
                                                                 tid_end_idx(NULL),
                                                                 primary_key_index(primary_key_idx),
                                                                 version_key_index(write_op_key_idx),
                                                                 f_txn_status(final_txn_status),
                                                                 f_txn_id_queue(final_tid_queue),
                                                                 f_stmt_usage(final_stmt_usage),
                                                                 f_stmt_output(total_output)
{
    if (f_stmt_output.size() != f_txn_id_queue.size() || f_stmt_output.size() != f_stmt_usage.size())
    {
        cerr << "dependency_analyzer: total_output, final_tid_queue and final_stmt_usage size are not equal" << endl;
        throw runtime_error("dependency_analyzer: total_output, final_tid_queue and final_stmt_usage size are not equal");
    }
    stmt_num = f_stmt_output.size();

    f_txn_status.push_back(TXN_COMMIT); // for init txn;

    for (int txn_id = 0; txn_id < tid_num; txn_id++)
    {
        int txn_stmt_num = 0;
        for (int i = 0; i < stmt_num; i++)
        {
            if (f_txn_id_queue[i] == txn_id)
                txn_stmt_num++;
        }
        f_txn_size.push_back(txn_stmt_num);
    }

    dependency_graph = new set<dependency_type> *[tid_num];
    for (int i = 0; i < tid_num; i++)
        dependency_graph[i] = new set<dependency_type>[tid_num];

    // Add versions added the init transaction.
    for (auto &each_output : init_output)
    {
        if (each_output.empty())
            continue;
        for (auto &row : each_output)
        {
            auto row_id = stoi(row[primary_key_idx]);
            auto write_op_id = stoi(row[write_op_key_idx]);
            auto hash = hash_output(row);
            hash_to_output[hash] = row;
            operate_unit op(stmt_usage(AFTER_WRITE_READ, false), write_op_id, tid_num - 1, -1, row_id, hash);
            h.insert_to_history(op);
        }
    }

    // Add versions added by other transactions.
    for (int i = 0; i < stmt_num; i++)
    {
        auto &each_output = f_stmt_output[i];
        auto tid = f_txn_id_queue[i];
        auto stmt_u = f_stmt_usage[i];

        // do not analyze empty output select read;
        // write operation (insert, delete, update) will be analzye by using before/after-write read
        if (each_output.empty())
            continue;

        for (auto &row : each_output)
        {
            auto row_id = stoi(row[primary_key_idx]);
            auto write_op_id = stoi(row[write_op_key_idx]);
            auto hash = hash_output(row);
            hash_to_output[hash] = row;
            operate_unit op(stmt_u, write_op_id, tid, i, row_id, hash);
            h.insert_to_history(op);
        }
    }

    cerr << "Checking if the pk, vk are all distinct (" << stmt_num << " statements)... ";
    set<pair<int, int>> pk_vk_set;

    for (int i = 0; i < stmt_num; i++)
    {
        // Ignore non-write
        if (f_stmt_usage[i] != AFTER_WRITE_READ)
            continue;

        set<pair<int, int>> pkvk;
        set<int> pk;
        read_stmt_output_into_pk_and_version(i, pkvk, pk);
        for (auto &p : pkvk)
        {
            if (pk_vk_set.contains(p))
            {
                cerr << "pk: " << p.first << " vk: " << p.second << " is not distinct" << endl;
                throw runtime_error("pk, vk are not distinct");
            }
            pk_vk_set.insert(p);
        }
    }
    cerr << "done." << endl;

    // first build instrument dependency, make sure that the instrument is correct
    build_stmt_instrument_dependency();

    // generate ww, wr, rw dependency
    for (auto &row_history : h.change_history)
    {
        auto &row_op_list = row_history.row_op_list;
        auto size = row_op_list.size();
        for (int i = 0; i < size; i++)
        {
            auto &op_unit = row_op_list[i];
            if (op_unit.tid == tid_num - 1) // init txn do not depend on others
                continue;
            if (op_unit.stmt_u == AFTER_WRITE_READ)
                continue;

            // Directly Item-Read-Depends.
            build_directly_read_dependency(row_op_list, i); // it is a read itself

            if (op_unit.stmt_u == BEFORE_WRITE_READ)
            {
                // Directly Item-Write-Depends.
                build_directly_write_dependency(row_op_list, i);

                // Directly Item-Anti-Depends.
                build_directly_item_anti_dependency(row_op_list, i);
            }
        }
    }

    // // generate start dependency (for snapshot)
    build_start_dependency();

    // build version_set depend and overwrite depend that should be build after start depend
    // Directly Predicate-Read-Depends.
    // Part 1 of Directly Predicate-Write-Depends.
    build_VS_dependency();

    // TODO:
    // Directly Predicate-Anti-Depends.
    // Part 2 of Directly Predicate-Write-Depends.

    build_OW_dependency();

    // generate stmt inner depend
    build_stmt_inner_dependency();

    // // print dependency graph
    // print_dependency_graph();

    // println("Printing the start and end of each transaction.");
    // for (int txn = 0; txn < tid_num; txn++)
    // {
    //     println("Transaction {} starts at stmt {} and ends at stmt {}.", txn, tid_begin_idx[txn], tid_end_idx[txn]);
    // }
}

dependency_analyzer::~dependency_analyzer()
{
    delete[] tid_end_idx;
    delete[] tid_begin_idx;
    delete[] tid_strict_begin_idx;

    for (int i = 0; i < tid_num; i++)
        delete[] dependency_graph[i];
    delete[] dependency_graph;
}

// G1a: Aborted Reads. A history H exhibits phenomenon G1a if it contains an aborted
// transaction Ti and a committed transaction Tj such that Tj has read some object
// (maybe via a predicate) modified by Ti. Phenomenon G1a can be represented using
// the following history fragments:
// wi(xi:m) ... rj(xi:m) ... (ai and cj in any order)
// wi(xi:m) ... rj(P: xi:m, ...) ... (ai and cj in any order)
bool dependency_analyzer::check_G1a()
{
    // check whether there is a read dependency from Ti to Tj that is aborted
    for (int j = 0; j < tid_num; j++)
    {
        if (f_txn_status[j] != TXN_ABORT)
            continue; // txn j must be aborted

        for (int i = 0; i < tid_num; i++)
        {
            if (f_txn_status[i] != TXN_COMMIT)
                continue; // txn i must be committed

            auto &dependencies = dependency_graph[j][i]; // j(abort) -> WR -> i(commit) [i wr depend on j]
            if (dependencies.count(WRITE_READ) > 0)
            {
                cerr << "abort txn: " << j << endl;
                cerr << "commit txn: " << i << endl;
                return true;
            }
        }
    }
    return false;
}

// G1b: Intermediate Reads. A history H exhibits phenomenon G1b if it contains a
// committed transaction Tj that has read a version of object x (maybe via a predicate)
// written by transaction Ti that was not Ti’s final modification of x. The following history
// fragments represent this phenomenon:
// wi(xi:m) ... rj(xi:m) ... wi(xi:n) ... cj
// wi(xi:m) ... rj(P: xi:m, ...) ... wi(xi:n) ... cj
bool dependency_analyzer::check_G1b()
{
    for (auto &rch : h.change_history)
    {
        auto &op_list = rch.row_op_list;
        auto opl_size = op_list.size();
        for (int i = 0; i < opl_size; i++)
        {
            if (op_list[i].stmt_u != AFTER_WRITE_READ)
                continue;

            int wop_id = op_list[i].write_op_id;
            int tid = op_list[i].tid;
            int txn_end_idx = tid_end_idx[tid];
            int other_read_idx = -1;
            int second_write_idx = -1;

            for (int j = i + 1; j < opl_size; j++)
            {
                if (op_list[j].stmt_idx > txn_end_idx)
                    break; // the later stmt will not contain the write from txn i

                // check whether the earlier version is read
                if (other_read_idx == -1 &&
                    op_list[j].write_op_id == wop_id &&
                    op_list[j].tid != tid)
                    other_read_idx = j;

                // check whether it will be rewrite by itself
                if (second_write_idx == -1 &&
                    op_list[j].tid == tid &&
                    op_list[j].stmt_u == BEFORE_WRITE_READ)
                    second_write_idx = j;

                if (other_read_idx >= 0 && second_write_idx >= 0)
                {
                    cerr << "first_write_idx: " << i << endl;
                    cerr << "tid: " << tid << endl;
                    cerr << "outpout: " << endl;
                    auto &first_write_row = hash_to_output[op_list[i].hash];
                    for (int e = 0; e < first_write_row.size(); e++)
                        cerr << first_write_row[e] << " ";
                    cerr << endl;

                    cerr << "other_read_idx: " << other_read_idx << endl;
                    cerr << "tid: " << op_list[other_read_idx].tid << endl;
                    cerr << "outpout: " << endl;
                    auto &read_row = hash_to_output[op_list[other_read_idx].hash];
                    for (int e = 0; e < read_row.size(); e++)
                        cerr << read_row[e] << " ";
                    cerr << endl;

                    cerr << "second_write_idx: " << second_write_idx << endl;
                    cerr << "tid: " << op_list[second_write_idx].tid << endl;

                    return true;
                }
            }
        }
    }

    return false;
}

// recursively remove the node have 0 in-degree
// return false if graph is empty after reduction, otherwise true
bool dependency_analyzer::reduce_graph_indegree(int **direct_graph, int length)
{
    set<int> deleted_nodes;
    while (1)
    {
        // check whether the graph is empty
        if (deleted_nodes.size() == length)
            return false;

        // find a node whose in-degree is 0
        int zero_indegree_idx = -1;
        for (int i = 0; i < length; i++)
        {
            if (deleted_nodes.count(i) > 0)
                continue;

            bool has_indegree = false;
            for (int j = 0; j < length; j++)
            {
                if (direct_graph[j][i] > 0)
                {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false)
            {
                zero_indegree_idx = i;
                break;
            }
        }
        // if all nodes have indegree, there is a cycle
        if (zero_indegree_idx == -1)
            return true;

        // delete the node and edge from node to other node
        deleted_nodes.insert(zero_indegree_idx);
        for (int j = 0; j < length; j++)
            direct_graph[zero_indegree_idx][j] = 0;
    }

    return false;
}

// recursively remove the node have 0 out-degree
// return false if graph is empty after reduction, otherwise true
bool dependency_analyzer::reduce_graph_outdegree(int **direct_graph, int length)
{
    set<int> deleted_nodes;
    while (1)
    {
        // check whether the graph is empty
        if (deleted_nodes.size() == length)
            return false;

        // find a node whose out-degree is 0
        int zero_outdegree_idx = -1;
        for (int i = 0; i < length; i++)
        {
            if (deleted_nodes.count(i) > 0)
                continue;

            bool has_outdegree = false;
            for (int j = 0; j < length; j++)
            {
                if (direct_graph[i][j] > 0)
                {
                    has_outdegree = true;
                    break;
                }
            }
            if (has_outdegree == false)
            {
                zero_outdegree_idx = i;
                break;
            }
        }
        // if all nodes have outdegree, there is a cycle
        if (zero_outdegree_idx == -1)
            return true;

        // delete the node and edge from other node to this node
        deleted_nodes.insert(zero_outdegree_idx);
        for (int i = 0; i < length; i++)
            direct_graph[i][zero_outdegree_idx] = 0;
    }

    return false;
}

bool dependency_analyzer::check_G1c()
{
    set<dependency_type> ww_wr_set;
    ww_wr_set.insert(WRITE_WRITE);
    ww_wr_set.insert(WRITE_READ);

    auto tmp_dgraph = new int *[tid_num];
    for (int i = 0; i < tid_num; i++)
        tmp_dgraph[i] = new int[tid_num];
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            tmp_dgraph[i][j] = 0;
        }
    }

    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++)
    {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++)
        {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(ww_wr_set.begin(), ww_wr_set.end(),
                             dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                             inserter(res, res.begin()));

            // have needed edges
            if (res.empty() == false)
                tmp_dgraph[i][j] = 1;
        }
    }

    reduce_graph_indegree(tmp_dgraph, tid_num);
    bool have_cycle = reduce_graph_outdegree(tmp_dgraph, tid_num);
    if (have_cycle)
    {
        cerr << "have cycle in G1c" << endl;
        for (int i = 0; i < tid_num; i++)
        {
            for (int j = 0; j < tid_num; j++)
            {
                if (tmp_dgraph[i][j] == 1)
                    cerr << i << " " << j << endl;
            }
        }
    }

    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return have_cycle;
}

// G2-item: Item Anti-dependency Cycles. A history H exhibits phenomenon G2-item
// if DSG(H) contains a directed cycle having one or more item-anti-dependency edges.
bool dependency_analyzer::check_G2_item()
{
    set<dependency_type> ww_wr_rw_set;
    ww_wr_rw_set.insert(WRITE_WRITE);
    ww_wr_rw_set.insert(WRITE_READ);
    ww_wr_rw_set.insert(READ_WRITE);

    auto tmp_dgraph = new int *[tid_num];
    for (int i = 0; i < tid_num; i++)
        tmp_dgraph[i] = new int[tid_num];
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            tmp_dgraph[i][j] = 0;
        }
    }

    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++)
    {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++)
        {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(ww_wr_rw_set.begin(), ww_wr_rw_set.end(),
                             dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                             inserter(res, res.begin()));

            // have needed edges
            if (res.empty() == false)
                tmp_dgraph[i][j] = 1;
        }
    }

    reduce_graph_indegree(tmp_dgraph, tid_num);
    bool have_cycle = reduce_graph_outdegree(tmp_dgraph, tid_num);
    if (have_cycle)
    {
        cerr << "have cycle in G2_item" << endl;
        for (int i = 0; i < tid_num; i++)
        {
            for (int j = 0; j < tid_num; j++)
            {
                if (tmp_dgraph[i][j] == 1)
                {
                    cerr << i << " " << j << ": ";
                    for (auto &dependency : dependency_graph[i][j])
                        cerr << dependency << " ";
                    cerr << endl;
                }
            }
        }
    }

    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return have_cycle;
}

bool dependency_analyzer::check_GSIa()
{
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            // check whether they have ww or wr dependency
            if (dependency_graph[i][j].count(WRITE_WRITE) == 0 &&
                dependency_graph[i][j].count(WRITE_READ) == 0)
                continue;

            // check whether they have start dependency
            if (dependency_graph[i][j].count(START_DEPEND) == 0)
            {
                cerr << "txn i: " << i << endl;
                cerr << "txn j: " << j << endl;
                return true;
            }
        }
    }
    return false;
}

bool dependency_analyzer::check_GSIb()
{
    set<dependency_type> target_dependency_set;
    target_dependency_set.insert(WRITE_WRITE);
    target_dependency_set.insert(WRITE_READ);
    target_dependency_set.insert(READ_WRITE);
    target_dependency_set.insert(STRICT_START_DEPEND);

    auto tmp_dgraph = new int *[tid_num];
    for (int i = 0; i < tid_num; i++)
        tmp_dgraph[i] = new int[tid_num];
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            tmp_dgraph[i][j] = 0;
        }
    }

    // initialize tmp_dgraph
    for (int i = 0; i < tid_num; i++)
    {
        if (f_txn_status[i] != TXN_COMMIT)
            continue;
        for (int j = 0; j < tid_num; j++)
        {
            if (f_txn_status[j] != TXN_COMMIT)
                continue;
            set<dependency_type> res;
            set_intersection(target_dependency_set.begin(), target_dependency_set.end(),
                             dependency_graph[i][j].begin(), dependency_graph[i][j].end(),
                             inserter(res, res.begin()));

            // have needed edges
            if (res.empty() == false)
                tmp_dgraph[i][j] = 1;
        }
    }

    if (reduce_graph_indegree(tmp_dgraph, tid_num) == false ||
        reduce_graph_outdegree(tmp_dgraph, tid_num) == false)
    { // empty

        for (int i = 0; i < tid_num; i++)
            delete[] tmp_dgraph[i];
        delete[] tmp_dgraph;
        return false;
    }

    // check which edge only have rw dependency
    vector<pair<int, int>> rw_edges;
    for (int i = 0; i < tid_num; i++)
    {
        for (int j = 0; j < tid_num; j++)
        {
            if (tmp_dgraph[i][j] == 0)
                continue;
            // if there is other depend (WR, WW, START(equal to WR or WW according to GSIa)),
            // no need to remove it, will be report by G1c
            if (dependency_graph[i][j].size() > 1)
                continue;
            if (dependency_graph[i][j].count(READ_WRITE) == 0)
                continue;

            // exactly the READ_WRITE depend
            rw_edges.push_back(pair<int, int>(i, j));
            tmp_dgraph[i][j] = 0; // delete the edge
        }
    }

    // only leave one rw edege
    bool has_rw_cycle = false;
    for (auto &rw_edge : rw_edges)
    {
        // only insert 1 rw edge each time
        tmp_dgraph[rw_edge.first][rw_edge.second] = 1;
        if (reduce_graph_indegree(tmp_dgraph, tid_num))
        {
            has_rw_cycle = true;
            break;
        }
        tmp_dgraph[rw_edge.first][rw_edge.second] = 0;
    }

    if (has_rw_cycle)
    {
        cerr << "have cycle in GSIb" << endl;
        for (int i = 0; i < tid_num; i++)
        {
            for (int j = 0; j < tid_num; j++)
            {
                if (tmp_dgraph[i][j] == 1)
                {
                    cerr << i << " " << j << ": ";
                    for (auto &dependency : dependency_graph[i][j])
                        cerr << dependency << " ";
                    cerr << endl;
                }
            }
        }
    }

    for (int i = 0; i < tid_num; i++)
        delete[] tmp_dgraph[i];
    delete[] tmp_dgraph;

    return has_rw_cycle;
}

// stmt_dist_graph may have cycle
vector<stmt_id> dependency_analyzer::longest_stmt_path(
    map<pair<stmt_id, stmt_id>, int> &stmt_dist_graph)
{
    map<stmt_id, stmt_id> dad_stmt;
    map<stmt_id, int> dist_length;
    set<stmt_id> real_deleted_node; // to delete cycle
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        dist_length[stmt_i] = 0;
    }
    set<stmt_id> all_stmt_set;
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        all_stmt_set.insert(stmt_i);
    }

    auto tmp_stmt_graph = stmt_dist_graph;
    set<stmt_id> delete_node;
    while (delete_node.size() + real_deleted_node.size() < stmt_num)
    {
        int zero_indegree_idx = -1;
        // --- find zero-indegree statement ---
        for (int i = 0; i < stmt_num; i++)
        {
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (delete_node.count(stmt_i) > 0) // has been deleted from tmp_stmt_graph
                continue;
            if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
                continue;
            bool has_indegree = false;
            for (int j = 0; j < stmt_num; j++)
            {
                auto stmt_j = stmt_id(f_txn_id_queue, j);
                if (tmp_stmt_graph.count(make_pair(stmt_j, stmt_i)) > 0)
                {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false)
            {
                zero_indegree_idx = i;
                break;
            }
        }
        // ------------------------------------

        // if do not has zero-indegree statement, so there is a cycle
        if (zero_indegree_idx == -1)
        {
            // cerr << "There is a cycle in longest_stmt_path(), delete one node: ";
            // select one node to delete
            auto tmp_stmt_set = all_stmt_set;
            for (auto &node : delete_node)
                tmp_stmt_set.erase(node);
            for (auto &node : real_deleted_node)
                tmp_stmt_set.erase(node);
            auto r = rand() % tmp_stmt_set.size();
            auto select_one_it = tmp_stmt_set.begin();
            advance(select_one_it, r);

            // delete its set (version_set, before_read, itself, after_read)
            auto select_stmt_id = *select_one_it;
            auto select_queue_idx = select_stmt_id.transfer_2_stmt_idx(f_txn_id_queue);
            auto select_idx_set = get_instrumented_stmt_set(select_queue_idx);
            for (auto chosen_idx : select_idx_set)
            {
                auto chosen_stmt_id = stmt_id(f_txn_id_queue, chosen_idx);
                real_deleted_node.insert(chosen_stmt_id);
                for (int i = 0; i < stmt_num; i++)
                {
                    auto out_branch = make_pair(chosen_stmt_id, stmt_id(f_txn_id_queue, i));
                    auto in_branch = make_pair(stmt_id(f_txn_id_queue, i), chosen_stmt_id);
                    tmp_stmt_graph.erase(out_branch);
                    tmp_stmt_graph.erase(in_branch);
                }
                // cerr << chosen_stmt_id.txn_id << "." << chosen_stmt_id.stmt_idx_in_txn << ", ";
            }
            // cerr << endl;
            continue;
        }
        // ------------------------------------

        // if do has zero-indegree statement
        int cur_max_length = 0;
        stmt_id cur_max_dad;
        auto stmt_zero_idx = stmt_id(f_txn_id_queue, zero_indegree_idx);
        for (int i = 0; i < stmt_num; i++)
        {
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
                continue;
            auto branch = make_pair(stmt_i, stmt_zero_idx);
            if (stmt_dist_graph.count(branch) == 0)
                continue;
            if (dist_length[stmt_i] + stmt_dist_graph[branch] > cur_max_length)
            {
                cur_max_length = dist_length[stmt_i] + stmt_dist_graph[branch];
                cur_max_dad = stmt_i;
            }
        }
        dist_length[stmt_zero_idx] = cur_max_length;
        dad_stmt[stmt_zero_idx] = cur_max_dad; // the first one is (-1, -1): no dad

        delete_node.insert(stmt_zero_idx);
        for (int j = 0; j < stmt_num; j++)
        {
            auto branch = make_pair(stmt_zero_idx, stmt_id(f_txn_id_queue, j));
            tmp_stmt_graph.erase(branch);
        }
    }

    vector<stmt_id> longest_path;
    int longest_dist = 0;
    stmt_id longest_dist_stmt;
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        if (real_deleted_node.count(stmt_i) > 0) // // has been really deleted (for decycle)
            continue;
        auto path_length = dist_length[stmt_i];
        if (path_length > longest_dist)
        {
            longest_dist = path_length;
            longest_dist_stmt = stmt_i;
        }
    }

    while (longest_dist_stmt.txn_id != -1)
    { // default
        longest_path.insert(longest_path.begin(), longest_dist_stmt);
        longest_dist_stmt = dad_stmt[longest_dist_stmt];
    }

    cerr << "stmt path length: " << longest_dist << endl;
    return longest_path;
}

vector<stmt_id> dependency_analyzer::longest_stmt_path()
{
    map<pair<stmt_id, stmt_id>, int> stmt_dist_graph;
    for (int i = 0; i < stmt_num; i++)
    {
        if (f_txn_status[f_txn_id_queue[i]] != TXN_COMMIT)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        for (int j = 0; j < stmt_num; j++)
        {
            if (f_txn_status[f_txn_id_queue[j]] != TXN_COMMIT)
                continue;
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto branch = make_pair(stmt_i, stmt_j);
            if (stmt_dependency_graph.count(branch) == 0)
                continue;
            auto depend_set = stmt_dependency_graph[branch];
            depend_set.erase(START_DEPEND);
            depend_set.erase(INSTRUMENT_DEPEND);
            if (depend_set.empty())
                continue;
            if (depend_set.count(INNER_DEPEND) > 0 && depend_set.size() == 1)
                stmt_dist_graph[branch] = 1;
            else if (depend_set.count(STRICT_START_DEPEND) > 0 && depend_set.size() == 1)
                stmt_dist_graph[branch] = 10;
            else if (depend_set.count(STRICT_START_DEPEND) > 0 || depend_set.count(INNER_DEPEND) > 0)
                stmt_dist_graph[branch] = 100; // contain STRICT_START_DEPEND or INNER_DEPEND, and other
            else if (depend_set.count(WRITE_WRITE) > 0 ||
                     depend_set.count(WRITE_READ) > 0)
                stmt_dist_graph[branch] = 100000; // contain WRITE_READ or WRITE_WRITE, but do not contain start and inner
            else if (depend_set.count(VERSION_SET_DEPEND) > 0 ||
                     depend_set.count(OVERWRITE_DEPEND) > 0 ||
                     depend_set.count(READ_WRITE) > 0)
                stmt_dist_graph[branch] = 10000; // only contain VERSION_SET_DEPEND, OVERWRITE_DEPEND and READ_WRITE
        }
    }

    auto path = longest_stmt_path(stmt_dist_graph);
    auto path_size = path.size();
    for (int i = 0; i < path_size; i++)
    {
        auto txn_id = path[i].txn_id;
        auto stmt_pos = path[i].stmt_idx_in_txn;
        if (stmt_pos != 0 && f_txn_size[txn_id] != stmt_pos + 1)
            continue;
        // if it is the first one (begin), or last one (commit), delete it
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    // delete replaced stmt
    for (int i = 0; i < path_size; i++)
    {
        auto queue_idx = path[i].transfer_2_stmt_idx(f_txn_id_queue);
        if (f_stmt_usage[queue_idx] != INIT_TYPE)
            continue;
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    return path;
}

vector<stmt_id> dependency_analyzer::topological_sort_path(set<stmt_id> deleted_nodes, bool *delete_flag)
{
    if (delete_flag != NULL)
        *delete_flag = false;
    vector<stmt_id> path;
    auto tmp_stmt_dependency_graph = stmt_dependency_graph;
    set<stmt_id> outputted_node; // the node that has been outputted from graph
    set<stmt_id> all_stmt_set;   // record all stmts in the graph
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        all_stmt_set.insert(stmt_i);
    }

    // deleted_nodes include:
    //  1) the nodes that have been deleted for decycle,
    //  2) the nodes in abort stmt
    //  3) the nodes that have been deleted in transaction_test::multi_stmt_round_test

    // delete node that in abort txn
    for (int i = 0; i < stmt_num; i++)
    {
        auto txn_id = f_txn_id_queue[i];
        if (f_txn_status[txn_id] == TXN_COMMIT)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        deleted_nodes.insert(stmt_i);
        for (int j = 0; j < stmt_num; j++)
        {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            tmp_stmt_dependency_graph.erase(out_branch);
            tmp_stmt_dependency_graph.erase(in_branch);
        }
    }

    // delete start and inner dependency
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        for (int j = i; j < stmt_num; j++)
        {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            if (tmp_stmt_dependency_graph.count(out_branch))
            {
                tmp_stmt_dependency_graph[out_branch].erase(START_DEPEND);
                tmp_stmt_dependency_graph[out_branch].erase(STRICT_START_DEPEND);
                tmp_stmt_dependency_graph[out_branch].erase(INNER_DEPEND);
                if (tmp_stmt_dependency_graph[out_branch].empty())
                    tmp_stmt_dependency_graph.erase(out_branch);
            }
            if (tmp_stmt_dependency_graph.count(in_branch))
            {
                tmp_stmt_dependency_graph[in_branch].erase(START_DEPEND);
                tmp_stmt_dependency_graph[in_branch].erase(STRICT_START_DEPEND);
                tmp_stmt_dependency_graph[in_branch].erase(INNER_DEPEND);
                if (tmp_stmt_dependency_graph[in_branch].empty())
                    tmp_stmt_dependency_graph.erase(in_branch);
            }
        }
    }

    while (outputted_node.size() + deleted_nodes.size() < stmt_num)
    {
        int zero_indegree_idx = -1;
        set<int> checked_idx;
        // --- find zero-indegree stmt block ---
        for (int i = stmt_num - 1; i >= 0; i--)
        { // use reverse order as possible
            if (checked_idx.count(i) > 0)
                continue;
            auto stmt_i = stmt_id(f_txn_id_queue, i);
            if (outputted_node.count(stmt_i) > 0) // has been outputted from tmp_stmt_graph
                continue;
            if (deleted_nodes.count(stmt_i) > 0) // has been really deleted (for decycle)
                continue;
            bool has_indegree = false;

            // get its set (version_set, before_read, itself, after_read)
            // check whether the node and its set have indegree
            set<int> i_idx_set = get_instrumented_stmt_set(i);
            for (auto chosen_idx : i_idx_set)
            {
                checked_idx.insert(chosen_idx);
                auto stmt_chosen_idx = stmt_id(f_txn_id_queue, chosen_idx);
                for (int j = 0; j < stmt_num; j++)
                {
                    if (i_idx_set.count(j) > 0) // exclude self ring
                        continue;
                    auto stmt_j = stmt_id(f_txn_id_queue, j);
                    auto in_branch = make_pair(stmt_j, stmt_chosen_idx);
                    if (tmp_stmt_dependency_graph.count(in_branch) == 0)
                        continue;
                    // if (tmp_stmt_dependency_graph[in_branch].count(INSTRUMENT_DEPEND) > 0)
                    //     continue; // its self set edges

                    has_indegree = true; // have other depends
                    break;
                }
                if (has_indegree == true)
                    break;
            }
            if (has_indegree == false)
            {
                zero_indegree_idx = i;
                break;
            }
        }
        // ------------------------------------

        // if do not has zero-indegree statement, so there is a cycle
        // randomly select a stmt, delete it and its set
        if (zero_indegree_idx == -1)
        {
            // cerr << "There is a cycle in topological_sort_path()" << endl;
            if (delete_flag != NULL)
                *delete_flag = true;

            // find the node that has the most in-edges and out-edges, and delete it
            int max_edge_num = 0;
            int target_idx = 0;
            set<int> checked_idx_for_delete;
            for (int i = 0; i < stmt_num; i++)
            {
                if (checked_idx_for_delete.count(i) > 0)
                    continue;

                auto stmt_i = stmt_id(f_txn_id_queue, i);
                if (outputted_node.count(stmt_i) > 0) // has been outputted from tmp_stmt_graph
                    continue;
                if (deleted_nodes.count(stmt_i) > 0) // has been really deleted (for decycle)
                    continue;

                set<int> i_idx_set = get_instrumented_stmt_set(i);
                int edge_num = 0;
                for (auto chosen_idx : i_idx_set)
                {
                    checked_idx_for_delete.insert(chosen_idx);
                    auto stmt_chosen_idx = stmt_id(f_txn_id_queue, chosen_idx);
                    for (int j = 0; j < stmt_num; j++)
                    {
                        if (i_idx_set.count(j) > 0) // exclude self ring
                            continue;

                        auto stmt_j = stmt_id(f_txn_id_queue, j);
                        auto in_branch = make_pair(stmt_j, stmt_chosen_idx);
                        if (tmp_stmt_dependency_graph.count(in_branch) > 0)
                            edge_num++;
                        auto out_branch = make_pair(stmt_chosen_idx, stmt_j);
                        if (tmp_stmt_dependency_graph.count(out_branch) > 0)
                            edge_num++;
                    }
                }

                if (edge_num >= max_edge_num)
                {
                    max_edge_num = edge_num;
                    target_idx = i;
                }
            }
            auto select_stmt_id = stmt_id(f_txn_id_queue, target_idx);

            // delete its set (version_set, before_read, itself, after_read)
            auto select_queue_idx = select_stmt_id.transfer_2_stmt_idx(f_txn_id_queue);
            auto select_idx_set = get_instrumented_stmt_set(select_queue_idx);
            // cerr << "Delete nodes: ";
            for (auto chosen_idx : select_idx_set)
            {
                auto chosen_stmt_id = stmt_id(f_txn_id_queue, chosen_idx);
                deleted_nodes.insert(chosen_stmt_id);
                for (int i = 0; i < stmt_num; i++)
                {
                    auto out_branch = make_pair(chosen_stmt_id, stmt_id(f_txn_id_queue, i));
                    auto in_branch = make_pair(stmt_id(f_txn_id_queue, i), chosen_stmt_id);
                    tmp_stmt_dependency_graph.erase(out_branch);
                    tmp_stmt_dependency_graph.erase(in_branch);
                }
                // cerr << chosen_stmt_id.txn_id << "." << chosen_stmt_id.stmt_idx_in_txn << ", ";
            }
            // cerr << "max_edge_num: " << max_edge_num << endl;
            continue;
        }
        // ------------------------------------

        // if do has zero-indegree statement, push the stmt and its stmt set (version_set, before_read, itself, after_read) to the path
        set<int, less<int>> zero_idx_set = get_instrumented_stmt_set(zero_indegree_idx);
        for (auto output_idx : zero_idx_set)
        {
            auto output_stmt_id = stmt_id(f_txn_id_queue, output_idx);
            path.push_back(output_stmt_id);

            // mark the outputted node, and delete its edges.
            outputted_node.insert(output_stmt_id);
            for (int j = 0; j < stmt_num; j++)
            {
                auto stmt_j = stmt_id(f_txn_id_queue, j);
                auto out_branch = make_pair(output_stmt_id, stmt_j);
                auto in_branch = make_pair(stmt_j, output_stmt_id);
                tmp_stmt_dependency_graph.erase(out_branch);
                tmp_stmt_dependency_graph.erase(in_branch);
            }
        }
    }

    auto path_size = path.size();
    // delete begin stmts and commit/abort stmts
    for (int i = 0; i < path_size; i++)
    {
        auto txn_id = path[i].txn_id;
        auto stmt_pos = path[i].stmt_idx_in_txn;
        if (stmt_pos != 0 && f_txn_size[txn_id] != stmt_pos + 1)
            continue;
        // if it is the first one (begin), or last one (commit), delete it
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    // delete replaced stmts
    for (int i = 0; i < path_size; i++)
    {
        auto queue_idx = path[i].transfer_2_stmt_idx(f_txn_id_queue);
        if (f_stmt_usage[queue_idx] != INIT_TYPE)
            continue;
        path.erase(path.begin() + i);
        path_size--;
        i--;
    }

    return path;
}

// has cycle: cycle_nodes is not empty
// no cycle: cycle_nodes is empty, and sorted_nodes is the topo-sorted txn sequence
void dependency_analyzer::check_txn_graph_cycle(set<int> &cycle_nodes, vector<int> &sorted_nodes)
{
    set<int> removed_txn;
    // remove abort txn
    for (int i = 0; i < tid_num; i++)
    {
        if (f_txn_status[i] == TXN_ABORT)
            removed_txn.insert(i);
    }
    removed_txn.insert(tid_num - 1); // remove the init txn

    cycle_nodes.clear();
    sorted_nodes.clear();
    while (removed_txn.size() < tid_num)
    {
        int zero_indegree_idx = -1;
        for (int i = 0; i < tid_num; i++)
        {
            if (removed_txn.count(i) > 0)
                continue;
            bool has_indegree = false;
            for (int j = 0; j < tid_num; j++)
            {
                if (removed_txn.count(j) > 0)
                    continue;
                if (dependency_graph[j][i].count(WRITE_READ) ||
                    dependency_graph[j][i].count(WRITE_WRITE) ||
                    dependency_graph[j][i].count(READ_WRITE) ||
                    dependency_graph[j][i].count(VERSION_SET_DEPEND) ||
                    dependency_graph[j][i].count(OVERWRITE_DEPEND))
                // if (dependency_graph[j][i].size() > 0)
                {
                    has_indegree = true;
                    break;
                }
            }
            if (has_indegree == false)
            {
                zero_indegree_idx = i;
                break;
            }
        }
        if (zero_indegree_idx == -1) // no zero indegree txn, has cycle
            break;
        removed_txn.insert(zero_indegree_idx);
        sorted_nodes.push_back(zero_indegree_idx);
    }

    for (int i = 0; i < tid_num; i++)
    {
        if (removed_txn.count(i) > 0)
            continue;
        cycle_nodes.insert(i);
    }
    return;
}

vector<vector<stmt_id>> dependency_analyzer::get_all_topo_sort_path()
{
    vector<stmt_id> path;
    auto tmp_stmt_dependency_graph = stmt_dependency_graph;
    set<stmt_id> deleted_nodes;

    // deleted_nodes include:
    //  1) the nodes that have been deleted for decycle,
    //  2) the nodes in abort stmt
    //  3) the nodes that have been deleted in transaction_test::multi_stmt_round_test

    // delete node that in abort txn
    for (int i = 0; i < stmt_num; i++)
    {
        auto txn_id = f_txn_id_queue[i];
        if (f_txn_status[txn_id] == TXN_COMMIT)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        deleted_nodes.insert(stmt_i);
        for (int j = 0; j < stmt_num; j++)
        {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            tmp_stmt_dependency_graph.erase(out_branch);
            tmp_stmt_dependency_graph.erase(in_branch);
        }
    }

    for (int i = 0; i < stmt_num; i++)
    {
        if (f_stmt_usage[i] != INIT_TYPE)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        deleted_nodes.insert(stmt_i);
        for (int j = 0; j < stmt_num; j++)
        {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            tmp_stmt_dependency_graph.erase(out_branch);
            tmp_stmt_dependency_graph.erase(in_branch);
        }
    }

    // delete start and inner dependency
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        for (int j = i; j < stmt_num; j++)
        {
            auto stmt_j = stmt_id(f_txn_id_queue, j);
            auto out_branch = make_pair(stmt_i, stmt_j);
            auto in_branch = make_pair(stmt_j, stmt_i);
            if (tmp_stmt_dependency_graph.count(out_branch))
            {
                tmp_stmt_dependency_graph[out_branch].erase(START_DEPEND);
                tmp_stmt_dependency_graph[out_branch].erase(STRICT_START_DEPEND);
                tmp_stmt_dependency_graph[out_branch].erase(INNER_DEPEND);
                if (tmp_stmt_dependency_graph[out_branch].empty())
                    tmp_stmt_dependency_graph.erase(out_branch);
            }
            if (tmp_stmt_dependency_graph.count(in_branch))
            {
                tmp_stmt_dependency_graph[in_branch].erase(START_DEPEND);
                tmp_stmt_dependency_graph[in_branch].erase(STRICT_START_DEPEND);
                tmp_stmt_dependency_graph[in_branch].erase(INNER_DEPEND);
                if (tmp_stmt_dependency_graph[in_branch].empty())
                    tmp_stmt_dependency_graph.erase(in_branch);
            }
        }
    }

    vector<stmt_id> current_path;
    vector<vector<stmt_id>> total_path;
    auto path_nodes = topological_sort_path(deleted_nodes);
    set<stmt_id> path_nodes_set;
    for (auto node : path_nodes)
        path_nodes_set.insert(node);
    for (int i = 0; i < stmt_num; i++)
    {
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        if (path_nodes_set.count(stmt_i) == 0)
            deleted_nodes.insert(stmt_i);
    }
    recur_topo_sort(current_path, deleted_nodes, total_path, tmp_stmt_dependency_graph);
    return total_path;
}

void dependency_analyzer::recur_topo_sort(vector<stmt_id> current_path,
                                          set<stmt_id> deleted_nodes,
                                          vector<vector<stmt_id>> &total_path,
                                          map<pair<stmt_id, stmt_id>, set<dependency_type>> &graph)
{
    bool flag = false;
    set<int> visited_instrument;
    for (int i = 0; i < stmt_num; i++)
    {
        if (visited_instrument.count(i) > 0)
            continue;
        auto stmt_i = stmt_id(f_txn_id_queue, i);
        if (deleted_nodes.count(stmt_i) > 0) // has been visited
            continue;
        bool has_indegree = false;

        // get its set (version_set, before_read, itself, after_read)
        // check whether the node and its set have indegree
        set<int, less<int>> i_idx_set = get_instrumented_stmt_set(i);
        for (auto chosen_idx : i_idx_set)
        {
            visited_instrument.insert(chosen_idx);
            auto stmt_chosen_idx = stmt_id(f_txn_id_queue, chosen_idx);
            for (int j = 0; j < stmt_num; j++)
            {
                if (i_idx_set.count(j) > 0) // exclude self ring
                    continue;
                auto stmt_j = stmt_id(f_txn_id_queue, j);
                if (deleted_nodes.count(stmt_j) > 0) // has been visited
                    continue;

                auto in_branch = make_pair(stmt_j, stmt_chosen_idx);
                if (graph.count(in_branch) == 0)
                    continue;
                has_indegree = true; // have other depends
                break;
            }
            if (has_indegree == true)
                break;
        }
        if (has_indegree == true)
            continue;

        // a zero indegree node and its set "i_idx_set"
        auto tmp_current_path = current_path;
        auto tmp_deleted_nodes = deleted_nodes;
        for (auto idx : i_idx_set)
        {
            auto chosen_stmt_id = stmt_id(f_txn_id_queue, idx);
            current_path.push_back(chosen_stmt_id);
            deleted_nodes.insert(chosen_stmt_id);
        }
        recur_topo_sort(current_path, deleted_nodes, total_path, graph);

        // resetting visiting
        // current_path.pop_back();
        // deleted_nodes = tmp_deleted_nodes;
        for (auto idx : i_idx_set)
        {
            auto chosen_stmt_id = stmt_id(f_txn_id_queue, idx);
            current_path.pop_back();
            deleted_nodes.erase(chosen_stmt_id);
        }
        flag = true;
    }

    if (flag == false)
    {
        total_path.push_back(current_path);
        /*
        cerr << "current path: ";
        for (auto node : current_path) {
            cerr << "(" << node.txn_id << "," << node.stmt_idx_in_txn << ")->";
        }
        cerr << endl;
        */
        if (total_path.size() % 1000 == 0)
            cerr << "total path num: " << total_path.size() << endl;
    }
    return;
}
