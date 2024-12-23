#include "general_process.hh"

extern int write_op_id;

int make_dir_error_exit(string &folder)
{
    cerr << "try to mkdir " << folder << endl;
    int fail_time = 0;
    while (mkdir(folder.c_str(), 0700))
    {
        cout << "fail to mkdir " << folder << endl;
        if (folder.length() < 2)
            return 1;
        folder = folder.substr(0, folder.length() - 1) + "_tmp/";
        fail_time++;
        if (fail_time > 5)
            return 1;
    }
    cerr << "finally mkdir " << folder << endl;
    return 0;
}

shared_ptr<schema> get_schema(dbms_info &d_info)
{
    shared_ptr<schema> schema;
    static int try_time = 0;

    try
    {
        if (false)
        {
        }

#ifdef HAVE_MYSQL
        else if (d_info.dbms_name == "mysql")
            schema = make_shared<schema_mysql>(d_info.test_db, d_info.test_port);
#endif

#ifdef HAVE_MARIADB
        else if (d_info.dbms_name == "mariadb")
            schema = make_shared<schema_mariadb>(d_info.test_db, d_info.test_port);
#endif

#ifdef HAVE_TIDB
        else if (d_info.dbms_name == "tidb")
            schema = make_shared<schema_tidb>(d_info.test_db, d_info.test_port);
#endif

        else
        {
            cerr << d_info.dbms_name << " is not supported yet" << endl;
            throw runtime_error("Unsupported DBMS");
        }
    }
    catch (exception &e)
    { // may occur occastional error
        cerr << "exception: " << e.what() << endl;
        if (try_time >= 128)
        {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
            throw e;
        }
        try_time++;
        schema = get_schema(d_info);
        try_time--;
        return schema;
    }
    return schema;
}

shared_ptr<dut_base> dut_setup(dbms_info &d_info)
{
    shared_ptr<dut_base> dut;
    if (false)
    {
    }
#ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        dut = make_shared<dut_mysql>(d_info.test_db, d_info.test_port);
#endif

#ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        dut = make_shared<dut_mariadb>(d_info.test_db, d_info.test_port);
#endif

#ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        dut = make_shared<dut_tidb>(d_info.test_db, d_info.test_port);
#endif

    else
    {
        cerr << d_info.dbms_name << " is not installed, or it is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }

    return dut;
}

int save_backup_file(string path, dbms_info &d_info)
{
    if (false)
    {
    }
#ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        return dut_mysql::save_backup_file(path);
#endif

#ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        return dut_mariadb::save_backup_file(path);
#endif

#ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        return dut_tidb::save_backup_file(path);
#endif

    else
    {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }
}

int use_backup_file(string backup_file, dbms_info &d_info)
{
    if (false)
    {
    }
#ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        return dut_mysql::use_backup_file(backup_file);
#endif

#ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        return dut_mariadb::use_backup_file(backup_file);
#endif

#ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        return dut_tidb::use_backup_file(backup_file);
#endif

    else
    {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }
}

pid_t fork_db_server(dbms_info &d_info)
{
    pid_t fork_pid;
    if (false)
    {
    }

#ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        fork_pid = dut_mysql::fork_db_server();
#endif

#ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        fork_pid = dut_mariadb::fork_db_server();
#endif

#ifdef HAVE_OCEANBASE
    else if (d_info.dbms_name == "oceanbase")
        fork_pid = dut_oceanbase::fork_db_server();
#endif

#ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        fork_pid = dut_tidb::fork_db_server();
#endif

#ifdef HAVE_MONETDB
    else if (d_info.dbms_name == "monetdb")
        fork_pid = dut_monetdb::fork_db_server();
#endif

    else
    {
        cerr << d_info.dbms_name << " is not supported yet" << endl;
        throw runtime_error("Unsupported DBMS");
    }

    return fork_pid;
}

void user_signal(int signal)
{
    if (signal != SIGUSR1)
    {
        printf("unexpect signal %d\n", signal);
        exit(1);
    }

    cerr << "get SIGUSR1, stop the thread" << endl;
    pthread_exit(0);
}

void dut_reset(dbms_info &d_info)
{
    auto dut = dut_setup(d_info);
    dut->reset();
}

void dut_backup(dbms_info &d_info)
{
    auto dut = dut_setup(d_info);
    dut->backup();
}

void dut_reset_to_backup(dbms_info &d_info)
{
    auto dut = dut_setup(d_info);
    dut->reset_to_backup();
}

void dut_get_content(dbms_info &d_info,
                     map<string, vector<vector<string>>> &content)
{
    vector<string> table_names;
    auto schema = get_schema(d_info);
    for (auto &table : schema->tables)
        table_names.push_back(table.ident());

    auto dut = dut_setup(d_info);
    dut->get_content(table_names, content);
}

void interect_test(dbms_info &d_info,
                   shared_ptr<prod> (*tmp_statement_factory)(scope *),
                   vector<string> &rec_vec,
                   bool need_affect)
{
    auto schema = get_schema(d_info);
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try
    {
        auto dut = dut_setup(d_info);
        auto sql = s.str() + ";";
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);

        if (need_affect && affect_num <= 0)
            throw runtime_error(string("affect result empty"));

        rec_vec.push_back(sql);
    }
    catch (std::exception &e)
    { // ignore runtime error
        string err = e.what();
        cerr << "err: " << e.what() << endl;
        if (err.find("syntax") != string::npos)
        {
            cerr << "\n"
                 << e.what() << "\n"
                 << endl;
            cerr << s.str() << endl;
        }
        if (try_time >= 128)
        {
            cerr << "Fail in interect_test() " << try_time << " times, return" << endl;
            throw e;
        }
        try_time++;
        interect_test(d_info, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

void normal_test(dbms_info &d_info,
                 shared_ptr<schema> &schema,
                 shared_ptr<prod> (*tmp_statement_factory)(scope *),
                 vector<string> &rec_vec,
                 bool need_affect)
{
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);

    static int try_time = 0;
    try
    {
        auto dut = dut_setup(d_info);
        auto sql = s.str() + ";";
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);

        if (need_affect && affect_num <= 0)
            throw runtime_error(string("affect result empty"));
        // cerr << sql.substr(0, sql.size() > 10 ? 10 : sql.size()) << " affect: " << affect_num << endl;
        rec_vec.push_back(sql);
    }
    catch (std::exception &e)
    { // ignore runtime error
        string err = e.what();
        if (err.find("syntax") != string::npos)
        {
            cerr << "trigger a syntax problem: " << err << endl;
            cerr << "sql: " << s.str();
        }

        if (err.find("timeout") != string::npos)
            cerr << "time out in normal test: " << err << endl;

        if (err.find("BUG") != string::npos)
        {
            cerr << "BUG is triggered in normal_test: " << err << endl;
            throw e;
        }

        if (try_time >= 128)
        {
            cerr << "Fail in normal_test() " << try_time << " times, return" << endl;
            throw e;
        }
        try_time++;
        normal_test(d_info, schema, tmp_statement_factory, rec_vec, need_affect);
        try_time--;
    }
}

static size_t BKDRHash(const char *str, size_t hash)
{
    while (size_t ch = (size_t)*str++)
    {
        hash = hash * 131 + ch; // 也可以乘以31、131、1313、13131、131313..
    }
    return hash;
}

static void hash_output_to_set(vector<vector<string>> &output, vector<size_t> &hash_set)
{
    size_t hash = 0;
    auto row_size = output.size();
    for (int i = 0; i < row_size; i++)
    {
        auto column_size = output[i].size();
        for (int j = 0; j < column_size; j++)
            hash = BKDRHash(output[i][j].c_str(), hash);
        hash_set.push_back(hash);
        hash = 0;
    }

    // sort the set, because some output order is random
    sort(hash_set.begin(), hash_set.end());
    return;
}

static void output_diff(string item_name, vector<vector<string>> &a_result, vector<vector<string>> &b_result)
{
    ofstream ofile("/tmp/comp_diff.txt", ios::app);
    ofile << "============================" << endl;
    ofile << "item name: " << item_name << endl;
    ofile << "A result: " << endl;
    for (auto &row_str : a_result)
    {
        for (auto &str : row_str)
            ofile << "    " << str;
    }
    ofile << endl;
    ofile << "B result: " << endl;
    for (auto &row_str : b_result)
    {
        for (auto &str : row_str)
            ofile << "    " << str;
    }
    ofile.close();
}

static bool is_number(const string &s)
{
    if (s.empty() || s.length() <= 0)
        return false;

    int point = 0;
    if (s.length() == 1 && (s[0] > '9' || s[0] < '0'))
        return false;

    if (s.length() > 1)
    {
        if (s[0] != '.' && (s[0] > '9' || s[0] < '0') && s[0] != '-' && s[0] != '+')
            return false;

        if (s[0] == '.')
            ++point;

        if ((s[0] == '+' || s[0] == '-') && (s[1] > '9' || s[1] < '0'))
            return false;

        for (size_t i = 1; i < s.length(); ++i)
        {
            if (s[i] != '.' && (s[i] > '9' || s[i] < '0'))
                return false;

            if (s[i] == '.')
                ++point;
        }
    }

    if (point > 1)
        return false;

    return true;
}

static bool nomoalize_content(vector<vector<string>> &content)
{
    auto size = content.size();

    for (int i = 0; i < size; i++)
    {
        auto column_num = content[i].size();
        for (int j = 0; j < column_num; j++)
        {
            auto str = content[i][j];
            double value = 0;

            if (!is_number(str) || str.find(".") == string::npos)
                continue;

            // value is a float
            value = stod(str);
            value = round(value * 100) / 100; // keep 2 number after the point
            content[i][j] = to_string(value);
        }
    }
    return true;
}

bool compare_content(map<string, vector<vector<string>>> &a_content,
                     map<string, vector<vector<string>>> &b_content)
{
    if (a_content.size() != b_content.size())
    {
        cerr << "size not equal: " << a_content.size() << " " << b_content.size() << endl;
        return false;
    }

    for (auto iter = a_content.begin(); iter != a_content.begin(); iter++)
    {
        auto &table = iter->first;
        auto &con_table_content = iter->second;

        if (b_content.count(table) == 0)
        {
            cerr << "b_content does not have " << table << endl;
            return false;
        }

        auto &seq_table_content = b_content[table];

        nomoalize_content(con_table_content);
        nomoalize_content(seq_table_content);

        vector<size_t> con_table_set, seq_table_set;
        hash_output_to_set(con_table_content, con_table_set);
        hash_output_to_set(seq_table_content, seq_table_set);

        auto size = con_table_set.size();
        if (size != seq_table_set.size())
        {
            cerr << "table " + table + " sizes are not equal" << endl;
            output_diff(table, con_table_content, seq_table_content);
            return false;
        }

        for (auto i = 0; i < size; i++)
        {
            if (con_table_set[i] != seq_table_set[i])
            {
                cerr << "table " + table + " content are not equal" << endl;
                output_diff(table, con_table_content, seq_table_content);
                return false;
            }
        }
    }

    return true;
}

bool compare_output(vector<vector<vector<string>>> &a_output,
                    vector<vector<vector<string>>> &b_output)
{
    auto size = a_output.size();
    if (size != b_output.size())
    {
        // cerr << "stmt output sizes are not equel: "<< a_output.size() << " " << b_output.size() << endl;
        return false;
    }

    for (auto i = 0; i < size; i++)
    { // for each stmt
        auto &a_stmt_output = a_output[i];
        auto &b_stmt_output = b_output[i];

        nomoalize_content(a_stmt_output);
        nomoalize_content(b_stmt_output);

        vector<size_t> a_hash_set, b_hash_set;
        hash_output_to_set(a_stmt_output, a_hash_set);
        hash_output_to_set(b_stmt_output, b_hash_set);

        size_t stmt_output_size = a_hash_set.size();
        if (stmt_output_size != b_hash_set.size())
        {
            // cerr << "stmt[" << i << "] output sizes are not equel: " << a_hash_set.size() << " " << b_hash_set.size() << endl;
            // output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++)
        {
            if (a_hash_set[j] != b_hash_set[j])
            {
                // cerr << "stmt[" << i << "] output are not equel" << endl;
                // output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
                return false;
            }
        }
    }

    return true;
}

int generate_database(dbms_info &d_info)
{
    vector<string> stage_1_rec;
    vector<string> stage_2_rec;

    cerr << "generating database ... ";
    dut_reset(d_info);

    auto ddl_stmt_num = d6() + 1; // at least 2 statements to create 2 tables
    for (auto i = 0; i < ddl_stmt_num; i++)
        interect_test(d_info, &ddl_statement_factory, stage_1_rec, false); // has disabled the not null, check and unique clause

    auto basic_dml_stmt_num = 10 + d6(); // 11-20 statements to insert data
    auto schema = get_schema(d_info);    // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++)
        normal_test(d_info, schema, &basic_dml_statement_factory, stage_2_rec, true);

    dut_backup(d_info);
    cerr << "done" << endl;
    return 0;
}

/**
 * Generate statements for one transaction.
 *
 * @param db_schema: the schema of the current database state.
 * @param trans_stmt_num: the number of statements to generate.
 * @param trans_rec: the vector to store the generated statements.
 * @param d_info: the database information.
 */
void gen_stmts_for_one_txn(shared_ptr<schema> &db_schema,
                           int trans_stmt_num,
                           vector<shared_ptr<prod>> &trans_rec,
                           dbms_info &d_info)
{
    auto can_error = d_info.can_trigger_error_in_txn;
    if (can_error == false || d_info.ouput_or_affect_num > 0)
        dut_reset_to_backup(d_info);

    vector<shared_ptr<prod>> all_tested_stmts; // if crash, report such statement
    scope scope;
    db_schema->fill_scope(scope);
    int stmt_num = 0;
    bool succeed = true;
    int fail_time = 0;
    int choice = -1;
    while (1)
    {
        if (succeed)
            choice = d12();
        else
        { // if fail, do not change choice
            fail_time++;
            if (fail_time >= 8)
            {
                choice = d12();
                fail_time = 0;
            }
        }
        shared_ptr<prod> gen = txn_statement_factory(&scope, choice);
        succeed = false;

        ostringstream stmt_stream;
        gen->out(stmt_stream);
        auto stmt = stmt_stream.str() + ";";

        if (can_error == false || d_info.ouput_or_affect_num > 0)
        {
            try
            {
                auto dut = dut_setup(d_info);
                int affect_num = 0;
                vector<vector<string>> output;
                all_tested_stmts.push_back(gen);

                dut->test(stmt, &output, &affect_num);
                if (output.size() + affect_num < d_info.ouput_or_affect_num)
                    continue;
            }
            catch (exception &e)
            {
                string err = e.what();
                if (err.find("CONNECTION FAIL") != string::npos ||
                    err.find("BUG") != string::npos)
                {

                    cerr << err << endl;
                    ofstream bug_file(NORMAL_BUG_FILE);
                    for (auto &stmt : all_tested_stmts)
                        bug_file << print_stmt_to_string(stmt) << "\n"
                                 << endl;
                    bug_file.close();
                    throw e;
                }
                // cerr << err << ", try again" << endl;
                // if (err.find("syntax") != string::npos && err.find("error") != string::npos) {
                //     cerr << RED << "The error statement: " << RESET << endl;
                //     cerr << stmt << endl;
                // }
                continue;
            }
        }
        trans_rec.push_back(gen);
        succeed = true;
        stmt_num++;
        if (stmt_num == trans_stmt_num)
            break;
    }
}

void save_current_testcase(vector<shared_ptr<prod>> &stmt_queue,
                           vector<int> &tid_queue,
                           vector<stmt_usage> &usage_queue,
                           string stmt_file_name,
                           string tid_file_name,
                           string usage_file_name,
                           string with_conn = "")
{
    // save stmt queue
    ofstream mimimized_stmt_output(stmt_file_name);
    for (int i = 0; i < stmt_queue.size(); i++)
    {
        mimimized_stmt_output << print_stmt_to_string(stmt_queue[i]) << endl;
        mimimized_stmt_output << endl;
    }
    mimimized_stmt_output.close();

    // save tid queue
    ofstream minimized_tid_output(tid_file_name);
    for (int i = 0; i < tid_queue.size(); i++)
    {
        minimized_tid_output << tid_queue[i] << endl;
    }
    minimized_tid_output.close();

    // save stmt usage queue
    ofstream minimized_usage_output(usage_file_name);
    for (int i = 0; i < usage_queue.size(); i++)
    {
        minimized_usage_output << usage_queue[i] << endl;
    }
    minimized_usage_output.close();

    if (with_conn != "")
    {
        ofstream minimized_conn_output(with_conn);

        for (int i = 0; i < stmt_queue.size(); i++)
        {
            minimized_conn_output << "conn_" << tid_queue[i] << "> " << print_stmt_to_string(stmt_queue[i]) << endl;
            mimimized_stmt_output << endl;
        }

        ofstream min_usage_readable(with_conn + "_usage_readable.txt");
        for (int i = 0; i < usage_queue.size(); i++)
        {
            min_usage_readable << stmt_basic_type_to_string(usage_queue[i].stmt_type) << endl;
        }
    }

    return;
}

void delete_txn_from_test(vector<shared_ptr<prod>> &stmt_queue,
                          vector<int> &tid_queue,
                          vector<stmt_usage> &usage_queue,
                          int tid)
{
    // Count total nr of predicate matches.
    int nr_matches = 0;
    // Count nr of bpm and apm.
    int nr_bpm = 0, nr_apm = 0;

    for (auto i : usage_queue)
    {
        if (i == PREDICATE_MATCH)
            nr_matches++;
        if (i == BEFORE_PREDICATE_MATCH)
            nr_bpm++;
        if (i == AFTER_PREDICATE_MATCH)
            nr_apm++;
    }

    assert(nr_bpm == nr_apm && nr_apm % nr_matches == 0);

    set<int> predicate_match_positions;
    for (int i = 0, poz = 0; i < (int)usage_queue.size(); i++)
    {
        if (usage_queue[i] == PREDICATE_MATCH)
        {
            if (tid_queue[i] == tid)
                predicate_match_positions.insert(poz);
            poz++;
        }
    }

    // Eliminate the right BPM
    for (int i = 0, nr_bpm = 0; i < (int)usage_queue.size(); i++)
    {
        if (usage_queue[i] == BEFORE_PREDICATE_MATCH)
        {
            if (predicate_match_positions.contains(nr_bpm))
            {
                stmt_queue.erase(stmt_queue.begin() + i);
                tid_queue.erase(tid_queue.begin() + i);
                usage_queue.erase(usage_queue.begin() + i);
                i--;
            }
            nr_bpm = (nr_bpm + 1) % nr_matches;
        }
    }
    // Eliminate the right APM
    for (int i = 0, nr_apm = 0; i < (int)usage_queue.size(); i++)
    {
        if (usage_queue[i] == AFTER_PREDICATE_MATCH)
        {
            if (predicate_match_positions.contains(nr_apm))
            {
                stmt_queue.erase(stmt_queue.begin() + i);
                tid_queue.erase(tid_queue.begin() + i);
                usage_queue.erase(usage_queue.begin() + i);
                i--;
            }
            nr_apm = (nr_apm + 1) % nr_matches;
        }
    }

    for (int i = 0; i < tid_queue.size(); i++)
    {
        if (tid_queue[i] != tid)
            continue;

        stmt_queue.erase(stmt_queue.begin() + i);
        tid_queue.erase(tid_queue.begin() + i);
        usage_queue.erase(usage_queue.begin() + i);
        i--;
    }

    for (int i = 0; i < tid_queue.size(); i++)
    {
        if (tid_queue[i] < tid)
            continue;

        tid_queue[i]--;
    }

    return;
}

bool minimize_testcase(dbms_info &d_info,
                       vector<shared_ptr<prod>> &stmt_queue,
                       vector<int> &tid_queue,
                       vector<stmt_usage> usage_queue)
{
    cerr << "Check reproduce..." << endl;
    string original_err;
    auto r_check = reproduce_routine(d_info, stmt_queue, tid_queue, usage_queue, original_err);
    if (!r_check)
    {
        cerr << "No" << endl;
        return false;
    }
    cerr << "Yes" << endl;

    int max_tid = -1;
    for (auto tid : tid_queue)
    {
        if (tid > max_tid)
            max_tid = tid;
    }
    int txn_num = max_tid + 1;

    auto final_stmt_queue = stmt_queue;
    vector<int> final_tid_queue = tid_queue;
    vector<stmt_usage> final_usage_queue = usage_queue;

    // txn level minimize
    for (int tid = 0; tid < txn_num; tid++)
    {
        cerr << "Try to delete txn " << tid << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;
        vector<stmt_usage> tmp_usage_queue = final_usage_queue;

        delete_txn_from_test(tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue, tid);

        int try_time = 1;
        bool trigger_bug = false;
        while (try_time--)
        {
            trigger_bug = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue, original_err);
            if (trigger_bug == true)
                break;
        }
        if (trigger_bug == false)
            continue;
        // auto ret = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue);
        // if (ret == false)
        //     continue;

        // reduction succeed
        cerr << "Succeed to delete txn " << tid << "\n\n\n"
             << endl;

        // int pause;
        // cerr << "Enter an integer: 0 skip, other save" << endl;
        // cin >> pause;
        // if (pause == 0)
        //     continue;

        final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        final_usage_queue = tmp_usage_queue;
        tid--;
        txn_num--;

        save_current_testcase(final_stmt_queue, final_tid_queue, final_usage_queue,
                              "min_stmts.sql", "min_tid.txt", "min_usage.txt");
    }

    // stmt level minimize
    auto stmt_num = final_tid_queue.size();
    auto dut = dut_setup(d_info);
    for (int i = 0; i < stmt_num; i++)
    {
        cerr << "Try to delete stmt " << i << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;
        vector<stmt_usage> tmp_usage_queue = final_usage_queue;
        auto tmp_stmt_num = stmt_num;

        // do not delete commit or abort
        auto tmp_stmt_str = print_stmt_to_string(tmp_stmt_queue[i]);
        if (tmp_stmt_str.find(dut->begin_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->commit_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->abort_stmt()) != string::npos)
            continue;

        // do not delete instrumented stmts
        if (tmp_usage_queue[i].is_instrumented == true)
            continue;

        auto original_i = i;

        // delete possible AFTER_WRITE_READ
        if (i + 1 <= tmp_usage_queue.size() && tmp_usage_queue[i + 1] == AFTER_WRITE_READ)
        {
            // deleting later stmt donot need to goback the "i"
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i + 1);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i + 1);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i + 1);
            tmp_stmt_num--;
        }

        // delete the statement
        tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
        tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
        tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
        tmp_stmt_num--;
        i--;

        // delete possible BEFORE_WRITE_READ and VERSION_SET_READ, note that i point the element before its original position
        while (i >= 0 && (tmp_usage_queue[i] == BEFORE_WRITE_READ ||
                          tmp_usage_queue[i] == VERSION_SET_READ))
        {
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            tmp_stmt_num--;
            i--;
        }

        // Delete predicate match, before predicate match and after predicate match.
        if (i >= 0 && tmp_usage_queue[i] == PREDICATE_MATCH)
        {
            int nr_predicates = 0;
            for (auto us : tmp_usage_queue)
                if (us == PREDICATE_MATCH)
                    nr_predicates++;
            int smaller_predicates = 0;
            for (int j = 0; j < i; j++)
                if (tmp_usage_queue[j] == PREDICATE_MATCH)
                    smaller_predicates++;

            // Eliminate the right BPM
            for (int poz = 0, nr_bpm = 0; poz < (int)tmp_usage_queue.size(); poz++)
            {
                if (tmp_usage_queue[poz] == BEFORE_PREDICATE_MATCH)
                {
                    if (nr_bpm == smaller_predicates)
                    {
                        tmp_stmt_queue.erase(tmp_stmt_queue.begin() + poz);
                        tmp_tid_queue.erase(tmp_tid_queue.begin() + poz);
                        tmp_usage_queue.erase(tmp_usage_queue.begin() + poz);
                        tmp_stmt_num--;
                        if (poz < i)
                            i--;
                        poz--;
                    }
                    nr_bpm = (nr_bpm + 1) % nr_predicates;
                }
            }
            // Eliminate the right APM
            for (int poz = 0, nr_apm = 0; poz < (int)tmp_usage_queue.size(); poz++)
            {
                if (tmp_usage_queue[poz] == AFTER_PREDICATE_MATCH)
                {
                    if (nr_apm == smaller_predicates)
                    {
                        tmp_stmt_queue.erase(tmp_stmt_queue.begin() + poz);
                        tmp_tid_queue.erase(tmp_tid_queue.begin() + poz);
                        tmp_usage_queue.erase(tmp_usage_queue.begin() + poz);
                        tmp_stmt_num--;
                        if (poz < i)
                            i--;
                        poz--;
                    }
                    nr_apm = (nr_apm + 1) % nr_predicates;
                }
            }

            if (i >= (int)tmp_usage_queue.size() || tmp_usage_queue[i] != PREDICATE_MATCH)
                cerr << "Error: i = " << i << ", size = " << tmp_usage_queue.size() << endl;
            assert(tmp_usage_queue[i] == PREDICATE_MATCH);
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            tmp_stmt_num--;
            i--;
        }

        // Delete all BEFORE_PREDICATE_MATCH and AFTER_PREDICATE_MATCH
        while (i >= 0 && tmp_usage_queue[i] == BEFORE_PREDICATE_MATCH)
        {
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            tmp_stmt_num--;
            i--;
        }
        i++;
        while (i < (int)tmp_stmt_queue.size() && tmp_usage_queue[i] == AFTER_PREDICATE_MATCH)
        {
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            tmp_stmt_num--;
        }

        int try_time = 1;
        bool trigger_bug = false;
        while (try_time--)
        {
            trigger_bug = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue, original_err);
            if (trigger_bug == true)
                break;
        }
        if (trigger_bug == false)
        {
            i = original_i;
            continue;
        }

        // reduction succeed
        cerr << "Succeed to delete stmt " << "\n\n\n"
             << endl;

        // int pause;
        // cerr << "Enter an integer: 0 skip, other save" << endl;
        // cin >> pause;
        // if (pause == 0)
        // {
        //     i = original_i;
        //     continue;
        // }

        final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        final_usage_queue = tmp_usage_queue;
        stmt_num = tmp_stmt_num;
        save_current_testcase(final_stmt_queue, final_tid_queue, final_usage_queue,
                              "min_stmts.sql", "min_tid.txt", "min_usage.txt");
    }

    if (final_stmt_queue.size() == stmt_queue.size())
        return false;

    stmt_queue = final_stmt_queue;
    tid_queue = final_tid_queue;
    usage_queue = final_usage_queue;

    save_current_testcase(stmt_queue, tid_queue, usage_queue,
                          "min_stmts.sql", "min_tid.txt", "min_usage.txt", "with_conn.sql");

    return true;
}

bool reproduce_routine(dbms_info &d_info,
                       vector<shared_ptr<prod>> &stmt_queue,
                       vector<int> &tid_queue,
                       vector<stmt_usage> usage_queue,
                       string &err_info)
{
    transaction_test::fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid : tid_queue)
    {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << ", tid queue: " << re_test.tid_queue.size() << ", stmt queue: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size())
    {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return 0;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++)
    {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++)
    {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty())
        {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }

    try
    {
        re_test.trans_test();
        // /* only check memory bugs
        shared_ptr<dependency_analyzer> tmp_da;
        if (re_test.analyze_txn_dependency(tmp_da))
        {
            string bug_str = "Find bugs in analyze_txn_dependency";
            cerr << RED << bug_str << RESET << endl;
            if (err_info != "" && err_info != bug_str)
            {
                cerr << "not same as the original bug" << endl;
                return false;
            }
            err_info = bug_str;
            return true;
        }
        return false;
    }
    catch (exception &e)
    {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        if (cur_err_info.find("INSTRUMENT_ERR") != string::npos) // it is cause by: after instrumented, the scheduling change and error in txn_test happens
            return false;
        if (err_info != "" && err_info != cur_err_info)
        {
            cerr << "not same as the original bug" << endl;
            return false;
        }
        err_info = cur_err_info;
        return true;
    }

    return false;
}

bool check_txn_cycle(dbms_info &d_info,
                     vector<shared_ptr<prod>> &stmt_queue,
                     vector<int> &tid_queue,
                     vector<stmt_usage> &usage_queue)
{
    transaction_test::fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid : tid_queue)
    {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
         << "tid_queue size: " << re_test.tid_queue.size() << endl
         << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size())
    {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return false;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++)
    {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++)
    {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty())
        {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }

    try
    {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        set<int> cycle_nodes;
        vector<int> sorted_nodes;
        tmp_da->check_txn_graph_cycle(cycle_nodes, sorted_nodes);
        if (!cycle_nodes.empty())
        {
            cerr << "Has transactional cycles" << endl;
            return true;
        }
        else
        {
            cerr << "No transactional cycle" << endl;
            return false;
        }
    }
    catch (exception &e)
    {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
    }

    return false;
}

void txn_decycle_test(dbms_info &d_info,
                      vector<shared_ptr<prod>> &stmt_queue,
                      vector<int> &tid_queue,
                      vector<stmt_usage> &usage_queue,
                      int &succeed_time,
                      int &all_time,
                      vector<int> delete_nodes)
{
    transaction_test::fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid : tid_queue)
    {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
         << "tid_queue size: " << re_test.tid_queue.size() << endl
         << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size())
    {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++)
    {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++)
    {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty())
        {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }

    try
    {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        set<int> cycle_nodes;
        vector<int> sorted_nodes;
        tmp_da->check_txn_graph_cycle(cycle_nodes, sorted_nodes);
        tmp_da->print_dependency_graph();
        if (!cycle_nodes.empty())
        { // need decycle
            for (auto txn_id : cycle_nodes)
            {
                auto new_stmt_queue = stmt_queue;
                auto new_usage_queue = usage_queue;
                int stmt_num = new_stmt_queue.size();

                // delete the txn whose id is txn_id
                for (int i = 0; i < stmt_num; i++)
                {
                    if (tid_queue[i] != txn_id)
                        continue;
                    // commit and abort stmt
                    if (usage_queue[i] == INIT_TYPE)
                        continue;
                    new_stmt_queue[i] = make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT);
                    new_usage_queue[i] = INIT_TYPE;
                    new_usage_queue[i].is_instrumented = false;
                }

                for (int tid = 0; tid < re_test.trans_num; tid++)
                {
                    re_test.trans_arr[tid].dut.reset();
                }

                delete_nodes.push_back(txn_id);
                cerr << "delete nodes: ";
                for (auto node : delete_nodes)
                    cerr << node << " ";
                cerr << endl;

                // after deleting the txn, try it again
                txn_decycle_test(d_info, new_stmt_queue, tid_queue, new_usage_queue, succeed_time, all_time, delete_nodes);
                delete_nodes.pop_back();
            }
        }
        else
        { // no cycle, perform txn sorting and check results
            vector<stmt_id> txn_stmt_path;
            for (auto txn_id : sorted_nodes)
            {
                auto txn_stmt_num = re_test.trans_arr[txn_id].stmt_num;
                for (int count = 0; count < txn_stmt_num; count++)
                {
                    auto s_id = stmt_id(txn_id, count);
                    auto stmt_idx = s_id.transfer_2_stmt_idx(tid_queue);
                    if (usage_queue[stmt_idx] == INIT_TYPE) // skip begin, commit, abort, SPACE_HOLDER_STMT
                        continue;
                    txn_stmt_path.push_back(s_id);
                }
            }

            re_test.normal_stmt_test(txn_stmt_path);
            if (re_test.check_normal_stmt_result(txn_stmt_path, false) == false)
            {
                string bug_str = "Find bugs in check_normal_stmt_result";
                cerr << RED << bug_str << RESET << endl;
                succeed_time++;
            }
            all_time++;
            cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
        }
    }
    catch (exception &e)
    {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        all_time++;
        cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
    }

    return;
}

void check_topo_sort(dbms_info &d_info,
                     vector<shared_ptr<prod>> &stmt_queue,
                     vector<int> &tid_queue,
                     vector<stmt_usage> &usage_queue,
                     int &succeed_time,
                     int &all_time)
{
    transaction_test::fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid : tid_queue)
    {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
         << "tid_queue size: " << re_test.tid_queue.size() << endl
         << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size())
    {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++)
    {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++)
    {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty())
        {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }

    try
    {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        auto all_topo_sort = tmp_da->get_all_topo_sort_path();
        cerr << "topo sort size: " << all_topo_sort.size() << endl;
        for (auto &sort : all_topo_sort)
        {
            cerr << RED << "stmt path for normal test: " << RESET;
            print_stmt_path(sort, tmp_da->stmt_dependency_graph);

            re_test.normal_stmt_output.clear();
            re_test.normal_stmt_err_info.clear();
            re_test.normal_stmt_db_content.clear();
            re_test.normal_stmt_test(sort);
            if (re_test.check_normal_stmt_result(sort, false) == false)
            {
                succeed_time++;
            }
            all_time++;
            cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << "/" << all_topo_sort.size() << endl;
        }
    }
    catch (exception &e)
    {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        all_time++;
        cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
    }

    return;
}

string print_stmt_to_string(shared_ptr<prod> stmt)
{
    ostringstream stmt_stream;
    stmt->out(stmt_stream);
    return stmt_stream.str() + ";";
}
