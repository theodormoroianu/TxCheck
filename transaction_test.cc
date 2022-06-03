#include "transaction_test.hh"

#define MAX_CONCURRENT_TXN_NUM  5

void transaction_test::assign_txn_id()
{
    set<int> concurrent_tid;
    set<int> available_tid;
    int tid_insertd_stmt[trans_num];
    for (int i = 0; i < trans_num; i++) {
        available_tid.insert(i);
        tid_insertd_stmt[i] = 0;
    }

    while (available_tid.empty() == false) {
        int tid;
        if (concurrent_tid.size() < MAX_CONCURRENT_TXN_NUM) {
            auto idx = dx(available_tid.size()) - 1;
            tid = *next(available_tid.begin(), idx);
            concurrent_tid.insert(tid);
        }
        else {
            auto idx = dx(concurrent_tid.size()) - 1;
            tid = *next(concurrent_tid.begin(), idx);
        }

        tid_queue.push_back(tid);
        tid_insertd_stmt[tid]++;
        if (tid_insertd_stmt[tid] >= trans_arr[tid].stmt_num) {
            available_tid.erase(tid);
            concurrent_tid.erase(tid);
        }
    }

    return;
}

void transaction_test::assign_txn_status()
{   
    for (int i = 0; i < commit_num; i++) 
        trans_arr[i].status = TXN_COMMIT;

    for (int i = commit_num; i < trans_num; i++) 
        trans_arr[i].status = TXN_ABORT;
    
    cerr << YELLOW << "show status" << RESET << endl;
    for (int i = 0; i < trans_num; i++) {
        cerr << i << " " << trans_arr[i].status << endl;
    }

    return;
}

void transaction_test::gen_txn_stmts()
{    
    int stmt_pos_of_trans[trans_num];

    db_schema = get_schema(test_dbms_info);
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].dut = dut_setup(test_dbms_info);
        stmt_pos_of_trans[tid] = 0;
        
        // save 2 stmts for begin and commit/abort
        smith::rng.seed(time(NULL));
        gen_stmts_for_one_txn(db_schema, trans_arr[tid].stmt_num - 2, trans_arr[tid].stmts, test_dbms_info);
        // insert begin and end stmts
        trans_arr[tid].stmts.insert(trans_arr[tid].stmts.begin(), 
                make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->begin_stmt()));
        if (trans_arr[tid].status == TXN_COMMIT) 
            trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->commit_stmt()));
        else 
            trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
    }

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = trans_arr[tid].stmts[stmt_pos_of_trans[tid]];
        stmt_queue.push_back(stmt);
        stmt_use.push_back(INIT_TYPE);
        stmt_pos_of_trans[tid]++;
    }
}

void transaction_test::instrument_txn_stmts()
{
    instrumentor i(stmt_queue, tid_queue, db_schema);
    stmt_queue = i.final_stmt_queue;
    tid_queue = i.final_tid_queue;
    stmt_use = i.final_stmt_usage;
    stmt_num = stmt_queue.size();

    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmts.clear();

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = stmt_queue[i];
        trans_arr[tid].stmts.push_back(stmt);
    }

    for (int tid = 0; tid < trans_num; tid++) 
        trans_arr[tid].stmt_num = trans_arr[tid].stmts.size();
}

void transaction_test::clean_instrument()
{
    vector<shared_ptr<prod>> clean_stmt_queue;
    vector<int> clean_tid_queue;
    vector<stmt_usage> clean_stmt_usage_queue;

    for (int i = 0; i < stmt_num; i++) {
        if (stmt_use[i] == BEFORE_WRITE_READ || 
                stmt_use[i] == AFTER_WRITE_READ ||
                stmt_use[i] == VERSION_SET_READ)
            continue;
        clean_stmt_queue.push_back(stmt_queue[i]);
        clean_tid_queue.push_back(tid_queue[i]);
        clean_stmt_usage_queue.push_back(stmt_use[i]);
    }
    stmt_num = clean_stmt_queue.size();
    stmt_queue = clean_stmt_queue;
    tid_queue = clean_tid_queue;
    stmt_use = clean_stmt_usage_queue;

    for (int tid = 0; tid < trans_num; tid++)
        trans_arr[tid].stmts.clear();

    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto &stmt = stmt_queue[i];
        trans_arr[tid].stmts.push_back(stmt);
    }

    for (int tid = 0; tid < trans_num; tid++) 
        trans_arr[tid].stmt_num = trans_arr[tid].stmts.size();
}

vector<int> transaction_test::get_longest_path_from_graph(shared_ptr<dependency_analyzer>& da)
{
    auto longest_path = da->PL2_longest_path();
    if (longest_path.empty() == false)
        longest_path.erase(longest_path.begin());
    
    cerr << "longest_path: ";
    for (int i = 0; i < longest_path.size(); i++)
        cerr << longest_path[i] << " ";
    cerr << endl;

    return longest_path;
}

// true: changed
// false: no need to change
bool transaction_test::change_txn_status(int target_tid, txn_status final_status)
{
    if (final_status != TXN_ABORT && final_status != TXN_COMMIT) {
        cerr << "[change_txn_status] illegal final_status: " << final_status << endl;
        throw runtime_error("illegal final_status");
    }
    
    if (trans_arr[target_tid].status == final_status) 
        return false;
    
    trans_arr[target_tid].status = final_status;
    trans_arr[target_tid].stmts.pop_back();
    if (final_status == TXN_ABORT)
        trans_arr[target_tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[target_tid].dut->abort_stmt()));
    else if (final_status == TXN_COMMIT)
        trans_arr[target_tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[target_tid].dut->commit_stmt()));
    
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto stmt = print_stmt_to_string(stmt_queue[i]);

        if (target_tid != tid)
            continue;
        if (stmt.find(trans_arr[tid].dut->commit_stmt()) == string::npos &&
                stmt.find(trans_arr[tid].dut->abort_stmt()) == string::npos)
            continue;
        
        // find out the commit and abort stmt
        stmt_queue[i] = trans_arr[tid].stmts.back();
    }
    return true;
}

bool transaction_test::analyze_txn_dependency(shared_ptr<dependency_analyzer>& da)
{
    vector<stmt_output> init_content_vector;
    for (auto iter = init_db_content.begin(); iter != init_db_content.end(); iter++)
        init_content_vector.push_back(iter->second);
    
    vector<txn_status> real_txn_status;
    for (int tid = 0; tid < trans_num; tid++) 
        real_txn_status.push_back(trans_arr[tid].status);

    da = make_shared<dependency_analyzer>(init_content_vector, // init_output 
                            real_output_queue, // total_output
                            real_tid_queue, // final_tid_queue
                            real_stmt_usage, // final_stmt_usage
                            real_txn_status, // final_txn_status
                            trans_num, // t_num
                            1, // primary_key_idx
                            0); // write_op_key_idx

    cerr << "check_G1a ...!!" << endl;
    if (da->check_G1a() == true) {
        cerr << "check_G1a violate!!" << endl;
        return true;
    }
    cerr << "check_G1b ...!!" << endl;
    if (da->check_G1b() == true){
        cerr << "check_G1b violate!!" << endl;
        return true;
    }
    cerr << "check_G1c ...!!" << endl;
    if (da->check_G1c() == true){
        cerr << "check_G1c violate!!" << endl;
        return true;
    }
    // cerr << "check_G2_item ...!!" << endl;
    // if (da->check_G2_item() == true){
    //     cerr << "check_G2_item violate!!" << endl;
    //     return true;
    // }
    // cerr << "check_GSIa ...!!" << endl;
    // if (da->check_GSIa() == true){
    //     cerr << "check_GSIa violate!!" << endl;
    //     return true;
    // }
    // cerr << "check_GSIb ...!!" << endl;
    // if (da->check_GSIb() == true){
    //     cerr << "check_GSIb violate!!" << endl;
    //     return true;
    // }

    longest_seq_txn_order = get_longest_path_from_graph(da);
    
    return false;
}

void transaction_test::clear_execution_status()
{
    for (int tid = 0; tid < trans_num; tid++) {
        trans_arr[tid].stmt_err_info.clear();
        trans_arr[tid].stmt_outputs.clear();
        trans_arr[tid].dut = dut_setup(test_dbms_info);
        trans_arr[tid].is_blocked = false;

        // clear the normal execution result
        trans_arr[tid].normal_stmts.clear();
        trans_arr[tid].normal_outputs.clear();
        trans_arr[tid].normal_err_info.clear();
    }
    init_db_content.clear();

    real_tid_queue.clear();
    real_stmt_queue.clear();
    real_output_queue.clear();
    real_stmt_usage.clear();
    trans_db_content.clear();

    longest_seq_txn_order.clear();
    normal_db_content.clear();

    // normal test related
    normal_stmt_output.clear();
    normal_stmt_err_info.clear();
    normal_stmt_db_content.clear();
}

bool transaction_test::refine_txn_as_txn_order()
{
    set<int> seq_txn;
    for (int i = 0; i < longest_seq_txn_order.size(); i++)
        seq_txn.insert(longest_seq_txn_order[i]);
    
    bool is_refined = false;
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        auto stmt = print_stmt_to_string(stmt_queue[i]);

        if (trans_arr[tid].status == TXN_ABORT)
            continue;
        if (seq_txn.count(tid) > 0)
            continue;
        if (stmt.find(trans_arr[tid].dut->commit_stmt()) == string::npos) // it is not commit stmt
            continue;

        is_refined = true;
        // should be change to abort
        trans_arr[tid].status = TXN_ABORT;
        trans_arr[tid].stmts.pop_back();
        trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
        stmt_queue[i] = trans_arr[tid].stmts.back();
    }

    if (is_refined == false)
        return false;
    
    clear_execution_status();
    return true;
}

// 2: fatal error (e.g. restart transaction, current transaction is aborted), skip the stmt
// 1: executed
// 0: blocked, not executed
int transaction_test::trans_test_unit(int stmt_pos, stmt_output& output, bool debug_mode)
{
    auto tid = tid_queue[stmt_pos];
    auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);

    auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
    replace(show_str.begin(), show_str.end(), '\n', ' ');
    
    try {
        trans_arr[tid].dut->test(stmt, &output);
        trans_arr[tid].stmt_outputs.push_back(output);
        trans_arr[tid].stmt_err_info.push_back("");
        if (debug_mode)
            cerr << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size() - 1 << ": " << show_str << endl;
        return 1;
    } catch(exception &e) {
        string err = e.what();
        // if (debug_mode)
            cerr << RED << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size()  << ": " << show_str << ": fail, err: " << err << RESET << endl;

        if (err.find("ost connection") != string::npos) // lost connection
            throw e;
        if (err.find("blocked") != string::npos)
            return 0;
        if (err.find("skipped") != string::npos) {
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back("");
            return 2;
        }
            
        if (err.find("sent sql stmt changed") != string::npos) 
            exit(-1);
        
        // store the error info of non-commit statement
        if (stmt.find(trans_arr[tid].dut->commit_stmt()) == string::npos) {
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back(err);
            return 1;
        }
        
        // is commit stmt   
        // if commit fail, just abort
        trans_arr[tid].status = TXN_ABORT;
        trans_arr[tid].stmts.pop_back();
        trans_arr[tid].stmts.push_back(make_shared<txn_string_stmt>((prod *)0, trans_arr[tid].dut->abort_stmt()));
        stmt_queue[stmt_pos] = trans_arr[tid].stmts.back();

        stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
        try {
            trans_arr[tid].dut->test(stmt);
            stmt_output empty_output;
            output = empty_output;
            trans_arr[tid].stmt_outputs.push_back(empty_output);
            trans_arr[tid].stmt_err_info.push_back("");
            if (debug_mode)
                cerr << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size() - 1  << ": " << show_str << endl;
            return 1;
        } catch(exception &e2) {
            err = e2.what();
            if (debug_mode)
                cerr << RED << "T" << tid << " S" << trans_arr[tid].stmt_outputs.size()  << ": " << show_str << ": fail, err: " << err << RESET << endl;
        }
    }

    return 0;
}

void transaction_test::retry_block_stmt(int cur_stmt_num, shared_ptr<int[]> status_queue, bool debug_mode)
{
    if (debug_mode)
        cerr << YELLOW << "retrying process begin..." << RESET << endl;

    // firstly try the first stmt of each blocked transaction
    set<int> first_tried_tid;
    for (int i = 0; i < cur_stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        auto tid = tid_queue[i];
        if (trans_arr[tid].is_blocked == false)
            continue;

        if (first_tried_tid.count(tid) != 0) // have tried
            continue;
        
        first_tried_tid.insert(tid);
        stmt_output output;
        auto is_executed = trans_test_unit(i, output, debug_mode);
        if (is_executed == 1) { // executed
            trans_arr[tid].is_blocked = false;
            status_queue[i] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[i]);
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_use[i]);
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(INIT_TYPE);
            status_queue[i] = 1;
        } else {// blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    
    auto is_serializable = test_dbms_info.serializable;
    for (int stmt_pos = 0; stmt_pos < cur_stmt_num; stmt_pos++) {
        auto tid = tid_queue[stmt_pos];
        // skip the tried but still blocked transaction
        if (trans_arr[tid].is_blocked)
            continue;
        
        // skip the executed stmt
        if (status_queue[stmt_pos] == 1)
            continue;

        stmt_output output;
        auto is_executed = trans_test_unit(stmt_pos, output, debug_mode);
        // successfully execute the stmt, so label as not blocked
        if (is_executed == 1) {
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(stmt_queue[stmt_pos]);
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(stmt_use[stmt_pos]);
            
            auto stmt = print_stmt_to_string(stmt_queue[stmt_pos]);
            if (stmt.find(trans_arr[tid].dut->commit_stmt()) != string::npos ||
                    stmt.find(trans_arr[tid].dut->abort_stmt()) != string::npos) {
                retry_block_stmt(stmt_pos, status_queue, debug_mode);
            }
        } else if (is_executed == 2) { // skipped
            trans_arr[tid].is_blocked = false;
            status_queue[stmt_pos] = 1;

            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(INIT_TYPE);
        }
        else { // still blocked
            trans_arr[tid].is_blocked = true;
        }
    }
    if (debug_mode)
        cerr << YELLOW << "retrying process end..." << RESET << endl;
}

void transaction_test::trans_test(bool debug_mode)
{
    dut_reset_to_backup(test_dbms_info);
    dut_get_content(test_dbms_info, init_db_content); // get initial database content
    
    if (debug_mode)
        cerr << YELLOW << "transaction test" << RESET << endl;
    // status_queue: 0 -> blocked, 1->executed (succeed or fail)
    shared_ptr<int[]> status_queue(new int[stmt_num]);
    
    for (int i = 0; i < stmt_num; i++) 
        status_queue[i] = 0;
    
    for (int stmt_index = 0; stmt_index < stmt_num; stmt_index++) {
        auto tid = tid_queue[stmt_index];
        auto& stmt = stmt_queue[stmt_index];
        auto su = stmt_use[stmt_index];
        
        if (trans_arr[tid].is_blocked)
            continue;
        
        stmt_output output;
        auto is_executed = trans_test_unit(stmt_index, output, debug_mode);
        if (is_executed == 0) {
            trans_arr[tid].is_blocked = true;
            continue;
        }
        if (is_executed == 2) { // the executed stmt fail
            status_queue[stmt_index] = 1;
            real_tid_queue.push_back(tid);
            real_stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT));
            real_output_queue.push_back(output);
            real_stmt_usage.push_back(INIT_TYPE);
            continue;
        }
        status_queue[stmt_index] = 1;
        real_tid_queue.push_back(tid);
        real_stmt_queue.push_back(stmt);
        real_output_queue.push_back(output);
        real_stmt_usage.push_back(su);
        
        // after a commit or abort, retry the statement
        auto stmt_str = print_stmt_to_string(stmt);
        if (stmt_str.find(trans_arr[tid].dut->commit_stmt()) != string::npos ||
                stmt_str.find(trans_arr[tid].dut->abort_stmt()) != string::npos) {
            retry_block_stmt(stmt_index, status_queue, debug_mode);
        }
    }

    while (1) {
        int old_executed = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (status_queue[i] == 1)
                old_executed++;
        }

        retry_block_stmt(stmt_num, status_queue, debug_mode);
        
        int new_executed = 0;
        for (int i = 0; i < stmt_num; i++) {
            if (status_queue[i] == 1)
                new_executed++;
        }

        if (old_executed == new_executed)
            break;
    }
    
    for (int i = 0; i < stmt_num; i++) {
        if (status_queue[i] == 1)
            continue;
        
        cerr << RED << "something error, some stmt is still not executed" << RESET << endl;
        throw runtime_error("some stmt is still not executed");
    }

    if (real_stmt_queue.size() != stmt_num) {
        cerr << "real_stmt_queue size: " << real_stmt_queue.size() << endl;
        cerr << "stmt_num: " << stmt_num << endl;
        cerr << "real_stmt_queue size is not equal to stmt_queue size, something wrong" << endl;
        throw runtime_error("real_stmt_queue size is not equal to stmt_queue size, something wrong");
    }

    // collect database information
    dut_get_content(test_dbms_info, trans_db_content);
}

void transaction_test::save_test_case(string dir_name)
{
    cerr << RED << "Saving test cases..." << RESET;
    // save stmt queue
    string total_stmts_file = dir_name + "stmts.sql";
    ofstream total_stmt_output(total_stmts_file);
    for (int i = 0; i < stmt_num; i++) {
        total_stmt_output << print_stmt_to_string(stmt_queue[i]) << endl;
        total_stmt_output << endl;
    }
    total_stmt_output.close();

    // save tid queue
    string total_tid_file = dir_name + "tid.txt";
    ofstream total_tid_output(total_tid_file);
    for (int i = 0; i < stmt_num; i++) {
        total_tid_output << tid_queue[i] << endl;
    }
    total_tid_output.close();

    // save stmt use queue
    string total_stmt_use_file = dir_name + "stmt_use.txt";
    ofstream total_stmt_use_output(total_stmt_use_file);
    for (int i = 0; i < stmt_num; i++) {
        total_stmt_use_output << stmt_use[i] << endl;
    }
    total_stmt_use_output.close();

    cerr << RED << "done" << RESET << endl;
}

int transaction_test::record_bug_num = 0;
pid_t transaction_test::server_process_id = 0xabcde;

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

void kill_process_with_SIGTERM(pid_t process_id)
{
    kill(process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    while (1) {
        ret = kill(process_id, 0);
        if (ret != 0)
            break;
        
        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS)
            break;
    }
}

bool transaction_test::try_to_kill_server()
{
    cerr << "try killing the server..." << endl;
    kill(server_process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    bool flag = false;
    while (1) {
        ret = kill(server_process_id, 0);
        if (ret != 0) { // the process die
            flag = true;
            break;
        }

        int status;
        auto res = waitpid(server_process_id, &status, WNOHANG);
        if (res < 0) {
            cerr << "waitpid() fail: " <<  res << endl;
            throw runtime_error(string("waitpid() fail"));
        }
        if (res == server_process_id) { // the dead process is collected
            cerr << "waitpid succeed for the server process !!!" << endl;
            flag = true;
            break;
        }

        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS) {
            flag = false;
            break;
        }
    }
    return flag;
}

bool transaction_test::fork_if_server_closed()
{
    bool server_restart = false;
    auto time_begin = get_cur_time_ms();

    while (1) {
        try {
            auto dut = dut_setup(test_dbms_info);
            if (server_restart)
                sleep(3);
            break; // connect successfully, so break;
        
        } catch (exception &e) { // connect fail
            auto ret = kill(server_process_id, 0);
            if (ret != 0) { // server has die
                cerr << "testing server die, restart it" << endl;

                while (try_to_kill_server() == false) {} // just for safe
                server_process_id = fork_db_server(test_dbms_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }

            auto time_end = get_cur_time_ms();
            if (time_end - time_begin > WAIT_FOR_PROC_TIME_MS) {
                cerr << "testing server hang, kill it and restart" << endl;
                
                while (try_to_kill_server() == false) {}
                server_process_id = fork_db_server(test_dbms_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }
        }
    }

    return server_restart;
}

void transaction_test::normal_test()
{
    // get normal execute statement
    int real_stmt_num = real_tid_queue.size();
    for (int i = 0; i < real_stmt_num; i++) {
        auto real_tid = real_tid_queue[i];
        if (trans_arr[real_tid].status != TXN_COMMIT)
            continue;
        
        auto real_stmt = real_stmt_queue[i];
        trans_arr[real_tid].normal_stmts.push_back(real_stmt);
    }

    for (int tid = 0; tid < trans_num; tid++) {
        // if it is commit, erase "begin" and "commit"
        if (trans_arr[tid].status == TXN_COMMIT) {
            trans_arr[tid].normal_stmts.erase(trans_arr[tid].normal_stmts.begin());
            trans_arr[tid].normal_stmts.pop_back();
        }
    }

    dut_reset_to_backup(test_dbms_info);
    auto normal_dut = dut_setup(test_dbms_info);

    for (auto tid : longest_seq_txn_order) {
        vector<vector<vector<string>>> normal_output;
        vector<string> normal_err_info;

        auto normal_stmt_num = trans_arr[tid].normal_stmts.size();
        for (int i = 0; i < normal_stmt_num; i++) {
            auto stmt = print_stmt_to_string(trans_arr[tid].normal_stmts[i]);
            auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
            replace(show_str.begin(), show_str.end(), '\n', ' ');
            vector<vector<string>> output;
            try {
                normal_dut->test(stmt, &output);
                normal_output.push_back(output);
                cerr << "T" << tid << ": " << show_str << endl;
            } catch (exception &e) {
                string err = e.what();
                cerr << RED 
                    << "T" << tid << ": " << show_str << ": fail, err: " 
                    << err << RESET << endl;
                stmt_output empty_output;
                normal_output.push_back(empty_output);
                normal_err_info.push_back(err);
            }
        }
        vector<vector<string>> empty_output;
        normal_output.insert(normal_output.begin(), empty_output); // for begin stmt
        normal_output.push_back(empty_output); // for commit stmt
        trans_arr[tid].normal_outputs = normal_output;
        trans_arr[tid].normal_err_info = normal_err_info;
    }
    dut_get_content(test_dbms_info, normal_db_content);
}

bool transaction_test::check_txn_normal_result()
{
    if (!compare_content(trans_db_content, normal_db_content)) {
        cerr << "trans_db_content is not equal to normal_db_content" << endl;
        return false;
    }

    for (auto i = 0; i < trans_num; i++) {
        if (trans_arr[i].stmt_num <= 2) // just ignore the 0 stmts, and the one only have begin, commit
            continue;
        if (trans_arr[i].status != TXN_COMMIT) // donot check abort transaction
            continue;
        
        if (!compare_output(trans_arr[i].stmt_outputs, trans_arr[i].normal_outputs)) {
            cerr << "trans "<< i << " output is not equal to normal one" << endl;
            return false;
        }

        if (trans_arr[i].stmt_err_info.size() != trans_arr[i].normal_err_info.size()) {
            cerr << "trans "<< i << " error num is not equal to normal one" << endl;
            return false;
        }
    }

    return true;
}

bool transaction_test::multi_round_test()
{
    trans_test(); // first run, get all dependency information
    shared_ptr<dependency_analyzer> init_da;
    if (analyze_txn_dependency(init_da)) 
        throw runtime_error("BUG: found in analyze_txn_dependency()");
    txn_status init_status[trans_num];
    for (int tid = 0; tid < trans_num; tid++) 
        init_status[tid] = trans_arr[tid].status;
    
    while (1) {
        // use the longest path to refine
        cerr << "\n\n";
        cerr << RED << "one round test" << RESET << endl;
        cerr << "ideal test path: ";
        for (auto i:longest_seq_txn_order) 
            cerr << i << " ";
        cerr << endl;
        auto ideal_test_path = longest_seq_txn_order;

        shared_ptr<dependency_analyzer> tmp_da;
        while (refine_txn_as_txn_order() == true) { // if not stable
            trans_test();
            if (analyze_txn_dependency(tmp_da)) 
                throw runtime_error("BUG: found in analyze_txn_dependency()");
        }

        cerr << "real test path: ";
        for (auto i:longest_seq_txn_order) 
            cerr << i << " ";
        cerr << endl;
        auto real_test_path = longest_seq_txn_order;
        
        normal_test();
        if (check_txn_normal_result() == false)
            return true;
        
        // after executing the possible longest path, delete the edge in init_da
        auto& graph = init_da->dependency_graph;
        // delete the real test one
        int path_length = real_test_path.size();
        for (int i = 0; i + 1 < path_length; i++) {
            auto cur_tid = real_test_path[i];
            auto next_tid = real_test_path[i + 1];

            // delete all the dependency edges except START_DEPEND or STRICT_START_DEPEND
            graph[cur_tid][next_tid].erase(WRITE_READ);
            graph[cur_tid][next_tid].erase(WRITE_WRITE);
            graph[cur_tid][next_tid].erase(READ_WRITE);
        }
        // delete the ideal test one, so that will not choose this path again
        path_length = ideal_test_path.size();
        for (int i = 0; i + 1 < path_length; i++) {
            auto cur_tid = ideal_test_path[i];
            auto next_tid = ideal_test_path[i + 1];

            // delete all the dependency edges except START_DEPEND or STRICT_START_DEPEND
            graph[cur_tid][next_tid].erase(WRITE_READ);
            graph[cur_tid][next_tid].erase(WRITE_WRITE);
            graph[cur_tid][next_tid].erase(READ_WRITE);
        }

        // after deleting, get another longest path
        longest_seq_txn_order = get_longest_path_from_graph(init_da);
        // check whether the path contains conflict depend
        path_length = longest_seq_txn_order.size();
        bool has_conflict_depend = false;
        for (int i = 0; i + 1 < path_length; i++) {
            auto cur_tid = longest_seq_txn_order[i];
            auto next_tid = longest_seq_txn_order[i + 1];

            if (graph[cur_tid][next_tid].count(WRITE_READ) || 
                    graph[cur_tid][next_tid].count(WRITE_WRITE) ||
                    graph[cur_tid][next_tid].count(READ_WRITE)) {
                
                has_conflict_depend = true;
                break;
            }
        }
        // if there is not conflict depend on longest path, stop test
        if (has_conflict_depend == false)
            break;
        
        // change back the txn status to init one
        for (int tid = 0; tid < trans_num; tid++) 
            change_txn_status(tid, init_status[tid]);
    }
    return false;
}

bool transaction_test::refine_stmt_queue(vector<stmt_id>& stmt_path)
{
    // use real one to replace 
    // stmt_queue = real_stmt_queue;
    // tid_queue = real_tid_queue;
    // stmt_use = real_stmt_usage;

    // refine txn_stmt because the skipped stmt has been changed
    int stmt_pos_of_txn[trans_num];
    for (int i = 0; i < trans_num; i++) 
        stmt_pos_of_txn[i] = 0;
    for (int i = 0; i < stmt_num; i++) {
        auto casted = dynamic_pointer_cast<txn_string_stmt>(stmt_queue[i]);
        auto tid = tid_queue[i];
        if (casted.use_count() > 0) 
            trans_arr[tid].stmts[stmt_pos_of_txn[tid]] = casted;
        stmt_pos_of_txn[tid]++;
    }
    
    bool is_refined = false;
    // change txns that are not in the path to abort
    auto path_length = stmt_path.size();
    set<int> exist_tid;
    set<stmt_id> exist_stmt;
    for (int i = 0; i < path_length; i++) {
        exist_tid.insert(stmt_path[i].txn_id);
        exist_stmt.insert(stmt_path[i]);
    }
    for (int i = 0; i < trans_num; i++) {
        if (exist_tid.count(i) > 0)
            continue;
        if (change_txn_status(i, TXN_ABORT))
            is_refined = true;
    }

    for (int i = 0; i < trans_num; i++) 
        stmt_pos_of_txn[i] = 0;
    for (int i = 0; i < stmt_num; i++) {
        auto tid = tid_queue[i];
        if (exist_tid.count(tid) == 0) 
            continue;
        auto stmt_pos = stmt_pos_of_txn[tid];
        stmt_id stmt_idx(tid, stmt_pos);
        stmt_pos_of_txn[tid]++;
        
        if (exist_stmt.count(stmt_idx) > 0) // matched
            continue;

        auto casted = dynamic_pointer_cast<txn_string_stmt>(stmt_queue[i]);
        if (casted.use_count() > 0) // already been replaced, include commit and abort stmt
            continue;
        
        cerr << RED << "Refining: " << stmt_idx.txn_id << "." << stmt_idx.stmt_idx_in_txn << RESET << endl;

        is_refined = true;
        // txn in the path, and stmt not match, should change to SELECT 1 WHERE FALSE
        stmt_queue[i] = make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT);
        stmt_use[i] = INIT_TYPE;
    }
    
    if (is_refined == false)
        return false;
    
    clear_execution_status();
    return true;
}

void transaction_test::normal_stmt_test(vector<stmt_id>& stmt_path)
{
    cerr << "normal testing" << endl;
    dut_reset_to_backup(test_dbms_info);
    auto normal_dut = dut_setup(test_dbms_info);
    for (auto& stmt_id : stmt_path) {
        auto tid = stmt_id.txn_id;
        auto stmt_pos = stmt_id.stmt_idx_in_txn;
        auto stmt = print_stmt_to_string(trans_arr[tid].stmts[stmt_pos]);
        auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
        replace(show_str.begin(), show_str.end(), '\n', ' ');
        stmt_output output;
        try {
            normal_dut->test(stmt, &output);
            normal_stmt_output.push_back(output);
            normal_stmt_err_info.push_back("");
            cerr << "T" << tid << "P" << stmt_pos << ": " << show_str << endl;
        } catch (exception &e) {
            string err = e.what();
            stmt_output empty_output;
            cerr << RED 
                << "T" << tid << "P" << stmt_pos << ": " << show_str << ": fail, err: " 
                << err << RESET << endl;
            if (err.find("skipped") != string::npos) {
                normal_stmt_output.push_back(empty_output);
                normal_stmt_err_info.push_back("");
                continue;
            }
            normal_stmt_output.push_back(empty_output);
            normal_stmt_err_info.push_back(err);
        }
    }
    dut_get_content(test_dbms_info, normal_stmt_db_content);
}

bool transaction_test::check_normal_stmt_result(vector<stmt_id>& stmt_path)
{
    if (!compare_content(trans_db_content, normal_stmt_db_content)) {
        cerr << "trans_db_content is not equal to normal_stmt_db_content" << endl;
        return false;
    }

    vector<stmt_output> path_txn_output;
    vector<string> path_txn_err_info;

    auto path_length = stmt_path.size();
    for (int i = 0; i < path_length; i++) {
        auto tid = stmt_path[i].txn_id;
        auto stmt_pos = stmt_path[i].stmt_idx_in_txn;
        path_txn_output.push_back(trans_arr[tid].stmt_outputs[stmt_pos]);
        path_txn_err_info.push_back(trans_arr[tid].stmt_err_info[stmt_pos]);
    }

    if (!compare_output(path_txn_output, normal_stmt_output)) {
        cerr << "txn output is not equal to normal stmt one" << endl;
        return false;
    }

    auto err_info_size = path_txn_err_info.size();
    if (err_info_size != normal_stmt_err_info.size()) {
        cerr << "txn error info size is not equal to normal stmt one" << endl;
        cerr << "path_txn_err_info: " << err_info_size << ", normal_stmt_err_info" << normal_stmt_err_info.size() << endl;
        return false;
    }
    bool err_result = true;
    for (int i = 0; i < err_info_size; i++) {
        // cerr << "err idx: " << i << endl;
        // cerr << "txn one: " << path_txn_err_info[i] << endl;
        // cerr << "normal one: " << normal_stmt_err_info[i] << endl;
        
        if (path_txn_err_info[i] != normal_stmt_err_info[i]) {
            if (path_txn_err_info[i] != "" && normal_stmt_err_info[i] != "") // both has error, the content could be different
                continue;
            cerr << "txn error info is not equal to normal stmt one, idx: " << i << endl;
            cerr << "txn one: " << path_txn_err_info[i] << endl;
            cerr << "normal one: " << normal_stmt_err_info[i] << endl;
            err_result = false;
        }
    }

    return err_result;
}

bool transaction_test::multi_stmt_round_test()
{
    instrument_txn_stmts();
    trans_test(); // first run, get all dependency information
    shared_ptr<dependency_analyzer> init_da;
    if (analyze_txn_dependency(init_da)) 
        throw runtime_error("BUG: found in analyze_txn_dependency()");
    auto longest_stmt_path = init_da->longest_stmt_path();
    
    // record init status
    auto init_stmt_queue = stmt_queue;
    auto init_tid_queue = tid_queue;
    auto init_stmt_usage = stmt_use;
    txn_status init_txn_status[trans_num];
    vector<shared_ptr<prod>> init_txn_stmt[trans_num];
    for (int tid = 0; tid < trans_num; tid++) {
        init_txn_status[tid] = trans_arr[tid].status;
        init_txn_stmt[tid] = trans_arr[tid].stmts;
    }

    while (1) {
        cerr << "\n\n";
        cerr << RED << "one round test" << RESET << endl;
        cerr << "ideal test stmt path: ";
        for (int i = 0; i < longest_stmt_path.size(); i++) {
            auto& cur_sid = longest_stmt_path[i];
            cerr << "(" << cur_sid.txn_id << "." << cur_sid.stmt_idx_in_txn << ")" << "-";
            if (i + 1 < longest_stmt_path.size()) {
                auto& next_sid = longest_stmt_path[i + 1];
                auto branch = make_pair<>(cur_sid, next_sid);
                auto& dset = init_da->stmt_dependency_graph[branch];
                if (dset.count(WRITE_READ))
                    cerr << RED << "0" << RESET;
                if (dset.count(WRITE_WRITE))
                    cerr << RED << "1" << RESET;
                if (dset.count(READ_WRITE))
                    cerr << RED << "2" << RESET;
                if (dset.count(STRICT_START_DEPEND))
                    cerr << RED << "3" << RESET;
                if (dset.count(INSTRUMENT_DEPEND))
                    cerr << "4";
            }
            cerr << "->";
        }
        cerr << endl;
        auto ideal_test_path = longest_stmt_path;
        
        for (int i = 0; i < longest_stmt_path.size(); i++) {
            auto& cur_sid = longest_stmt_path[i];
            auto tid = cur_sid.txn_id;
            auto pos = cur_sid.stmt_idx_in_txn;
            auto stmt = print_stmt_to_string(trans_arr[tid].stmts[pos]);
            auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
            replace(show_str.begin(), show_str.end(), '\n', ' ');
            cerr << "S" << pos << " T" << tid << ": " << show_str << endl;
        }

        cerr << YELLOW << "current stmt_queue: " << RESET << endl;
        for (int i = 0; i < stmt_queue.size(); i++) {
            auto stmt = print_stmt_to_string(stmt_queue[i]);
            auto show_str = stmt.substr(0, stmt.size() > SHOW_CHARACTERS ? SHOW_CHARACTERS : stmt.size());
            replace(show_str.begin(), show_str.end(), '\n', ' ');
            cerr << "T" << tid_queue[i] << ": " << show_str << endl;
        }

        // use the longest path to refine
        shared_ptr<dependency_analyzer> tmp_da;
        while (refine_stmt_queue(longest_stmt_path) == true) {
            clean_instrument();
            block_scheduling();
            instrument_txn_stmts();

            cerr << "txn testing:" << endl;
            trans_test();
            if (analyze_txn_dependency(tmp_da)) 
                throw runtime_error("BUG: found in analyze_txn_dependency()");
            longest_stmt_path = tmp_da->longest_stmt_path();

            cerr << "stmt path for refining: ";
            for (int i = 0; i < longest_stmt_path.size(); i++) {
                auto& cur_sid = longest_stmt_path[i];
                cerr << "(" << cur_sid.txn_id << "." << cur_sid.stmt_idx_in_txn << ")" << "-";
                if (i + 1 < longest_stmt_path.size()) {
                    auto& next_sid = longest_stmt_path[i + 1];
                    auto branch = make_pair<>(cur_sid, next_sid);
                    auto& dset = tmp_da->stmt_dependency_graph[branch];
                    if (dset.count(WRITE_READ))
                        cerr << RED << "0" << RESET;
                    if (dset.count(WRITE_WRITE))
                        cerr << RED << "1" << RESET;
                    if (dset.count(READ_WRITE))
                        cerr << RED << "2" << RESET;
                    if (dset.count(STRICT_START_DEPEND))
                        cerr << RED << "3" << RESET;
                    if (dset.count(INSTRUMENT_DEPEND))
                        cerr << "4";
                }
                cerr << "->";
            }
            cerr << endl;
        }

        // normal test and check
        normal_stmt_test(longest_stmt_path);
        if (check_normal_stmt_result(longest_stmt_path) == false)
            return true;
        
        auto& stmt_graph = init_da->stmt_dependency_graph;
        auto ideal_path_length = ideal_test_path.size();
        for (int i = 0; i + 1 < ideal_path_length; i++) {
            auto& cur_sid = ideal_test_path[i];
            auto& next_sid = ideal_test_path[i + 1];
            auto branch = make_pair(cur_sid, next_sid);
            if (stmt_graph.count(branch) == 0)
                continue;
            stmt_graph[branch].erase(WRITE_READ);
            stmt_graph[branch].erase(WRITE_WRITE);
            stmt_graph[branch].erase(READ_WRITE);
            if (stmt_graph[branch].empty())
                stmt_graph.erase(branch);
        }

        longest_stmt_path = init_da->longest_stmt_path();
        auto new_path_length = longest_stmt_path.size();
        bool has_conflict_depend = false;
        for (int i = 0; i + 1 < new_path_length; i++) {
            auto& cur_sid = longest_stmt_path[i];
            auto& next_sid = longest_stmt_path[i + 1];
            auto branch = make_pair(cur_sid, next_sid);

            if (stmt_graph[branch].count(WRITE_READ) ||
                    stmt_graph[branch].count(WRITE_WRITE) ||
                    stmt_graph[branch].count(READ_WRITE)) {
            
                has_conflict_depend = true; 
                break;   
            }
        }
        cerr << "next test stmt path: ";
        for (auto& sid:longest_stmt_path) 
            cerr << "(" << sid.txn_id << "." << sid.stmt_idx_in_txn << ")" << "->";
        cerr << endl;
        cerr << "has_conflict_depend: " << has_conflict_depend <<endl;
        if (has_conflict_depend == false)
            break;
        
        stmt_queue = init_stmt_queue;
        stmt_use = init_stmt_usage;
        tid_queue = init_tid_queue;
        stmt_num = stmt_queue.size();
        for (int tid = 0; tid < trans_num; tid++) {
            trans_arr[tid].stmts = init_txn_stmt[tid];
            change_txn_status(tid, init_txn_status[tid]);
        }
    }
    return false;
}

void transaction_test::block_scheduling()
{
    cerr << RED << "block scheduling" << RESET << endl;
    int round = 0;
    while (1) {
        cerr << RED << "\nscheduling: " << round << RESET << endl;
        trans_test(false);
        if (tid_queue == real_tid_queue) 
            break;
        stmt_queue = real_stmt_queue;
        stmt_use = real_stmt_usage;
        tid_queue = real_tid_queue;
        clear_execution_status();
        round++;
    }
    clear_execution_status();
    cerr << RED << "schedule round: " << round << RESET << endl;
}

int transaction_test::test()
{
    try {
        assign_txn_id();
        assign_txn_status();
        gen_txn_stmts();
        block_scheduling();
        // instrument_txn_stmts();
        // block_scheduling(); // no necessary
        // throw runtime_error("test");
    } catch(exception &e) {
        cerr << RED << "Trigger a normal bugs when inializing the stmts" << RESET << endl;
        cerr << "Bug info: " << e.what() << endl;

        string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_normal/"; 
        record_bug_num++;

        make_dir_error_exit(dir_name);
        string cmd = "mv " + string(NORMAL_BUG_FILE) + " " + dir_name;
        if (system(cmd.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + " in transaction_test::test!");
        
        // check whether the server is still alive
        fork_if_server_closed();

        // save database
        auto dut = dut_setup(test_dbms_info);
        dut->save_backup_file(dir_name);

        exit(-1);
        // return 1; // not need to do other transaction thing
    }
    
    try {
        // if (multi_round_test() == false)
        //     return 0;
        multi_stmt_round_test();
        // if (multi_stmt_round_test() == false)
        //     return 0;
    } catch(exception &e) {
        cerr << "error captured by test: " << e.what() << endl;
    }

    string dir_name = output_path_dir + "bug_" + to_string(record_bug_num) + "_trans/"; 
    record_bug_num++;
    make_dir_error_exit(dir_name);

    // check whether the server is still alive
    fork_if_server_closed();

    cerr << RED << "Saving database..." << RESET << endl;
    auto dut = dut_setup(test_dbms_info);
    dut->save_backup_file(dir_name);
    
    save_test_case(dir_name);
    
    exit(-1);
    // return 1;
}

transaction_test::transaction_test(dbms_info& d_info)
{
    trans_num = 15; // 12
    test_dbms_info = d_info;

    trans_arr = new transaction[trans_num];
    commit_num = trans_num; // all commit
    stmt_num = 0;
    for (int i = 0; i < trans_num; i++) {
        trans_arr[i].stmt_num = 4 + d6(); // 5 - 10
        stmt_num += trans_arr[i].stmt_num;
    }

    output_path_dir = "found_bugs/";
    struct stat buffer;
    if (stat(output_path_dir.c_str(), &buffer) != 0) {
        make_dir_error_exit(output_path_dir);
    }
}

transaction_test::~transaction_test()
{
    delete[] trans_arr;
}