/// @file
/// @brief Base class for device under test

#ifndef DUT_HH
#define DUT_HH
#include <stdexcept>
#include <string>
#include <vector>
#include <map>

#include "prod.hh"

using namespace std;

namespace dut {
  
struct failure : public std::exception {
  std::string errstr;
  std::string sqlstate;
  const char* what() const throw()
  {
    return errstr.c_str();
  }
  failure(const char *s, const char *sqlstate_ = "") throw()
       : errstr(), sqlstate() {
    errstr = s;
    sqlstate = sqlstate_;
  };
};

struct broken : failure {
  broken(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

struct timeout : failure {
  timeout(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

struct syntax : failure {
  syntax(const char *s, const char *sqlstate_ = "") throw()
    : failure(s, sqlstate_) { }
};

}

struct dut_base {
  std::string version;
  virtual void test(const std::string &stmt, std::vector<std::string>* output = NULL, int* affected_row_num = NULL) = 0;
  virtual void reset(void) = 0;

  virtual void backup(void) = 0;
  virtual void reset_to_backup(void) = 0;
  virtual int save_backup_file(string path) = 0;

  virtual void wrap_stmts_as_trans(vector<std::string> &stmt_vec, bool is_commit) = 0;
  virtual bool is_commit_abort_stmt(string& stmt) = 0;
  virtual bool is_begin_stmt(string& stmt) = 0;
  
  virtual void trans_test(const std::vector<std::string> &stmt_vec
                          , std::vector<std::string>* exec_stmt_vec
                          , vector<vector<string>>* output = NULL
                          , int commit_or_not = 1) = 0;
  virtual void get_content(vector<string>& tables_name, map<string, vector<string>>& content) = 0;
};

#endif