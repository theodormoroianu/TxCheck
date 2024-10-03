#include <typeinfo>
#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <cassert>

#include "random.hh"
#include "relmodel.hh"
#include "grammar_manipulation.hh"
#include "schema.hh"
#include "impedance.hh"

using namespace std;

shared_ptr<query_spec> ExtractQueryFromUpdate(shared_ptr<update_stmt> update)
{
    auto pprod = update->pprod;
    auto scope = update->scope;
    auto from_table = update->victim;
    auto condition = update->search;

    return shared_ptr<query_spec>(new query_spec(pprod, scope, from_table, condition));
}