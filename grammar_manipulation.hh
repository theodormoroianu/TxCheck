#ifndef GRAMMAR_MANIPULATION_HH
#define GRAMMAR_MANIPULATION_HH

#include "grammar.hh"

/**
 * Extracts the query from an update statement. It extracts all fields of the queried table, while keeping the same
 * predicate.
 */
shared_ptr<query_spec> ExtractQueryFromUpdate(shared_ptr<update_stmt> update);

#endif
