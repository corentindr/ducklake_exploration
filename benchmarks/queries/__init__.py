"""
Each benchmark is a named function that accepts a FormatAdapter and returns
a list of (query_name: str, description: str, result: QueryResult).

The runner calls these in sequence for each format.
"""
from benchmarks.queries.reads import READ_BENCHMARKS
from benchmarks.queries.aggregations import AGGREGATION_BENCHMARKS
from benchmarks.queries.updates import UPDATE_BENCHMARKS
from benchmarks.queries.schema_evolution import SCHEMA_EVOLUTION_BENCHMARKS

ALL_BENCHMARKS = {
    "reads": READ_BENCHMARKS,
    "aggregations": AGGREGATION_BENCHMARKS,
    "updates": UPDATE_BENCHMARKS,
    "schema_evolution": SCHEMA_EVOLUTION_BENCHMARKS,
}
