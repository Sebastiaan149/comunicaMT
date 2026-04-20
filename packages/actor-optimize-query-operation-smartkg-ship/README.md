# Smart-KG Query Operation Optimizer

This Comunica actor optimizes SPARQL query operations for Smart-KG by:

1. **Decomposing JOINs into Stars**: Groups triple patterns with the same subject
2. **Analyzing Predicate Coverage**: Determines which predicates are covered by Smart-KG families
3. **Deciding Shipping Strategies**: Chooses between:
   - **Partition Shipping (P-S)**: Download large partitions for stars with 2+ patterns
   - **Triple Pattern Shipping (TP-S)**: Send small requests for single patterns or uncovered predicates
4. **Annotating the Query Plan**: Attaches shipping strategy metadata for execution

## Key Principles

According to the SMART-KG paper (Section 4.2):

- Stars with **only 1 pattern** must always use TP-S (no partition benefit)
- Stars with **2+ patterns** should use P-S if predicate coverage is good
- Predicates **not covered by families** are supplemented with TPF queries

## Integration

This actor runs on the `bus:optimize-query-operation` bus and is typically placed in the optimization pipeline before source selection and query execution.

The optimization metadata is attached to JOIN operations for use by:
- Query executors (to decide execution strategy)
- Source selectors (to optimize data shipping)
- Smart-KG's query source (to handle mixed P-S and TP-S operations)
