# Actor Query Operation BGP SmartKG

A [Comunica](https://github.com/comunica/comunica) actor that optimizes SPARQL Basic Graph Pattern (BGP) queries for SmartKG sources.

## Features

- **Star Pattern Decomposition**: Automatically decomposes BGPs into star patterns (patterns grouped by subject variable)
- **Family-Based Optimization**: Routes star patterns to SmartKG when beneficial based on predicate families
- **Intelligent Fallback**: Delegates to standard BGP handling when SmartKG is not optimal
- **Configurable Thresholds**: Max families and minimum pattern count can be customized

## Installation

```bash
npm install @comunica/actor-query-operation-bgp-smartkg
```

## Configuration

This actor can be registered in a Comunica engine configuration:

```json
{
  "@id": "urn:comunica:my-engine",
  "@type": "comunica:Engine",
  "components": [
    {
      "@id": "urn:comunica:my-engine:actor/query-operation/bgp-smartkg",
      "@type": "ActorQueryOperationBgpSmartKg",
      "name": "actor-query-operation-bgp-smartkg",
      "bus": { "@id": "urn:comunica:my-engine:bus/query-operation" },
      "mediatorQueryOperation": { "@id": "urn:comunica:my-engine:mediator/query-operation" },
      "maxFamilies": 100,
      "minPatternCount": 2
    }
  ]
}
```

## Parameters

- `maxFamilies` (integer, default: 100) - Maximum number of SmartKG families to fetch. If exceeded, delegates to standard BGP handling.
- `minPatternCount` (integer, default: 2) - Minimum number of patterns in a star for SmartKG optimization to be applied.

## How It Works

1. **Receives BGP**: Actor receives a SPARQL BGP query
2. **Decomposes Stars**: Splits BGP into star patterns (patterns with same subject)
3. **Analyzes Each Star**: For each star pattern:
   - Checks if it has enough patterns for SmartKG benefit
   - Extracts predicates used in the star
   - Verifies no infrequent predicates are used
   - Queries SmartKG metadata for matching families
   - Calculates if number of families is within threshold
4. **Routes Patterns**:
   - SmartKG candidates → routed to SmartKG source
   - Others → routed to standard BGP handling
5. **Joins Results**: Combines results from all patterns

## Example

```sparql
SELECT ?film ?actress WHERE {
  ?film dbo:starring ?actress .       # Star 1
  ?film foaf:name ?filmName .         # (same subject: ?film)
  ?actress foaf:name ?actressName .   # Star 2
}
```

This BGP would be decomposed into 2 stars and evaluated optimally based on available SmartKG families.

## License

MIT
