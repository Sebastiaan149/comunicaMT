# Comunica SPARQL SmartKG

[![npm version](https://badge.fury.io/js/%40comunica%2Fquery-sparql-smartkg.svg)](https://www.npmjs.com/package/@comunica/query-sparql-smartkg)

A SmartKG-optimized SPARQL query engine for JavaScript that queries family-partitioned RDF knowledge graphs using intelligent source routing and predicate-based optimization.

## Key Features

* Execute SPARQL 1.2 queries optimized for SmartKG partitioned sources
* **Family-based partitioning**: Automatically decomposes basic graph patterns (BGPs) into star patterns and routes them to SmartKG family-based partitions
* **Intelligent source identification**: Automatically detects and optimizes queries for SmartKG sources with metadata-driven family selection
* **Exclusive SmartKG focus**: Optimized specifically for SmartKG sources - does not fall back to generic SPARQL query handling for non-SmartKG sources
* **Integrated HTTP interface**: Built-in SPARQL Protocol server for remote query execution
* Reduced package size compared to full Comunica SPARQL (focused dependencies)

## Data Sources Supported

- **SmartKG HTTP servers**: Remote SmartKG servers with metadata endpoints
- **Local HDT files**: HDT (Header Dictionary Triples) partitioned files
- **Local RDF files**: N3, RDF/XML, and other RDF formats

## Usage

### Command-line Interface

Query a SmartKG source:

```bash
comunica-sparql-smartkg https://example.com/smartkg 'SELECT * WHERE { ?s ?p ?o }'
```

### HTTP Server

Start an HTTP SPARQL Protocol endpoint:

```bash
comunica-sparql-smartkg-http --listen 3000 https://example.com/smartkg
```

### Programmatic API

```javascript
import { QueryEngine } from '@comunica/query-sparql-smartkg';

const engine = new QueryEngine();
const result = await engine.query(
  'SELECT * WHERE { ?s ?p ?o }',
  { sources: ['https://example.com/smartkg'] }
);
```

## Configuration

The engine includes SmartKG-specific actors:

- **ActorQueryOperationBgpSmartKg**: Decomposes BGPs into star patterns, analyzes predicate families, and routes queries to SmartKG
- **ActorQuerySourceIdentifyHypermediaSmartKg**: Identifies SmartKG sources and executes queries using family-based partitioning

### Configuration Parameters

- `maxFamilies`: Maximum number of predicate families to fetch before delegating (default: 100)
- `minPatternCount`: Minimum number of triple patterns to consider for SmartKG optimization (default: 2)

## Important Notes

- This engine is **SmartKG-only** and will not fall back to generic SPARQL query processing for non-SmartKG sources
- For generic SPARQL querying, use [@comunica/query-sparql](https://github.com/comunica/comunica/tree/master/engines/query-sparql)
- For local file support without HTTP restrictions, use [@comunica/query-sparql-file](https://github.com/comunica/comunica/tree/master/engines/query-sparql-file)

## Learn More

- **[Comunica website](https://comunica.dev/)**
- **[SmartKG documentation](https://smart-kg.github.io/)**
- **[Comunica SPARQL engine](https://github.com/comunica/comunica/tree/master/engines/query-sparql)**

## License

MIT
