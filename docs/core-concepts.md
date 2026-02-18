# Core Concepts

These are the key concepts to understand before building on the platform.

## Catalog and Objects

- A **catalog** is a top-level container for schemas, tables, and views.
- A **schema** groups related tables/views.
- A **table** stores data; a **view** stores a reusable query definition.

See endpoint coverage in [Catalogs](/reference/generated/api/endpoints/catalogs).

## Identity and Access

- **Principals** represent users or service identities.
- **Groups** let you manage permissions in bulk.
- **Grants** assign privileges on securable objects.

See [Security](/reference/generated/api/endpoints/security) for operations.

## Query Execution

- Queries run through `POST /v1/query` as the authenticated principal.
- Access checks happen at execution time based on grants and security policies.

See [Query](/reference/generated/api/endpoints/query).

## Data Security Controls

- **Row filters** restrict visible rows by principal.
- **Column masks** obfuscate sensitive values for selected principals.

Both are modeled as first-class API resources in Security endpoints.

## Operations and Governance

- **Ingestion** loads and commits data into the platform.
- **Lineage** tracks dependencies between tables and columns.
- **Tags** and search support discoverability and policy workflows.

See [Platform Features](/reference/generated/api/features) for a complete list.

## Declarative Management

You can manage many resources as declarative configuration documents.

- Schema reference: [/reference/generated/declarative/index](/reference/generated/declarative/index)
- Implementation notes: [/declarative-schema](/declarative-schema)
