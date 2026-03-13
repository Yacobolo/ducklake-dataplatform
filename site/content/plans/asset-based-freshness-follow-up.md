# Follow-Up Plan: Asset-Based Freshness For Dashboards

## Goal
Let users declare freshness on data products like dashboards, semantic models, and metrics, then have the platform propagate that requirement upstream and refresh the nearest executable assets needed to satisfy it.

Example:
- user sets dashboard freshness to 30 minutes
- system resolves upstream dependencies
- system triggers pre-aggregations / models / published notebook outputs as needed
- dashboard freshness becomes derived from upstream execution state

## Product Model
- Treat assets as graph nodes.
- Split assets into two categories:
  - logical assets: `DASHBOARD`, `SEMANTIC_MODEL`, `METRIC`, `MODEL`, `NOTEBOOK_OUTPUT`
  - executable assets: `MODEL`, `SEMANTIC_PRE_AGGREGATION`, `NOTEBOOK_OUTPUT`
- Dashboards and metrics are assets in the graph, but not direct execution targets.
- Freshness policies can be attached to logical assets.
- Materialization and auto-refresh policies can only be attached to executable assets.

## Phase 1: Upgrade The Asset Contract
- Extend `DataAsset` public write surface so it can actually carry:
  - freshness policy
  - materialization policy
  - auto-materialize policy
- Add new asset types:
  - `DASHBOARD`
  - `SEMANTIC_MODEL`
  - `METRIC`
  - `SEMANTIC_PRE_AGGREGATION`
  - `NOTEBOOK_OUTPUT`
- Keep `NOTEBOOK` document assets out of orchestration unless they produce a published output.
- Extend API and CLI asset endpoints to support these policy fields.
- Keep validation strict:
  - logical assets may have freshness policy
  - only executable assets may have materialization or auto-materialize settings

## Phase 2: Register New Resources Into The Asset Graph
- Add asset adapters/sync for:
  - semantic models
  - semantic metrics
  - semantic pre-aggregations
  - dashboards
  - published notebook outputs
- Build dependencies:
  - dashboard -> widget source assets
  - semantic model -> base model
  - metric -> semantic model and selected pre-aggregations where applicable
  - pre-aggregation -> semantic model / underlying model
  - notebook output -> upstream notebook/model dependencies if publish metadata exists
- Do not create a parallel graph outside the asset system.

## Phase 3: Freshness Propagation Engine
- Add a resolver that computes required freshness upstream from any logical asset.
- Rules:
  - dashboard freshness propagates to widget source assets
  - metric freshness propagates to semantic model and executable serving assets
  - semantic model freshness propagates to base model / pre-aggregations
  - multiple downstream requirements collapse to the strictest SLA upstream
- Add derived freshness states:
  - `fresh`
  - `stale`
  - `refreshing`
  - `blocked`
  - `unknown`
- Preserve explainability:
  - why an asset is stale
  - which upstream asset is violating the SLA
  - which executable asset is currently refreshing

## Phase 4: Execution Integration
- Add execution adapters for executable semantic/data-product assets:
  - semantic pre-aggregation refresh runner
  - published notebook output refresh runner
  - existing model execution path reused as-is
- Reconciler behavior:
  - on freshness breach, find nearest executable upstream assets
  - enqueue materialization only for those assets
  - dedupe runs across shared dependencies
- Do not enqueue runs for dashboards, metrics, or semantic models directly.

## Phase 5: API, CLI, And Explainability
- Add API endpoints to:
  - get effective freshness for any asset
  - explain freshness propagation for an asset
  - list upstream freshness requirements
  - list blocking upstream assets
- Add CLI support to:
  - set freshness on dashboards / semantic assets
  - inspect freshness graph
  - explain why a dashboard is stale
- Example user workflow:
  - set dashboard freshness to 30m
  - inspect resolved upstream requirements
  - see pre-aggregation/model runs triggered
  - verify dashboard returned to fresh

## Phase 6: UI Surfaces
- Show freshness and dependency state on dashboard detail:
  - fresh/stale badge
  - upstream blockers
  - current refresh activity
- Show semantic asset lineage:
  - dashboard -> metric -> semantic model -> executable asset
- Keep dashboard "refresh" UI as a user convenience that delegates to underlying executable assets, not as dashboard materialization.

## Design Constraints
- Do not introduce a second orchestration system for dashboards/semantics.
- Do not make dashboards or metrics executable jobs.
- Do not treat notebook documents as refresh targets; only published notebook outputs.
- Keep freshness policy declarative and derived, not imperative cron-job sprawl.
- Preserve API-first operability for agents and CLI users.

## Implementation Order
1. Asset schema/API uplift
2. New asset types and validation
3. Semantic/dashboard/notebook-output asset adapters
4. Freshness propagation resolver
5. Pre-aggregation and notebook-output execution adapters
6. API/CLI explain endpoints
7. UI freshness graph/status surfaces

## Acceptance Criteria
- A dashboard can be assigned a freshness policy through API/CLI.
- A dashboard freshness requirement propagates through metrics and semantic models to executable upstream assets.
- The strictest downstream freshness requirement is enforced for shared upstream assets.
- Only executable assets are enqueued for refresh work.
- Semantic pre-aggregations and published notebook outputs can be refreshed by the orchestrator.
- Users can inspect why a dashboard is stale and which upstream asset is blocking freshness.
- All new behavior is available through API and CLI, not just UI.
- No separate dashboard-only orchestration path is introduced.

## Out Of Scope For This Follow-Up
- Arbitrary notebook documents as scheduled data products
- Dashboard snapshot materialization
- General-purpose SLA language beyond freshness/max-lag
- Full cost-based optimization of which executable upstream to refresh
