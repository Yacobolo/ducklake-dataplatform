# DuckDB RBAC + RLS Hardening — Implementation Plan

## Overview

This plan addresses all identified security, correctness, and robustness issues in the
duck-demo RBAC/RLS system, ordered into 8 phases with explicit dependencies. Each phase
follows TDD: write failing tests first, then implement the fix.

**Constraints:**
- Go 1.25.5
- TDD: failing tests first, then implementation
- Backward compatible: all existing tests must continue to pass
- Proto types: `github.com/substrait-io/substrait-protobuf/go/substraitpb`
- Clone via: `google.golang.org/protobuf/proto`

---

## Phase 1: CRITICAL — Rewriter Missing Rel Type Traversals (Security Bypass)

**Priority:** CRITICAL — RLS filters are silently skipped for tables accessed through
HashJoin, MergeJoin, ExtensionSingle, and ExtensionMulti.

**Complexity:** S

**Dependencies:** None

**Files to modify:**
- `engine/rewriter.go` (lines 107-194, `rewriteRel` switch)
- `engine/rewriter_test.go` (new tests)
- `engine/walker_test.go` (new helper functions)

**What to do:**

1. **Write failing tests** in `engine/rewriter_test.go`:
   - `TestRewriteDescendsIntoHashJoin`: Build a plan with `Rel_HashJoin` containing
     two `ReadRel` children with schemas; apply RLS rules for both tables; assert filters
     are injected into both ReadRels.
   - `TestRewriteDescendsIntoMergeJoin`: Same pattern for `Rel_MergeJoin`.
   - `TestRewriteDescendsIntoExtensionSingle`: Build `Rel_ExtensionSingle` wrapping a
     ReadRel; apply RLS; assert filter injected.
   - `TestRewriteDescendsIntoExtensionMulti`: Build `Rel_ExtensionMulti` with multiple
     ReadRel inputs; apply RLS; assert all filters injected.

2. **Add helper functions** in `engine/walker_test.go`:
   - `hashJoinRel(left, right *pb.Rel) *pb.Rel`
   - `mergeJoinRel(left, right *pb.Rel) *pb.Rel`
   - `extensionSingleRel(input *pb.Rel) *pb.Rel`
   - `extensionMultiRel(inputs ...*pb.Rel) *pb.Rel`

3. **Implement** in `engine/rewriter.go` — add cases to the `rewriteRel` switch:
   ```go
   case *pb.Rel_HashJoin:
       // Descend into Left and Right
   case *pb.Rel_MergeJoin:
       // Descend into Left and Right
   case *pb.Rel_ExtensionSingle:
       // Descend into Input
   case *pb.Rel_ExtensionMulti:
       // Descend into all Inputs
   ```

**Test expectations:**
- All 4 new tests pass (RLS filter present on ReadRels accessed through these join types).
- All existing tests still pass.

---

## Phase 2: HIGH — Fix NamedTable.Names Compound Identifier Handling

**Priority:** HIGH — Wrong table name extracted from multi-segment identifiers like
`["catalog", "schema", "table"]`.

**Complexity:** S

**Dependencies:** None (can be done in parallel with Phase 1)

**Files to modify:**
- `engine/walker.go` (lines 33-39)
- `engine/rewriter.go` (line 210)
- `engine/walker_test.go` (new tests)
- `engine/rewriter_test.go` (new tests)

**What to do:**

1. **Write failing tests**:
   - `engine/walker_test.go`:
     - `TestExtractTableNameFromCompoundIdentifier`: Create a ReadRel with
       `Names: ["my_catalog", "my_schema", "orders"]`; assert `ExtractTableNames` returns
       `["orders"]` (last element), not all three.
     - `TestExtractTableNameSingleElement`: Confirm `Names: ["orders"]` still works (regression).
   - `engine/rewriter_test.go`:
     - `TestRewriteCompoundNamedTable`: Create a ReadRel with
       `Names: ["cat", "sch", "titanic"]` and a schema; apply RLS for `"titanic"`; assert
       filter is injected.

2. **Fix `engine/walker.go` lines 33-39:**
   ```go
   case *pb.Rel_Read:
       if nt := r.Read.GetNamedTable(); nt != nil {
           names := nt.GetNames()
           if len(names) > 0 {
               name := names[len(names)-1] // Use last element
               if !seen[name] {
                   seen[name] = true
                   *tables = append(*tables, name)
               }
           }
       }
   ```

3. **Fix `engine/rewriter.go` line 210:**
   ```go
   names := nt.GetNames()
   tableName := names[len(names)-1] // Use last element, not [0]
   ```

**Test expectations:**
- Compound identifier tests pass.
- Single-element identifier tests pass (backward compat).
- All existing tests still pass (they all use single-element Names).

---

## Phase 3: HIGH — Thread Safety in PolicyStore

**Priority:** HIGH — Concurrent map access will panic at runtime.

**Complexity:** S

**Dependencies:** None (can be done in parallel with Phases 1-2)

**Files to modify:**
- `policy/policy.go` (lines 74-96)
- `policy/policy_test.go` (new tests)

**What to do:**

1. **Write failing test** in `policy/policy_test.go`:
   - `TestConcurrentPolicyStoreAccess`: Launch 100 goroutines that each call `AddRole`
     and `GetRole` concurrently with `sync.WaitGroup`. Run with `-race` flag. The test
     passes if there are no race conditions or panics.
   - `TestConcurrentReadAccess`: Multiple goroutines calling `GetRole` concurrently.

2. **Implement** in `policy/policy.go`:
   - Add `mu sync.RWMutex` field to `PolicyStore`.
   - `AddRole`: acquire `mu.Lock()` / `mu.Unlock()`.
   - `GetRole`: acquire `mu.RLock()` / `mu.RUnlock()`.

**Test expectations:**
- Concurrent test passes under `-race`.
- All existing policy tests still pass.

---

## Phase 4: MEDIUM — Safe Plan Mutation (Clone Before Rewrite)

**Priority:** MEDIUM — A partial rewrite failure leaves the plan in a corrupted state.

**Complexity:** M

**Dependencies:** Phase 1 (rewriter must handle all Rel types before we clone-and-rewrite)

**Files to modify:**
- `engine/rewriter.go` (line 21, `RewritePlan` signature and body)
- `engine/engine.go` (line 70, call site)
- `engine/rewriter_test.go` (new tests)

**What to do:**

1. **Write failing tests** in `engine/rewriter_test.go`:
   - `TestRewritePlanDoesNotMutateOriginal`: Build a plan, clone it manually with
     `proto.Clone`, call `RewritePlan` with RLS rules, then compare the original plan to
     the clone to ensure the original is unchanged.
   - `TestRewritePlanReturnsCopy`: Verify the returned plan is a different pointer.
   - `TestRewritePlanHandlesBareRel`: Build a plan with `PlanRel_Rel` (not `PlanRel_Root`);
     apply RLS rules; assert filter is injected.

2. **Change `RewritePlan` signature:**
   ```go
   func RewritePlan(plan *pb.Plan, rulesByTable map[string][]policy.RLSRule) (*pb.Plan, error)
   ```
   - Clone the plan at the start: `cloned := proto.Clone(plan).(*pb.Plan)`
   - Operate on `cloned`.
   - Add handling for `PlanRel_Rel` (bare rel without Root wrapper):
     ```go
     if bare := rel.GetRel(); bare != nil {
         newRel, err := rewriteRel(bare, rulesByTable, cloned, anchorAlloc)
         if err != nil { return nil, err }
         rel.RelType = &pb.PlanRel_Rel{Rel: newRel}
     }
     ```
   - Return `cloned, nil`.

3. **Update call site** in `engine/engine.go` line 70:
   ```go
   rewrittenPlan, err := RewritePlan(plan, rulesByTable)
   if err != nil { ... }
   // Use rewrittenPlan for marshal
   ```

4. **Update all existing tests** that call `RewritePlan` to accept the new return value.

**Test expectations:**
- Original plan is provably unchanged after rewrite.
- Bare `PlanRel_Rel` plans get RLS filters injected.
- All existing tests pass with updated call signature.

---

## Phase 5: MEDIUM — Broader Type Support in buildComparisonComponents

**Priority:** MEDIUM — Many common column types will fail with "unsupported column type."

**Complexity:** M

**Dependencies:** None (can be done in parallel with Phases 1-4)

**Files to modify:**
- `engine/rewriter.go` (lines 373-451, `buildComparisonComponents`)
- `engine/rewriter_test.go` (new tests)

**What to do:**

1. **Write failing tests** in `engine/rewriter_test.go` — one test per new type:
   - `TestI16LiteralFilter`: Schema with `Type_I16`, RLS rule with `int32` value
     (since Go has no int16, use int32 and document the narrowing).
   - `TestI8LiteralFilter`: Schema with `Type_I8`, RLS rule with `int32` value.
   - `TestFp32LiteralFilter`: Schema with `Type_Fp32`, RLS rule with `float32` value.
   - `TestDateLiteralFilter`: Schema with `Type_Date`, RLS rule with `int32` value
     (days since epoch per Substrait spec).
   - `TestTimestampLiteralFilter`: Schema with `Type_Timestamp`, RLS rule with `int64`
     value (microseconds since epoch).
   - `TestVarCharLiteralFilter`: Schema with `Type_VarChar`, RLS rule with `string` value.
   - `TestFixedCharLiteralFilter`: Schema with `Type_FixedChar`, RLS rule with `string` value.
   - `TestDecimalLiteralFilter`: Schema with `Type_Decimal`, RLS rule with `[]byte` value
     (Substrait decimal encoding).

2. **Implement** new cases in `buildComparisonComponents`:
   ```go
   case *pb.Type_I16_:
       val, ok := rule.Value.(int32) // Go doesn't have int16; accept int32
       funcName := fmt.Sprintf("%s:i16_i16", opPrefix)
       // Literal: &pb.Expression_Literal_I16{I16: val}
       // (Note: substrait proto uses int32 for i16 literal field)

   case *pb.Type_I8_:
       // Similar to I16

   case *pb.Type_Fp32:
       val, ok := rule.Value.(float32)
       funcName := fmt.Sprintf("%s:fp32_fp32", opPrefix)

   case *pb.Type_Date_:
       val, ok := rule.Value.(int32) // days since epoch
       funcName := fmt.Sprintf("%s:date_date", opPrefix)

   case *pb.Type_Timestamp_:
       val, ok := rule.Value.(int64) // microseconds since epoch
       funcName := fmt.Sprintf("%s:ts_ts", opPrefix)

   case *pb.Type_VarChar_:
       val, ok := rule.Value.(string)
       funcName := fmt.Sprintf("%s:vchar_vchar", opPrefix)

   case *pb.Type_FixedChar_:
       val, ok := rule.Value.(string)
       funcName := fmt.Sprintf("%s:fchar_fchar", opPrefix)

   case *pb.Type_Decimal_:
       val, ok := rule.Value.([]byte)
       funcName := fmt.Sprintf("%s:dec_dec", opPrefix)
   ```

   > **Note:** Verify exact proto field types and Substrait function naming conventions by
   > inspecting the substrait-protobuf Go types before implementing. The names above are
   > illustrative and must be confirmed against the spec.

**Test expectations:**
- Each new type test passes.
- Existing i64/i32/string/fp64/bool tests still pass.

---

## Phase 6: MEDIUM — Error Handling Hardening

**Priority:** MEDIUM — Missing bounds checks, nil guards, and error wrapping.

**Complexity:** M

**Dependencies:** Phase 4 (RewritePlan signature change)

**Files to modify:**
- `engine/rewriter.go` (lines 217, 256, 353)
- `engine/engine.go` (lines 34, 56)
- `engine/rewriter_test.go` (new tests)
- `engine/engine_test.go` (new tests)

**What to do:**

1. **Write failing tests:**
   - `engine/rewriter_test.go`:
     - `TestBoundsCheckOnSchemaTypes`: Build a schema with 2 types but an RLS rule
       referencing column index 5; assert error (not panic).
     - `TestNilSchemaHandling`: Build a ReadRel with `BaseSchema: nil` and matching RLS
       rules; assert a clear error message.
     - `TestColumnNotFoundError`: RLS rule references column `"nonexistent"`; assert error
       with descriptive message.
     - `TestUnsupportedOperator`: RLS rule with operator `"like"`; assert error.
     - `TestDuplicateColumnNameInSchema`: Schema with `Names: ["id", "id"]`; document
       behavior (first match wins) or return error.
     - `TestNilPlan`: Call `RewritePlan(nil, rules)` — should return error, not panic.
     - `TestNilRel`: Ensure `rewriteRel(nil, ...)` returns `(nil, nil)` cleanly (already
       handled, but add explicit test).
   - `engine/engine_test.go`:
     - `TestCheckAccessErrorIsWrapped`: Verify the error from `CheckAccess` is wrapped
       with `fmt.Errorf("access denied: %w", err)` so callers can use `errors.Is`.

2. **Implement fixes:**

   a. **`engine/rewriter.go` line 353** — bounds check:
      ```go
      if colIdx >= len(schema.Struct.Types) {
          return "", nil, fmt.Errorf("column index %d out of range (schema has %d types)", colIdx, len(schema.Struct.Types))
      }
      ```

   b. **`engine/rewriter.go` line 217** — nil schema guard:
      ```go
      if read.GetBaseSchema() == nil {
          return nil, fmt.Errorf("table %q: ReadRel has no base schema, cannot apply RLS", tableName)
      }
      ```

   c. **`engine/rewriter.go` line 256** — duplicate column handling:
      ```go
      for i, name := range schema.Names {
          if _, exists := idx[name]; exists {
              // First occurrence wins (matches Substrait semantics)
              continue
          }
          idx[name] = i
      }
      ```
      Or return an error — decide based on Substrait spec behavior.

   d. **`engine/engine.go` line 56** — wrap CheckAccess error:
      ```go
      if err := role.CheckAccess(tables); err != nil {
          return nil, fmt.Errorf("rbac check: %w", err)
      }
      ```

   e. **`engine/engine.go` line 34** — add `context.Context` support:
      ```go
      func (e *SecureEngine) Query(ctx context.Context, roleName, sqlQuery string) (*sql.Rows, error) {
      ```
      Use `ctx` in `db.QueryRowContext` and `db.QueryContext` calls.
      **Breaking change** — update all call sites (main.go, engine_test.go).

   f. **`engine/rewriter.go`** — nil plan guard at top of `RewritePlan`:
      ```go
      if plan == nil {
          return nil, fmt.Errorf("plan is nil")
      }
      ```

**Test expectations:**
- All error-path tests pass with descriptive, wrapped errors.
- No panics on nil/out-of-bounds inputs.
- Context propagation works end-to-end (integration tests updated).
- All existing tests still pass (with updated signatures).

---

## Phase 7: MEDIUM — Policy Store Improvements

**Priority:** MEDIUM — Silent overwrites and no way to remove roles.

**Complexity:** S

**Dependencies:** Phase 3 (must have mutex in place first)

**Files to modify:**
- `policy/policy.go` (lines 84-96)
- `policy/policy_test.go` (new tests)

**What to do:**

1. **Write failing tests** in `policy/policy_test.go`:
   - `TestAddRoleDuplicateReturnsError`: Add a role, then add another role with the same
     name; assert error.
   - `TestUpdateRoleSuccess`: Add a role, then update it with new AllowedTables; assert
     the change took effect.
   - `TestUpdateRoleNotFound`: Update a non-existent role; assert error.
   - `TestRemoveRoleSuccess`: Add a role, remove it, then `GetRole` returns error.
   - `TestRemoveRoleNotFound`: Remove a non-existent role; assert error.

2. **Implement:**

   a. **Change `AddRole`** to return an error:
      ```go
      func (s *PolicyStore) AddRole(role *Role) error {
          s.mu.Lock()
          defer s.mu.Unlock()
          if _, exists := s.roles[role.Name]; exists {
              return fmt.Errorf("role %q already exists", role.Name)
          }
          s.roles[role.Name] = role
          return nil
      }
      ```

   b. **Add `UpdateRole`:**
      ```go
      func (s *PolicyStore) UpdateRole(role *Role) error {
          s.mu.Lock()
          defer s.mu.Unlock()
          if _, exists := s.roles[role.Name]; !exists {
              return fmt.Errorf("role %q not found", role.Name)
          }
          s.roles[role.Name] = role
          return nil
      }
      ```

   c. **Add `RemoveRole`:**
      ```go
      func (s *PolicyStore) RemoveRole(name string) error {
          s.mu.Lock()
          defer s.mu.Unlock()
          if _, exists := s.roles[name]; !exists {
              return fmt.Errorf("role %q not found", name)
          }
          delete(s.roles, name)
          return nil
      }
      ```

   d. **Update all call sites** that use `AddRole` (main.go, test files) to handle the
      returned error. For existing code that adds roles at init time, the simplest approach
      is to add a helper `MustAddRole` that panics on error, or to ignore the error in
      test setup where roles are known to be unique.

**Test expectations:**
- Duplicate add returns error.
- Update and Remove work correctly.
- Existing tests updated and still pass.

---

## Phase 8: Comprehensive Test Coverage

**Priority:** TEST COVERAGE — Ensure all behaviors are verified.

**Complexity:** L

**Dependencies:** All previous phases (1-7)

**Files to modify:**
- `engine/walker_test.go`
- `engine/rewriter_test.go`
- `engine/engine_test.go` (if integration tests feasible)
- `policy/policy_test.go`

**What to do:**

This phase covers any remaining test gaps not already addressed in Phases 1-7.
Many of these tests will have been written in earlier phases; this phase ensures
completeness.

### 8a. Walker Traversal Tests (`engine/walker_test.go`)

- `TestWalkerSortRel`: Verify `ExtractTableNames` descends into `SortRel`.
- `TestWalkerAggregateRel`: Verify descent into `AggregateRel`.
- `TestWalkerCrossRel`: Verify descent into `CrossRel` left and right.
- `TestWalkerHashJoinRel`: Verify descent into `HashJoinRel` left and right.
- `TestWalkerMergeJoinRel`: Verify descent into `MergeJoinRel` left and right.
- `TestWalkerSetRel`: Verify descent into all `SetRel` inputs.
- `TestWalkerExtensionSingleRel`: Verify descent into `ExtensionSingleRel`.
- `TestWalkerExtensionMultiRel`: Verify descent into all `ExtensionMultiRel` inputs.
- `TestWalkerBareRelInPlan`: Verify `ExtractTableNames` handles `PlanRel_Rel` (not just Root).

### 8b. Rewriter Operator Tests (`engine/rewriter_test.go`)

For each non-equal operator, verify the correct Substrait function is registered:
- `TestLessThanOperator`: `policy.OpLessThan` → `lt:i64_i64`
- `TestLessEqualOperator`: `policy.OpLessEqual` → `lte:i64_i64`
- `TestGreaterThanOperator`: `policy.OpGreaterThan` → `gt:i64_i64`
- `TestGreaterEqualOperator`: `policy.OpGreaterEqual` → `gte:i64_i64`
- `TestNotEqualOperator`: `policy.OpNotEqual` → `not_equal:i64_i64`

### 8c. Integration Tests (`engine/engine_test.go`)

These require DuckDB + substrait extension + parquet file. Use existing `setupEngine` helper:

- `TestJoinWithRLSOnBothTables`: Create a second table, apply RLS rules to both; verify
  both tables are filtered. (Requires creating a second parquet/table in test setup.)
- `TestContextCancellation`: Pass a cancelled context; verify query returns error promptly.

### 8d. Edge Case Tests

- `TestEmptyRulesMap`: Already exists — verify no-op behavior.
- `TestEmptyRulesForTable`: Rules map has key but empty slice — no filter injected.
- `TestReadRelWithNoNamedTable`: VirtualTable or LocalFiles — no filter injected, no error.
- `TestMultipleRelationsInPlan`: Plan with 2+ root relations; each gets rewritten.

**Test expectations:**
- 100% of walker Rel types have at least one traversal test.
- 100% of rewriter Rel types have at least one RLS injection test.
- All 6 operators tested in unit tests.
- Error paths covered (column not found, unsupported type, nil schema, bounds check).
- New types covered (Phase 5 types).
- Concurrency covered (Phase 3 test).
- Plan immutability covered (Phase 4 test).

---

## Dependency Graph

```
Phase 1 (CRITICAL: Rewriter Rel types)  ──┐
Phase 2 (HIGH: NamedTable.Names)           │──→ Phase 4 (Safe Plan Mutation)
Phase 3 (HIGH: Thread Safety)  ────────────│──→ Phase 7 (Policy Improvements)
Phase 5 (MEDIUM: Broader Types)            │
                                           │
                              Phase 4 ─────┤──→ Phase 6 (Error Handling)
                                           │
                              All (1-7) ───┘──→ Phase 8 (Test Coverage)
```

Phases 1, 2, 3, and 5 can all be started in parallel.
Phase 4 depends on Phase 1.
Phase 6 depends on Phase 4.
Phase 7 depends on Phase 3.
Phase 8 depends on all previous phases.

---

## Summary Table

| Phase | Priority | Complexity | Files Modified | Key Deliverable |
|-------|----------|------------|----------------|-----------------|
| 1 | CRITICAL | S | rewriter.go, rewriter_test.go, walker_test.go | HashJoin/MergeJoin/Extension traversal in rewriter |
| 2 | HIGH | S | walker.go, rewriter.go, walker_test.go, rewriter_test.go | Last-element NamedTable.Names resolution |
| 3 | HIGH | S | policy.go, policy_test.go | sync.RWMutex on PolicyStore |
| 4 | MEDIUM | M | rewriter.go, engine.go, rewriter_test.go | proto.Clone + bare PlanRel_Rel support |
| 5 | MEDIUM | M | rewriter.go, rewriter_test.go | i16, i8, fp32, date, timestamp, varchar, fixedchar, decimal |
| 6 | MEDIUM | M | rewriter.go, engine.go, rewriter_test.go, engine_test.go | Bounds checks, nil guards, error wrapping, context.Context |
| 7 | MEDIUM | S | policy.go, policy_test.go | AddRole returns error, UpdateRole, RemoveRole |
| 8 | — | L | all *_test.go files | Comprehensive coverage for all Rel types, operators, edge cases |
