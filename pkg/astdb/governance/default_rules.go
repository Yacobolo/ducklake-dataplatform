// Package governance runs DuckDB-backed architecture governance queries.
package governance

func defaultRules() []Rule {
	return []Rule{
		{
			ID:          "GOV001_IMPORT_BOUNDARIES",
			Category:    "architecture",
			Severity:    "error",
			Description: "Internal package imports must respect architecture layer boundaries",
			Enabled:     true,
			QuerySQL: `
WITH boundaries(source_prefix, forbidden_prefix, hint) AS (
    VALUES
      ('duck-demo/internal/domain', 'duck-demo/internal/api', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/internal/service', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/internal/db', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/internal/engine', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/internal/middleware', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/internal/declarative', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/cmd', 'domain may only import domain'),
      ('duck-demo/internal/domain', 'duck-demo/pkg/cli', 'domain may only import domain'),

      ('duck-demo/internal/service', 'duck-demo/internal/api', 'service should depend on domain and service-local packages'),
      ('duck-demo/internal/service', 'duck-demo/internal/db', 'service should depend on domain and service-local packages'),
      ('duck-demo/internal/service', 'duck-demo/internal/engine', 'service should depend on domain and service-local packages'),
      ('duck-demo/internal/service', 'duck-demo/internal/middleware', 'service should depend on domain and service-local packages'),
      ('duck-demo/internal/service', 'duck-demo/cmd', 'service should depend on domain and service-local packages'),
      ('duck-demo/internal/service', 'duck-demo/pkg/cli', 'service should depend on domain and service-local packages'),

      ('duck-demo/internal/api', 'duck-demo/internal/db', 'api should depend on service/domain/api packages'),
      ('duck-demo/internal/api', 'duck-demo/internal/engine', 'api should depend on service/domain/api packages'),
      ('duck-demo/internal/api', 'duck-demo/internal/declarative', 'api should depend on service/domain/api packages'),
      ('duck-demo/internal/api', 'duck-demo/cmd', 'api should depend on service/domain/api packages'),
      ('duck-demo/internal/api', 'duck-demo/pkg/cli', 'api should depend on service/domain/api packages'),

      ('duck-demo/internal/db', 'duck-demo/internal/api', 'db should depend on domain and db-local packages'),
      ('duck-demo/internal/db', 'duck-demo/internal/service', 'db should depend on domain and db-local packages'),
      ('duck-demo/internal/db', 'duck-demo/internal/engine', 'db should depend on domain and db-local packages'),
      ('duck-demo/internal/db', 'duck-demo/internal/middleware', 'db should depend on domain and db-local packages'),
      ('duck-demo/internal/db', 'duck-demo/cmd', 'db should depend on domain and db-local packages'),
      ('duck-demo/internal/db', 'duck-demo/pkg/cli', 'db should depend on domain and db-local packages'),

      ('duck-demo/internal/engine', 'duck-demo/internal/api', 'engine should depend on domain and engine-local packages'),
      ('duck-demo/internal/engine', 'duck-demo/internal/service', 'engine should depend on domain and engine-local packages'),
      ('duck-demo/internal/engine', 'duck-demo/cmd', 'engine should depend on domain and engine-local packages'),
      ('duck-demo/internal/engine', 'duck-demo/pkg/cli', 'engine should depend on domain and engine-local packages'),

      ('duck-demo/internal/middleware', 'duck-demo/internal/service', 'middleware should depend on domain and middleware-local packages'),
      ('duck-demo/internal/middleware', 'duck-demo/internal/db', 'middleware should depend on domain and middleware-local packages'),
      ('duck-demo/internal/middleware', 'duck-demo/internal/engine', 'middleware should depend on domain and middleware-local packages')
),
import_edges AS (
    SELECT
      f.path AS file_path,
      'duck-demo/' || regexp_replace(f.path, '/[^/]+$', '') AS source_pkg,
      replace(coalesce(n.node_text, ''), '"', '') AS import_path
    FROM nodes n
    JOIN files f ON f.file_id = n.file_id
    WHERE n.kind = '*ast.ImportSpec'
      AND f.path LIKE 'internal/%'
      AND f.path NOT LIKE '%_test.go'
      AND f.path NOT LIKE '%.gen.go'
      AND f.path NOT LIKE '%_gen.go'
      AND f.path NOT LIKE '%.sql.go'
)
SELECT
  i.file_path AS file_path,
  i.source_pkg AS symbol,
  ('imports ' || i.import_path || '; allowed direction: ' || b.hint) AS detail,
  0 AS line
FROM import_edges i
JOIN boundaries b
  ON (i.source_pkg = b.source_prefix OR i.source_pkg LIKE b.source_prefix || '/%')
 AND (i.import_path = b.forbidden_prefix OR i.import_path LIKE b.forbidden_prefix || '/%')
WHERE i.import_path LIKE 'duck-demo/%'
ORDER BY i.file_path, i.import_path
`,
		},
	}
}
