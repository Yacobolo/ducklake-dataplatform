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
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/api', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/service', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/db', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/engine', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/middleware', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/internal/declarative', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/cmd', 'domain may only import domain'),
      ('github.com/Yacobolo/quackstack/internal/domain', 'github.com/Yacobolo/quackstack/pkg/cli', 'domain may only import domain'),

      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/internal/api', 'service should depend on domain and service-local packages'),
      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/internal/db', 'service should depend on domain and service-local packages'),
      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/internal/engine', 'service should depend on domain and service-local packages'),
      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/internal/middleware', 'service should depend on domain and service-local packages'),
      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/cmd', 'service should depend on domain and service-local packages'),
      ('github.com/Yacobolo/quackstack/internal/service', 'github.com/Yacobolo/quackstack/pkg/cli', 'service should depend on domain and service-local packages'),

      ('github.com/Yacobolo/quackstack/internal/api', 'github.com/Yacobolo/quackstack/internal/db', 'api should depend on service/domain/api packages'),
      ('github.com/Yacobolo/quackstack/internal/api', 'github.com/Yacobolo/quackstack/internal/engine', 'api should depend on service/domain/api packages'),
      ('github.com/Yacobolo/quackstack/internal/api', 'github.com/Yacobolo/quackstack/internal/declarative', 'api should depend on service/domain/api packages'),
      ('github.com/Yacobolo/quackstack/internal/api', 'github.com/Yacobolo/quackstack/cmd', 'api should depend on service/domain/api packages'),
      ('github.com/Yacobolo/quackstack/internal/api', 'github.com/Yacobolo/quackstack/pkg/cli', 'api should depend on service/domain/api packages'),

      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/internal/api', 'db should depend on domain and db-local packages'),
      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/internal/service', 'db should depend on domain and db-local packages'),
      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/internal/engine', 'db should depend on domain and db-local packages'),
      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/internal/middleware', 'db should depend on domain and db-local packages'),
      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/cmd', 'db should depend on domain and db-local packages'),
      ('github.com/Yacobolo/quackstack/internal/db', 'github.com/Yacobolo/quackstack/pkg/cli', 'db should depend on domain and db-local packages'),

      ('github.com/Yacobolo/quackstack/internal/engine', 'github.com/Yacobolo/quackstack/internal/api', 'engine should depend on domain and engine-local packages'),
      ('github.com/Yacobolo/quackstack/internal/engine', 'github.com/Yacobolo/quackstack/internal/service', 'engine should depend on domain and engine-local packages'),
      ('github.com/Yacobolo/quackstack/internal/engine', 'github.com/Yacobolo/quackstack/cmd', 'engine should depend on domain and engine-local packages'),
      ('github.com/Yacobolo/quackstack/internal/engine', 'github.com/Yacobolo/quackstack/pkg/cli', 'engine should depend on domain and engine-local packages'),

      ('github.com/Yacobolo/quackstack/internal/middleware', 'github.com/Yacobolo/quackstack/internal/service', 'middleware should depend on domain and middleware-local packages'),
      ('github.com/Yacobolo/quackstack/internal/middleware', 'github.com/Yacobolo/quackstack/internal/db', 'middleware should depend on domain and middleware-local packages'),
      ('github.com/Yacobolo/quackstack/internal/middleware', 'github.com/Yacobolo/quackstack/internal/engine', 'middleware should depend on domain and middleware-local packages')
),
import_edges AS (
    SELECT
      f.path AS file_path,
      'github.com/Yacobolo/quackstack/' || regexp_replace(f.path, '/[^/]+$', '') AS source_pkg,
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
WHERE i.import_path LIKE 'github.com/Yacobolo/quackstack/%'
ORDER BY i.file_path, i.import_path
`,
		},
	}
}
