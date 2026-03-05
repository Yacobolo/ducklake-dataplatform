package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

type initOptions struct {
	profile        string
	env            string
	catalogName    string
	metastoreType  string
	metastoreDSN   string
	credentialName string
	prefix         string
	fromEnv        bool
	withSecurity   bool

	bucket   string
	endpoint string
	region   string
	keyID    string
	secret   string
	urlStyle string
}

type initDesiredState struct {
	CatalogName    string
	MetastoreType  string
	MetastoreDSN   string
	DataPath       string
	Schemas        []string
	SchemaLocation map[string]string
	Credential     initStorageCredential
	Locations      []initLocation
	Groups         []string
	Principals     []string
	Memberships    []initMembership
	SchemaGrants   []initGrantSpec
	ServiceGrants  []initGrantSpec
	Showcase       initShowcaseSpec
}

type initShowcaseSpec struct {
	PipelineName      string
	RawTableName      string
	BronzeViewName    string
	SilverViewName    string
	GoldViewName      string
	QualityViewName   string
	SandboxSmokeTable string
}

type initStorageCredential struct {
	Name     string
	Type     string
	Endpoint string
	Region   string
	KeyID    string
	Secret   string
	URLStyle string
}

type initLocation struct {
	Name string
	URL  string
}

type initMembership struct {
	GroupName     string
	PrincipalName string
	PrincipalType string
}

type initGrantSpec struct {
	PrincipalName string
	PrincipalType string
	SchemaName    string
	Privilege     string
}

type initExistingState struct {
	Credentials map[string]bool
	Locations   map[string]bool
	Catalogs    map[string]bool
	Schemas     map[string]string
	Tables      map[string]map[string]bool
	Views       map[string]map[string]bool
	Pipelines   map[string]bool
	Groups      map[string]string
	Principals  map[string]string
	Memberships map[string]map[string]bool
	Grants      map[string]bool
	GrantIDs    map[string]string
}

func newInitCmd(client *gen.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Bootstrap an opinionated medallion setup",
		Long:  "Creates or verifies an opinionated medallion baseline (landing + bronze/silver/gold, storage bindings, and RBAC presets).",
	}

	cmd.AddCommand(newInitPlanCmd(client))
	cmd.AddCommand(newInitApplyCmd(client))
	cmd.AddCommand(newInitVerifyCmd(client))
	cmd.AddCommand(newInitDestroyCmd(client))
	return cmd
}

func newInitPlanCmd(client *gen.Client) *cobra.Command {
	opts := defaultInitOptions()
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show what init would create",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolved, err := resolveInitOptions(opts)
			if err != nil {
				return err
			}
			desired := buildDesiredState(resolved)
			existing, err := fetchExistingState(client, desired)
			if err != nil {
				return err
			}

			plan := computeInitPlan(desired, existing)
			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, plan)
			}

			printPlan(plan)
			return nil
		},
	}
	bindInitFlags(cmd, &opts)
	return cmd
}

func newInitApplyCmd(client *gen.Client) *cobra.Command {
	opts := defaultInitOptions()
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply medallion bootstrap",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolved, err := resolveInitOptions(opts)
			if err != nil {
				return err
			}
			desired := buildDesiredState(resolved)
			existing, err := fetchExistingState(client, desired)
			if err != nil {
				return err
			}

			if !existing.Credentials[desired.Credential.Name] {
				if strings.TrimSpace(desired.Credential.KeyID) == "" || strings.TrimSpace(desired.Credential.Secret) == "" {
					return fmt.Errorf("missing S3 credentials: set S3_ACCESS_KEY and S3_SECRET_KEY (or --key-id/--secret)")
				}
			}

			if err := applyDesiredState(client, desired, existing); err != nil {
				return err
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			_, _ = fmt.Fprintln(os.Stdout, "init apply completed")
			printInitRunbook(desired)
			return nil
		},
	}
	bindInitFlags(cmd, &opts)
	return cmd
}

func newInitVerifyCmd(client *gen.Client) *cobra.Command {
	opts := defaultInitOptions()
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify medallion bootstrap state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolved, err := resolveInitOptions(opts)
			if err != nil {
				return err
			}
			desired := buildDesiredState(resolved)
			existing, err := fetchExistingState(client, desired)
			if err != nil {
				return err
			}

			plan := computeInitPlan(desired, existing)
			missing := countMissing(plan)
			healthIssues, err := runInitHealthChecks(client, desired)
			if err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				status := "ok"
				if missing > 0 || len(healthIssues) > 0 {
					status = "incomplete"
				}
				payload := map[string]interface{}{"status": status, "missing": missing, "health_issues": healthIssues, "plan": plan}
				return gen.PrintJSON(os.Stdout, payload)
			}

			if missing == 0 && len(healthIssues) == 0 {
				_, _ = fmt.Fprintln(os.Stdout, "init verify: all opinionated bootstrap resources are present")
				return nil
			}
			printPlan(plan)
			for _, issue := range healthIssues {
				_, _ = fmt.Fprintf(os.Stdout, "! %s\n", issue)
			}
			return fmt.Errorf("init verify: %d resources missing and %d health issues; run 'duck init apply'", missing, len(healthIssues))
		},
	}
	bindInitFlags(cmd, &opts)
	return cmd
}

func newInitDestroyCmd(client *gen.Client) *cobra.Command {
	opts := defaultInitOptions()
	var yes bool
	cmd := &cobra.Command{
		Use:   "destroy",
		Short: "Tear down opinionated bootstrap assets",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				if !gen.ConfirmPrompt("Destroy init-managed demo assets?") {
					return nil
				}
			}

			resolved, err := resolveInitOptions(opts)
			if err != nil {
				return err
			}
			desired := buildDesiredState(resolved)
			if err := destroyDesiredState(client, desired); err != nil {
				return err
			}

			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]string{"status": "ok"})
			}
			_, _ = fmt.Fprintln(os.Stdout, "init destroy completed")
			return nil
		},
	}
	bindInitFlags(cmd, &opts)
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip confirmation prompt")
	return cmd
}

func bindInitFlags(cmd *cobra.Command, opts *initOptions) {
	cmd.Flags().StringVar(&opts.profile, "profile", opts.profile, "Bootstrap profile (supported: medallion)")
	cmd.Flags().StringVar(&opts.env, "env", opts.env, "Environment label used in names and S3 prefixes")
	cmd.Flags().StringVar(&opts.catalogName, "catalog", opts.catalogName, "Catalog name")
	cmd.Flags().StringVar(&opts.metastoreType, "metastore-type", opts.metastoreType, "Catalog metastore type")
	cmd.Flags().StringVar(&opts.metastoreDSN, "metastore-dsn", opts.metastoreDSN, "Catalog metastore DSN")
	cmd.Flags().StringVar(&opts.credentialName, "credential-name", opts.credentialName, "Storage credential name override")
	cmd.Flags().StringVar(&opts.prefix, "prefix", opts.prefix, "Storage key prefix (defaults to <env>)")
	cmd.Flags().BoolVar(&opts.fromEnv, "from-env", opts.fromEnv, "Read S3 settings from S3_* environment variables")
	cmd.Flags().BoolVar(&opts.withSecurity, "with-security", opts.withSecurity, "Create opinionated groups/principals/grants")

	cmd.Flags().StringVar(&opts.bucket, "bucket", opts.bucket, "S3 bucket name")
	cmd.Flags().StringVar(&opts.endpoint, "endpoint", opts.endpoint, "S3 endpoint")
	cmd.Flags().StringVar(&opts.region, "region", opts.region, "S3 region")
	cmd.Flags().StringVar(&opts.keyID, "key-id", opts.keyID, "S3 access key id")
	cmd.Flags().StringVar(&opts.secret, "secret", opts.secret, "S3 secret access key")
	cmd.Flags().StringVar(&opts.urlStyle, "url-style", opts.urlStyle, "S3 URL style")
}

func defaultInitOptions() initOptions {
	return initOptions{
		profile:       "medallion",
		env:           "staging",
		catalogName:   "lake",
		metastoreType: "sqlite",
		metastoreDSN:  "./ducklake_lake.sqlite",
		fromEnv:       true,
		withSecurity:  true,
		urlStyle:      "path",
	}
}

func resolveInitOptions(opts initOptions) (initOptions, error) {
	resolved := opts
	resolved.profile = strings.ToLower(strings.TrimSpace(resolved.profile))
	resolved.env = strings.ToLower(strings.TrimSpace(resolved.env))
	resolved.catalogName = strings.TrimSpace(resolved.catalogName)
	resolved.metastoreType = strings.ToLower(strings.TrimSpace(resolved.metastoreType))
	resolved.metastoreDSN = strings.TrimSpace(resolved.metastoreDSN)
	resolved.credentialName = strings.TrimSpace(resolved.credentialName)
	resolved.prefix = strings.TrimSpace(resolved.prefix)
	resolved.bucket = strings.TrimSpace(resolved.bucket)
	resolved.endpoint = strings.TrimSpace(resolved.endpoint)
	resolved.region = strings.TrimSpace(resolved.region)
	resolved.keyID = strings.TrimSpace(resolved.keyID)
	resolved.secret = strings.TrimSpace(resolved.secret)
	resolved.urlStyle = strings.TrimSpace(resolved.urlStyle)

	if resolved.profile != "medallion" {
		return initOptions{}, fmt.Errorf("unsupported --profile %q (supported: medallion)", resolved.profile)
	}
	if resolved.env == "" {
		return initOptions{}, fmt.Errorf("--env is required")
	}
	if resolved.catalogName == "" {
		return initOptions{}, fmt.Errorf("--catalog is required")
	}
	if resolved.metastoreType == "" {
		resolved.metastoreType = "sqlite"
	}
	if resolved.metastoreDSN == "" {
		resolved.metastoreDSN = "./ducklake_lake.sqlite"
	}
	if resolved.urlStyle == "" {
		resolved.urlStyle = "path"
	}

	if resolved.fromEnv {
		if resolved.bucket == "" {
			resolved.bucket = strings.TrimSpace(os.Getenv("S3_BUCKET"))
		}
		if resolved.endpoint == "" {
			resolved.endpoint = strings.TrimSpace(os.Getenv("S3_ENDPOINT"))
		}
		if resolved.region == "" {
			resolved.region = strings.TrimSpace(os.Getenv("S3_REGION"))
		}
		if resolved.keyID == "" {
			resolved.keyID = strings.TrimSpace(os.Getenv("S3_ACCESS_KEY"))
		}
		if resolved.secret == "" {
			resolved.secret = strings.TrimSpace(os.Getenv("S3_SECRET_KEY"))
		}
	}

	if resolved.credentialName == "" {
		resolved.credentialName = fmt.Sprintf("%s-default-s3", resolved.env)
	}
	if resolved.prefix == "" {
		resolved.prefix = resolved.env
	}
	if resolved.bucket == "" {
		return initOptions{}, fmt.Errorf("S3 bucket is required (set --bucket or S3_BUCKET)")
	}
	if resolved.endpoint == "" {
		return initOptions{}, fmt.Errorf("S3 endpoint is required (set --endpoint or S3_ENDPOINT)")
	}
	if resolved.region == "" {
		return initOptions{}, fmt.Errorf("S3 region is required (set --region or S3_REGION)")
	}

	return resolved, nil
}

func buildDesiredState(opts initOptions) initDesiredState {
	prefix := trimPathPrefix(opts.prefix)
	base := fmt.Sprintf("s3://%s/%s", opts.bucket, prefix)

	state := initDesiredState{
		CatalogName:   opts.catalogName,
		MetastoreType: opts.metastoreType,
		MetastoreDSN:  opts.metastoreDSN,
		DataPath:      base,
		Schemas:       []string{"landing", "bronze", "silver", "gold", "sandbox"},
		SchemaLocation: map[string]string{
			"landing": fmt.Sprintf("%s-landing", opts.env),
			"bronze":  fmt.Sprintf("%s-bronze", opts.env),
			"silver":  fmt.Sprintf("%s-silver", opts.env),
			"gold":    fmt.Sprintf("%s-gold", opts.env),
			"sandbox": fmt.Sprintf("%s-sandbox", opts.env),
		},
		Credential: initStorageCredential{
			Name:     opts.credentialName,
			Type:     "S3",
			Endpoint: opts.endpoint,
			Region:   opts.region,
			KeyID:    opts.keyID,
			Secret:   opts.secret,
			URLStyle: opts.urlStyle,
		},
		Locations: []initLocation{
			{Name: fmt.Sprintf("%s-landing", opts.env), URL: base + "landing/"},
			{Name: fmt.Sprintf("%s-bronze", opts.env), URL: base + "bronze/"},
			{Name: fmt.Sprintf("%s-silver", opts.env), URL: base + "silver/"},
			{Name: fmt.Sprintf("%s-gold", opts.env), URL: base + "gold/"},
			{Name: fmt.Sprintf("%s-sandbox", opts.env), URL: base + "sandbox/"},
		},
		Showcase: initShowcaseSpec{
			PipelineName:      "rides_demo",
			RawTableName:      "rides_raw",
			BronzeViewName:    "rides_bronze_trips",
			SilverViewName:    "rides_silver_trips",
			GoldViewName:      "rides_gold_daily_metrics",
			QualityViewName:   "rides_quality_checks",
			SandboxSmokeTable: "sandbox_getting_started",
		},
	}

	if opts.withSecurity {
		state.Groups = []string{"platform-admins", "data-engineers", "analytics", "service-accounts"}
		state.Principals = []string{"svc-ingest", "svc-transform", "svc-bi"}
		state.Memberships = []initMembership{
			{GroupName: "service-accounts", PrincipalName: "svc-ingest", PrincipalType: "user"},
			{GroupName: "service-accounts", PrincipalName: "svc-transform", PrincipalType: "user"},
			{GroupName: "service-accounts", PrincipalName: "svc-bi", PrincipalType: "user"},
			{GroupName: "data-engineers", PrincipalName: "svc-ingest", PrincipalType: "user"},
			{GroupName: "data-engineers", PrincipalName: "svc-transform", PrincipalType: "user"},
			{GroupName: "analytics", PrincipalName: "svc-bi", PrincipalType: "user"},
		}
		state.SchemaGrants = []initGrantSpec{
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "landing", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "silver", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "sandbox", Privilege: "USAGE"},
			{PrincipalName: "analytics", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "analytics", PrincipalType: "group", SchemaName: "sandbox", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "landing", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "silver", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "sandbox", Privilege: "USAGE"},
		}
		state.ServiceGrants = []initGrantSpec{
			{PrincipalName: "svc-ingest", PrincipalType: "user", SchemaName: "landing", Privilege: "USAGE"},
			{PrincipalName: "svc-transform", PrincipalType: "user", SchemaName: "landing", Privilege: "USAGE"},
			{PrincipalName: "svc-ingest", PrincipalType: "user", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "svc-transform", PrincipalType: "user", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "svc-transform", PrincipalType: "user", SchemaName: "silver", Privilege: "USAGE"},
			{PrincipalName: "svc-transform", PrincipalType: "user", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "svc-bi", PrincipalType: "user", SchemaName: "gold", Privilege: "USAGE"},
		}
	}

	return state
}

func trimPathPrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func fetchExistingState(client *gen.Client, desired initDesiredState) (initExistingState, error) {
	state := initExistingState{
		Credentials: map[string]bool{},
		Locations:   map[string]bool{},
		Catalogs:    map[string]bool{},
		Schemas:     map[string]string{},
		Tables:      map[string]map[string]bool{},
		Views:       map[string]map[string]bool{},
		Pipelines:   map[string]bool{},
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
		GrantIDs:    map[string]string{},
	}

	var creds struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := doJSON(client, "GET", "/storage-credentials", nil, nil, &creds); err != nil {
		return state, err
	}
	for _, c := range creds.Data {
		state.Credentials[c.Name] = true
	}

	var locs struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := doJSON(client, "GET", "/external-locations", nil, nil, &locs); err != nil {
		return state, err
	}
	for _, l := range locs.Data {
		state.Locations[l.Name] = true
	}

	var catalogs struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := doJSON(client, "GET", "/catalogs", nil, nil, &catalogs); err != nil {
		return state, err
	}
	for _, c := range catalogs.Data {
		state.Catalogs[c.Name] = true
	}

	if state.Catalogs[desired.CatalogName] {
		path := fmt.Sprintf("/catalogs/%s/schemas", desired.CatalogName)
		var schemas struct {
			Data []struct {
				ID       string `json:"id"`
				SchemaID string `json:"schema_id"`
				Name     string `json:"name"`
			} `json:"data"`
		}
		if err := doJSON(client, "GET", path, nil, nil, &schemas); err != nil {
			return state, err
		}
		for _, s := range schemas.Data {
			id := s.SchemaID
			if id == "" {
				id = s.ID
			}
			state.Schemas[s.Name] = id
		}

		for _, schemaName := range desired.Schemas {
			tablesPath := fmt.Sprintf("/catalogs/%s/schemas/%s/tables", desired.CatalogName, schemaName)
			var tables struct {
				Data []struct {
					Name string `json:"name"`
				} `json:"data"`
			}
			if err := doJSON(client, "GET", tablesPath, nil, nil, &tables); err == nil {
				if state.Tables[schemaName] == nil {
					state.Tables[schemaName] = map[string]bool{}
				}
				for _, table := range tables.Data {
					state.Tables[schemaName][table.Name] = true
				}
			}

			viewsPath := fmt.Sprintf("/catalogs/%s/schemas/%s/views", desired.CatalogName, schemaName)
			var views struct {
				Data []struct {
					Name string `json:"name"`
				} `json:"data"`
			}
			if err := doJSON(client, "GET", viewsPath, nil, nil, &views); err == nil {
				if state.Views[schemaName] == nil {
					state.Views[schemaName] = map[string]bool{}
				}
				for _, view := range views.Data {
					state.Views[schemaName][view.Name] = true
				}
			}
		}
	}

	var pipelines struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := doJSON(client, "GET", "/pipelines", nil, nil, &pipelines); err == nil {
		for _, pipeline := range pipelines.Data {
			state.Pipelines[pipeline.Name] = true
		}
	}

	if len(desired.Groups) > 0 {
		var groups struct {
			Data []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"data"`
		}
		if err := doJSON(client, "GET", "/groups", nil, nil, &groups); err != nil {
			return state, err
		}
		for _, g := range groups.Data {
			state.Groups[g.Name] = g.ID
		}
	}

	if len(desired.Principals) > 0 {
		var principals struct {
			Data []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"data"`
		}
		if err := doJSON(client, "GET", "/principals", nil, nil, &principals); err != nil {
			return state, err
		}
		for _, p := range principals.Data {
			state.Principals[p.Name] = p.ID
		}
	}

	if len(desired.Memberships) > 0 {
		for _, membership := range desired.Memberships {
			groupID, ok := state.Groups[membership.GroupName]
			if !ok {
				continue
			}
			path := fmt.Sprintf("/groups/%s/members", groupID)
			var members struct {
				Data []struct {
					MemberID string `json:"member_id"`
				} `json:"data"`
			}
			if err := doJSON(client, "GET", path, nil, nil, &members); err != nil {
				return state, err
			}
			if state.Memberships[groupID] == nil {
				state.Memberships[groupID] = map[string]bool{}
			}
			for _, m := range members.Data {
				state.Memberships[groupID][m.MemberID] = true
			}
		}
	}

	if len(desired.SchemaGrants)+len(desired.ServiceGrants) > 0 {
		var grants struct {
			Data []struct {
				ID            string `json:"id"`
				GrantID       string `json:"grant_id"`
				PrincipalID   string `json:"principal_id"`
				PrincipalType string `json:"principal_type"`
				SecurableID   string `json:"securable_id"`
				SecurableType string `json:"securable_type"`
				Privilege     string `json:"privilege"`
			} `json:"data"`
		}
		if err := doJSON(client, "GET", "/grants", nil, nil, &grants); err != nil {
			return state, err
		}
		for _, g := range grants.Data {
			key := grantKey(g.PrincipalID, g.PrincipalType, g.SecurableID, g.SecurableType, g.Privilege)
			state.Grants[key] = true
			grantID := strings.TrimSpace(g.GrantID)
			if grantID == "" {
				grantID = strings.TrimSpace(g.ID)
			}
			if grantID != "" {
				state.GrantIDs[key] = grantID
			}
		}
	}

	return state, nil
}

type initPlan struct {
	Creates []string `json:"creates"`
	Exists  []string `json:"exists"`
}

func computeInitPlan(desired initDesiredState, existing initExistingState) initPlan {
	plan := initPlan{Creates: []string{}, Exists: []string{}}

	if existing.Credentials[desired.Credential.Name] {
		plan.Exists = append(plan.Exists, fmt.Sprintf("storage credential %q", desired.Credential.Name))
	} else {
		plan.Creates = append(plan.Creates, fmt.Sprintf("storage credential %q", desired.Credential.Name))
	}

	for _, loc := range desired.Locations {
		if existing.Locations[loc.Name] {
			plan.Exists = append(plan.Exists, fmt.Sprintf("storage location %q", loc.Name))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("storage location %q", loc.Name))
		}
	}

	if existing.Catalogs[desired.CatalogName] {
		plan.Exists = append(plan.Exists, fmt.Sprintf("catalog %q", desired.CatalogName))
	} else {
		plan.Creates = append(plan.Creates, fmt.Sprintf("catalog %q", desired.CatalogName))
	}

	for _, schema := range desired.Schemas {
		if _, ok := existing.Schemas[schema]; ok {
			plan.Exists = append(plan.Exists, fmt.Sprintf("schema %q", schema))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("schema %q", schema))
		}
	}

	for _, group := range desired.Groups {
		if _, ok := existing.Groups[group]; ok {
			plan.Exists = append(plan.Exists, fmt.Sprintf("group %q", group))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("group %q", group))
		}
	}

	for _, principal := range desired.Principals {
		if _, ok := existing.Principals[principal]; ok {
			plan.Exists = append(plan.Exists, fmt.Sprintf("principal %q", principal))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("principal %q", principal))
		}
	}

	for _, membership := range desired.Memberships {
		groupID, hasGroup := existing.Groups[membership.GroupName]
		memberID, hasPrincipal := existing.Principals[membership.PrincipalName]
		if hasGroup && hasPrincipal && existing.Memberships[groupID] != nil && existing.Memberships[groupID][memberID] {
			plan.Exists = append(plan.Exists, fmt.Sprintf("membership %q <- %q", membership.GroupName, membership.PrincipalName))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("membership %q <- %q", membership.GroupName, membership.PrincipalName))
		}
	}

	for _, grant := range append([]initGrantSpec{}, append(desired.SchemaGrants, desired.ServiceGrants...)...) {
		principalID, hasPrincipal := principalIDForGrant(existing, grant)
		schemaID, hasSchema := existing.Schemas[grant.SchemaName]
		if hasPrincipal && hasSchema {
			k := grantKey(principalID, grant.PrincipalType, schemaID, "schema", grant.Privilege)
			if existing.Grants[k] {
				plan.Exists = append(plan.Exists, fmt.Sprintf("grant %s on schema %q to %s %q", grant.Privilege, grant.SchemaName, grant.PrincipalType, grant.PrincipalName))
				continue
			}
		}
		plan.Creates = append(plan.Creates, fmt.Sprintf("grant %s on schema %q to %s %q", grant.Privilege, grant.SchemaName, grant.PrincipalType, grant.PrincipalName))
	}

	if existing.Tables["landing"] != nil && existing.Tables["landing"][desired.Showcase.RawTableName] {
		plan.Exists = append(plan.Exists, fmt.Sprintf("showcase table %q.%q", "landing", desired.Showcase.RawTableName))
	} else {
		plan.Creates = append(plan.Creates, fmt.Sprintf("showcase table %q.%q", "landing", desired.Showcase.RawTableName))
	}

	viewChecks := []struct {
		schema string
		name   string
	}{
		{schema: "bronze", name: desired.Showcase.BronzeViewName},
		{schema: "silver", name: desired.Showcase.SilverViewName},
		{schema: "gold", name: desired.Showcase.GoldViewName},
		{schema: "gold", name: desired.Showcase.QualityViewName},
	}
	for _, view := range viewChecks {
		if existing.Views[view.schema] != nil && existing.Views[view.schema][view.name] {
			plan.Exists = append(plan.Exists, fmt.Sprintf("showcase view %q.%q", view.schema, view.name))
		} else {
			plan.Creates = append(plan.Creates, fmt.Sprintf("showcase view %q.%q", view.schema, view.name))
		}
	}

	if existing.Tables["sandbox"] != nil && existing.Tables["sandbox"][desired.Showcase.SandboxSmokeTable] {
		plan.Exists = append(plan.Exists, fmt.Sprintf("sandbox table %q.%q", "sandbox", desired.Showcase.SandboxSmokeTable))
	} else {
		plan.Creates = append(plan.Creates, fmt.Sprintf("sandbox table %q.%q", "sandbox", desired.Showcase.SandboxSmokeTable))
	}

	if existing.Pipelines[desired.Showcase.PipelineName] {
		plan.Exists = append(plan.Exists, fmt.Sprintf("pipeline %q", desired.Showcase.PipelineName))
	} else {
		plan.Creates = append(plan.Creates, fmt.Sprintf("pipeline %q", desired.Showcase.PipelineName))
	}

	sort.Strings(plan.Creates)
	sort.Strings(plan.Exists)
	return plan
}

func printPlan(plan initPlan) {
	for _, c := range plan.Creates {
		_, _ = fmt.Fprintf(os.Stdout, "+ %s\n", c)
	}
	for _, e := range plan.Exists {
		_, _ = fmt.Fprintf(os.Stdout, "= %s\n", e)
	}
	_, _ = fmt.Fprintf(os.Stdout, "\nPlan: %d to create, %d already present.\n", len(plan.Creates), len(plan.Exists))
}

func countMissing(plan initPlan) int {
	return len(plan.Creates)
}

func applyDesiredState(client *gen.Client, desired initDesiredState, existing initExistingState) error {
	if !existing.Credentials[desired.Credential.Name] {
		body := map[string]interface{}{
			"name":            desired.Credential.Name,
			"credential_type": desired.Credential.Type,
			"endpoint":        desired.Credential.Endpoint,
			"region":          desired.Credential.Region,
			"key_id":          desired.Credential.KeyID,
			"secret":          desired.Credential.Secret,
			"url_style":       desired.Credential.URLStyle,
		}
		if err := doNoContentOrJSON(client, "POST", "/storage-credentials", body); err != nil {
			return fmt.Errorf("create storage credential %q: %w", desired.Credential.Name, err)
		}
	}

	for _, loc := range desired.Locations {
		if existing.Locations[loc.Name] {
			continue
		}
		body := map[string]interface{}{
			"name":            loc.Name,
			"credential_name": desired.Credential.Name,
			"storage_type":    "S3",
			"url":             loc.URL,
		}
		if err := doNoContentOrJSON(client, "POST", "/external-locations", body); err != nil {
			return fmt.Errorf("create storage location %q: %w", loc.Name, err)
		}
	}

	if !existing.Catalogs[desired.CatalogName] {
		body := map[string]interface{}{
			"name":           desired.CatalogName,
			"metastore_type": desired.MetastoreType,
			"dsn":            desired.MetastoreDSN,
			"data_path":      desired.DataPath,
		}
		if err := doNoContentOrJSON(client, "POST", "/catalogs", body); err != nil {
			return fmt.Errorf("create catalog %q: %w", desired.CatalogName, err)
		}
	}

	current, err := fetchExistingState(client, desired)
	if err != nil {
		return err
	}

	for _, schema := range desired.Schemas {
		if _, ok := current.Schemas[schema]; ok {
			continue
		}
		path := fmt.Sprintf("/catalogs/%s/schemas", desired.CatalogName)
		body := map[string]interface{}{"name": schema}
		if locationName, ok := desired.SchemaLocation[schema]; ok && strings.TrimSpace(locationName) != "" {
			body["location_name"] = locationName
		}
		if err := doNoContentOrJSON(client, "POST", path, body); err != nil {
			return fmt.Errorf("create schema %q: %w", schema, err)
		}
	}

	if err := applyShowcaseState(client, desired); err != nil {
		return err
	}

	if !current.Pipelines[desired.Showcase.PipelineName] {
		pipelineBody := map[string]interface{}{
			"name":              desired.Showcase.PipelineName,
			"description":       "Opinionated medallion demo pipeline (manual run)",
			"concurrency_limit": 1,
			"is_paused":         false,
		}
		if err := doNoContentOrJSON(client, "POST", "/pipelines", pipelineBody); err != nil {
			return fmt.Errorf("create pipeline %q: %w", desired.Showcase.PipelineName, err)
		}
	}

	if len(desired.Groups) == 0 && len(desired.Principals) == 0 {
		return nil
	}

	current, err = fetchExistingState(client, desired)
	if err != nil {
		return err
	}

	for _, group := range desired.Groups {
		if _, ok := current.Groups[group]; ok {
			continue
		}
		if err := doNoContentOrJSON(client, "POST", "/groups", map[string]interface{}{"name": group}); err != nil {
			return fmt.Errorf("create group %q: %w", group, err)
		}
	}

	for _, principal := range desired.Principals {
		if _, ok := current.Principals[principal]; ok {
			continue
		}
		body := map[string]interface{}{"name": principal, "type": "user", "is_admin": false}
		if err := doNoContentOrJSON(client, "POST", "/principals", body); err != nil {
			return fmt.Errorf("create principal %q: %w", principal, err)
		}
	}

	current, err = fetchExistingState(client, desired)
	if err != nil {
		return err
	}

	for _, membership := range desired.Memberships {
		groupID, ok := current.Groups[membership.GroupName]
		if !ok {
			continue
		}
		memberID, ok := current.Principals[membership.PrincipalName]
		if !ok {
			continue
		}
		if current.Memberships[groupID] != nil && current.Memberships[groupID][memberID] {
			continue
		}
		path := fmt.Sprintf("/groups/%s/members", groupID)
		body := map[string]interface{}{"member_id": memberID, "member_type": membership.PrincipalType}
		if err := doNoContentOrJSON(client, "POST", path, body); err != nil {
			return fmt.Errorf("add membership %q <- %q: %w", membership.GroupName, membership.PrincipalName, err)
		}
	}

	for _, grant := range append([]initGrantSpec{}, append(desired.SchemaGrants, desired.ServiceGrants...)...) {
		principalID, ok := principalIDForGrant(current, grant)
		if !ok {
			continue
		}
		schemaID, ok := current.Schemas[grant.SchemaName]
		if !ok {
			continue
		}
		key := grantKey(principalID, grant.PrincipalType, schemaID, "schema", grant.Privilege)
		if current.Grants[key] {
			continue
		}
		body := map[string]interface{}{
			"principal_id":   principalID,
			"principal_type": grant.PrincipalType,
			"securable_id":   schemaID,
			"securable_type": "schema",
			"privilege":      grant.Privilege,
		}
		if err := doNoContentOrJSON(client, "POST", "/grants", body); err != nil {
			return fmt.Errorf("create grant on %q for %q: %w", grant.SchemaName, grant.PrincipalName, err)
		}
	}

	return nil
}

func applyShowcaseState(client *gen.Client, desired initDesiredState) error {
	rawRef := qualifiedName(desired.CatalogName, "landing", desired.Showcase.RawTableName)
	bronzeRef := qualifiedName(desired.CatalogName, "bronze", desired.Showcase.BronzeViewName)
	silverRef := qualifiedName(desired.CatalogName, "silver", desired.Showcase.SilverViewName)
	goldRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.GoldViewName)
	qualityRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.QualityViewName)
	sandboxRef := qualifiedName(desired.CatalogName, "sandbox", desired.Showcase.SandboxSmokeTable)

	createRawSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		ride_id BIGINT,
		pickup_ts TIMESTAMP,
		dropoff_ts TIMESTAMP,
		pickup_zone VARCHAR,
		dropoff_zone VARCHAR,
		passenger_count INTEGER,
		trip_distance_mi DOUBLE,
		fare_amount DOUBLE,
		tip_amount DOUBLE,
		total_amount DOUBLE
	)`, rawRef)
	if err := executeSQL(client, createRawSQL); err != nil {
		return fmt.Errorf("create showcase table: %w", err)
	}

	rowCount, err := queryScalarInt64(client, fmt.Sprintf("SELECT COUNT(*) FROM %s", rawRef))
	if err != nil {
		return fmt.Errorf("query showcase row count: %w", err)
	}
	if rowCount == 0 {
		seedSQL := fmt.Sprintf(`INSERT INTO %s (ride_id, pickup_ts, dropoff_ts, pickup_zone, dropoff_zone, passenger_count, trip_distance_mi, fare_amount, tip_amount, total_amount) VALUES
		(1, '2026-02-01 08:05:00', '2026-02-01 08:22:00', 'Midtown East', 'Chelsea', 1, 3.2, 14.50, 3.20, 20.70),
		(2, '2026-02-01 09:10:00', '2026-02-01 09:31:00', 'SoHo', 'Upper West Side', 2, 5.8, 22.40, 5.10, 31.00),
		(3, '2026-02-01 11:48:00', '2026-02-01 12:03:00', 'Lower East Side', 'Financial District', 1, 2.4, 11.80, 2.40, 17.70),
		(4, '2026-02-02 07:25:00', '2026-02-02 07:55:00', 'Harlem', 'LaGuardia Airport', 3, 8.9, 35.10, 7.00, 47.60),
		(5, '2026-02-02 18:15:00', '2026-02-02 18:36:00', 'Chelsea', 'Brooklyn Heights', 1, 4.1, 17.20, 3.80, 24.30),
		(6, '2026-02-03 14:02:00', '2026-02-03 14:28:00', 'Union Square', 'Long Island City', 2, 6.5, 24.70, 5.60, 33.90),
		(7, '2026-02-03 21:41:00', '2026-02-03 22:05:00', 'West Village', 'JFK Airport', 1, 7.1, 38.20, 9.00, 51.70),
		(8, '2026-02-04 10:33:00', '2026-02-04 10:49:00', 'Tribeca', 'Midtown West', 1, 2.9, 13.60, 2.90, 19.00),
		(9, '2026-02-04 16:05:00', '2026-02-04 16:44:00', 'Upper East Side', 'Coney Island', 2, 9.7, 41.90, 8.30, 55.45),
		(10, '2026-02-05 06:50:00', '2026-02-05 07:12:00', 'Astoria', 'Times Square', 1, 4.6, 18.70, 4.20, 25.40)`, rawRef)
		if err := executeSQL(client, seedSQL); err != nil {
			return fmt.Errorf("seed showcase table: %w", err)
		}
	}

	createBronzeViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
	SELECT
		ride_id,
		pickup_ts,
		dropoff_ts,
		pickup_zone,
		dropoff_zone,
		COALESCE(passenger_count, 1) AS passenger_count,
		trip_distance_mi,
		fare_amount,
		tip_amount,
		total_amount
	FROM %s`, bronzeRef, rawRef)
	if err := executeSQL(client, createBronzeViewSQL); err != nil {
		return fmt.Errorf("create bronze showcase view: %w", err)
	}

	createSilverViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
	SELECT
		ride_id,
		pickup_ts,
		dropoff_ts,
		pickup_zone,
		dropoff_zone,
		passenger_count,
		trip_distance_mi,
		fare_amount,
		tip_amount,
		total_amount,
		ROUND(CASE WHEN total_amount > 0 THEN tip_amount / total_amount ELSE 0 END, 4) AS tip_rate
	FROM %s
	WHERE trip_distance_mi > 0
	  AND total_amount > 0`, silverRef, bronzeRef)
	if err := executeSQL(client, createSilverViewSQL); err != nil {
		return fmt.Errorf("create silver showcase view: %w", err)
	}

	createGoldViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
	SELECT
		CAST(date_trunc('day', pickup_ts) AS DATE) AS trip_date,
		COUNT(*) AS ride_count,
		ROUND(SUM(total_amount), 2) AS gross_revenue,
		ROUND(AVG(total_amount), 2) AS avg_fare,
		ROUND(AVG(trip_distance_mi), 2) AS avg_distance_mi
	FROM %s
	GROUP BY 1
	ORDER BY 1`, goldRef, silverRef)
	if err := executeSQL(client, createGoldViewSQL); err != nil {
		return fmt.Errorf("create gold showcase view: %w", err)
	}

	createQualitySQL := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
	SELECT 'silver_positive_distance' AS check_name, SUM(CASE WHEN trip_distance_mi <= 0 THEN 1 ELSE 0 END) = 0 AS passed FROM %s
	UNION ALL
	SELECT 'silver_non_null_pickup', SUM(CASE WHEN pickup_ts IS NULL THEN 1 ELSE 0 END) = 0 AS passed FROM %s
	UNION ALL
	SELECT 'gold_non_empty', COUNT(*) > 0 AS passed FROM %s`, qualityRef, silverRef, silverRef, goldRef)
	if err := executeSQL(client, createQualitySQL); err != nil {
		return fmt.Errorf("create quality showcase view: %w", err)
	}

	createSandboxSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT,
		note VARCHAR
	)`, sandboxRef)
	if err := executeSQL(client, createSandboxSQL); err != nil {
		return fmt.Errorf("create sandbox smoke table: %w", err)
	}

	return nil
}

func runInitHealthChecks(client *gen.Client, desired initDesiredState) ([]string, error) {
	issues := make([]string, 0)

	rawRef := qualifiedName(desired.CatalogName, "landing", desired.Showcase.RawTableName)
	goldRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.GoldViewName)
	qualityRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.QualityViewName)

	rawCount, err := queryScalarInt64(client, fmt.Sprintf("SELECT COUNT(*) FROM %s", rawRef))
	if err != nil {
		issues = append(issues, fmt.Sprintf("unable to query showcase table %q: %v", rawRef, err))
		return issues, nil
	}
	if rawCount == 0 {
		issues = append(issues, fmt.Sprintf("showcase table %q is empty", rawRef))
	}

	goldCount, err := queryScalarInt64(client, fmt.Sprintf("SELECT COUNT(*) FROM %s", goldRef))
	if err != nil {
		issues = append(issues, fmt.Sprintf("unable to query gold output %q: %v", goldRef, err))
		return issues, nil
	}
	if goldCount == 0 {
		issues = append(issues, fmt.Sprintf("gold output %q has no rows", goldRef))
	}

	failingChecks, err := queryScalarInt64(client, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE passed = false", qualityRef))
	if err != nil {
		issues = append(issues, fmt.Sprintf("unable to query quality checks %q: %v", qualityRef, err))
		return issues, nil
	}
	if failingChecks > 0 {
		issues = append(issues, fmt.Sprintf("%d quality checks failed in %q", failingChecks, qualityRef))
	}

	return issues, nil
}

func destroyDesiredState(client *gen.Client, desired initDesiredState) error {
	pipelinePath := fmt.Sprintf("/pipelines/%s", desired.Showcase.PipelineName)
	if err := doNoContentOrJSONAllowNotFound(client, "DELETE", pipelinePath, nil); err != nil {
		return fmt.Errorf("delete pipeline %q: %w", desired.Showcase.PipelineName, err)
	}

	goldRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.GoldViewName)
	qualityRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.QualityViewName)
	silverRef := qualifiedName(desired.CatalogName, "silver", desired.Showcase.SilverViewName)
	bronzeRef := qualifiedName(desired.CatalogName, "bronze", desired.Showcase.BronzeViewName)
	rawRef := qualifiedName(desired.CatalogName, "landing", desired.Showcase.RawTableName)
	sandboxRef := qualifiedName(desired.CatalogName, "sandbox", desired.Showcase.SandboxSmokeTable)

	dropSQL := []string{
		fmt.Sprintf("DROP VIEW IF EXISTS %s", qualityRef),
		fmt.Sprintf("DROP VIEW IF EXISTS %s", goldRef),
		fmt.Sprintf("DROP VIEW IF EXISTS %s", silverRef),
		fmt.Sprintf("DROP VIEW IF EXISTS %s", bronzeRef),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", rawRef),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", sandboxRef),
	}
	for _, sql := range dropSQL {
		if err := executeSQL(client, sql); err != nil {
			return fmt.Errorf("destroy showcase asset: %w", err)
		}
	}

	return nil
}

func printInitRunbook(desired initDesiredState) {
	goldRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.GoldViewName)
	qualityRef := qualifiedName(desired.CatalogName, "gold", desired.Showcase.QualityViewName)
	sandboxRef := qualifiedName(desired.CatalogName, "sandbox", desired.Showcase.SandboxSmokeTable)

	_, _ = fmt.Fprintln(os.Stdout, "")
	_, _ = fmt.Fprintln(os.Stdout, "Next steps:")
	_, _ = fmt.Fprintf(os.Stdout, "  1) Run pipeline now: duck pipelines runs create %s\n", desired.Showcase.PipelineName)
	_, _ = fmt.Fprintf(os.Stdout, "  2) Query gold output: duck query execute --sql \"SELECT * FROM %s ORDER BY trip_date LIMIT 10\"\n", goldRef)
	_, _ = fmt.Fprintf(os.Stdout, "  3) Check data quality: duck query execute --sql \"SELECT * FROM %s\"\n", qualityRef)
	_, _ = fmt.Fprintf(os.Stdout, "  4) Use sandbox safely: duck query execute --sql \"SELECT COUNT(*) FROM %s\"\n", sandboxRef)
	_, _ = fmt.Fprintln(os.Stdout, "  5) Tear down demo assets: duck init destroy")
}

func executeSQL(client *gen.Client, sql string) error {
	resp, err := client.Do("POST", "/query", nil, map[string]interface{}{"sql": sql})
	if err != nil {
		return err
	}
	if err := gen.CheckError(resp); err != nil {
		return err
	}
	_, err = gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	return nil
}

func queryScalarInt64(client *gen.Client, sql string) (int64, error) {
	var payload struct {
		Rows [][]interface{} `json:"rows"`
	}
	if err := doJSON(client, "POST", "/query", nil, map[string]interface{}{"sql": sql}, &payload); err != nil {
		return 0, err
	}
	if len(payload.Rows) == 0 || len(payload.Rows[0]) == 0 {
		return 0, fmt.Errorf("empty result set")
	}
	value := payload.Rows[0][0]
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unexpected scalar type %T", value)
	}
}

func qualifiedName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

func principalIDForGrant(state initExistingState, grant initGrantSpec) (string, bool) {
	if grant.PrincipalType == "group" {
		id, ok := state.Groups[grant.PrincipalName]
		return id, ok
	}
	id, ok := state.Principals[grant.PrincipalName]
	return id, ok
}

func grantKey(principalID, principalType, securableID, securableType, privilege string) string {
	return strings.ToLower(strings.Join([]string{principalID, principalType, securableID, securableType, privilege}, "|"))
}

func doJSON(client *gen.Client, method, path string, query url.Values, body interface{}, out interface{}) error {
	resp, err := client.Do(method, path, query, body)
	if err != nil {
		return err
	}
	if err := gen.CheckError(resp); err != nil {
		return err
	}
	respBody, err := gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

func doNoContentOrJSON(client *gen.Client, method, path string, body interface{}) error {
	resp, err := client.Do(method, path, nil, body)
	if err != nil {
		return err
	}
	if err := gen.CheckError(resp); err != nil {
		var apiErr *gen.APIError
		if errors.As(err, &apiErr) && apiErr.HTTPStatus == 409 {
			return nil
		}
		return err
	}
	_, err = gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	return nil
}

func doNoContentOrJSONAllowNotFound(client *gen.Client, method, path string, body interface{}) error {
	resp, err := client.Do(method, path, nil, body)
	if err != nil {
		return err
	}
	if err := gen.CheckError(resp); err != nil {
		var apiErr *gen.APIError
		if errors.As(err, &apiErr) && (apiErr.HTTPStatus == 404 || apiErr.HTTPStatus == 409) {
			return nil
		}
		return err
	}
	_, err = gen.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	return nil
}
