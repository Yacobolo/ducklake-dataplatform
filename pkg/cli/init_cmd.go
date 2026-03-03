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
	CatalogName   string
	MetastoreType string
	MetastoreDSN  string
	DataPath      string
	Schemas       []string
	Credential    initStorageCredential
	Locations     []initLocation
	Groups        []string
	Principals    []string
	Memberships   []initMembership
	SchemaGrants  []initGrantSpec
	ServiceGrants []initGrantSpec
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
	Groups      map[string]string
	Principals  map[string]string
	Memberships map[string]map[string]bool
	Grants      map[string]bool
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
			if getOutputFormat(cmd) == "json" {
				status := "ok"
				if missing > 0 {
					status = "incomplete"
				}
				payload := map[string]interface{}{"status": status, "missing": missing, "plan": plan}
				return gen.PrintJSON(os.Stdout, payload)
			}

			if missing == 0 {
				_, _ = fmt.Fprintln(os.Stdout, "init verify: all opinionated bootstrap resources are present")
				return nil
			}
			printPlan(plan)
			return fmt.Errorf("init verify: %d resources missing; run 'duck init apply'", missing)
		},
	}
	bindInitFlags(cmd, &opts)
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
		Schemas:       []string{"bronze", "silver", "gold"},
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
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "silver", Privilege: "USAGE"},
			{PrincipalName: "data-engineers", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "analytics", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "bronze", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "silver", Privilege: "USAGE"},
			{PrincipalName: "service-accounts", PrincipalType: "group", SchemaName: "gold", Privilege: "USAGE"},
		}
		state.ServiceGrants = []initGrantSpec{
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
		Groups:      map[string]string{},
		Principals:  map[string]string{},
		Memberships: map[string]map[string]bool{},
		Grants:      map[string]bool{},
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
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"data"`
		}
		if err := doJSON(client, "GET", path, nil, nil, &schemas); err != nil {
			return state, err
		}
		for _, s := range schemas.Data {
			state.Schemas[s.Name] = s.ID
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
		if err := doNoContentOrJSON(client, "POST", path, body); err != nil {
			return fmt.Errorf("create schema %q: %w", schema, err)
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
