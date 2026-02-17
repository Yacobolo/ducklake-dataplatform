package declarative

import "fmt"

func grantIdentityKey(g GrantSpec) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", g.Principal, g.PrincipalType, g.SecurableType, g.Securable, g.Privilege)
}

func effectiveGrants(state *DesiredState) []GrantSpec {
	if state == nil {
		return nil
	}

	grants := make([]GrantSpec, 0, len(state.Grants)+(len(state.Bindings)*2))
	grants = append(grants, state.Grants...)

	if len(state.PrivilegePresets) == 0 || len(state.Bindings) == 0 {
		return dedupeGrants(grants)
	}

	presetByName := make(map[string]PrivilegePresetSpec, len(state.PrivilegePresets))
	for _, p := range state.PrivilegePresets {
		presetByName[p.Name] = p
	}

	for _, b := range state.Bindings {
		preset, ok := presetByName[b.Preset]
		if !ok {
			continue
		}
		for _, privilege := range preset.Privileges {
			grants = append(grants, GrantSpec{
				Principal:     b.Principal,
				PrincipalType: b.PrincipalType,
				SecurableType: b.ScopeType,
				Securable:     b.Scope,
				Privilege:     privilege,
			})
		}
	}

	return dedupeGrants(grants)
}

func dedupeGrants(grants []GrantSpec) []GrantSpec {
	if len(grants) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(grants))
	out := make([]GrantSpec, 0, len(grants))
	for _, g := range grants {
		k := grantIdentityKey(g)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, g)
	}
	return out
}
