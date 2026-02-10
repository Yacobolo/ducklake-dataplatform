package engine

import (
	pb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
)

// resolveTableName extracts the table name from a NamedTable's compound
// identifier. In Substrait, Names is a compound identifier like
// ["catalog", "schema", "table"]. The actual table name is the last element.
// Returns empty string if names is empty.
func resolveTableName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[len(names)-1]
}

// ExtractTableNames returns deduplicated table names found in
// NamedTable ReadRel nodes across the entire plan.
func ExtractTableNames(plan *pb.Plan) []string {
	if plan == nil {
		return nil
	}

	seen := make(map[string]bool)
	var tables []string

	for _, rel := range plan.GetRelations() {
		if root := rel.GetRoot(); root != nil {
			walkRel(root.GetInput(), seen, &tables)
		}
		if bareRel := rel.GetRel(); bareRel != nil {
			walkRel(bareRel, seen, &tables)
		}
	}

	return tables
}

// walkRel recursively traverses a Rel tree, collecting NamedTable names.
func walkRel(rel *pb.Rel, seen map[string]bool, tables *[]string) {
	if rel == nil {
		return
	}

	switch r := rel.RelType.(type) {
	case *pb.Rel_Read:
		if nt := r.Read.GetNamedTable(); nt != nil {
			name := resolveTableName(nt.GetNames())
			if name != "" && !seen[name] {
				seen[name] = true
				*tables = append(*tables, name)
			}
		}

	case *pb.Rel_Project:
		walkRel(r.Project.GetInput(), seen, tables)

	case *pb.Rel_Filter:
		walkRel(r.Filter.GetInput(), seen, tables)

	case *pb.Rel_Fetch:
		walkRel(r.Fetch.GetInput(), seen, tables)

	case *pb.Rel_Sort:
		walkRel(r.Sort.GetInput(), seen, tables)

	case *pb.Rel_Aggregate:
		walkRel(r.Aggregate.GetInput(), seen, tables)

	case *pb.Rel_Join:
		walkRel(r.Join.GetLeft(), seen, tables)
		walkRel(r.Join.GetRight(), seen, tables)

	case *pb.Rel_Cross:
		walkRel(r.Cross.GetLeft(), seen, tables)
		walkRel(r.Cross.GetRight(), seen, tables)

	case *pb.Rel_HashJoin:
		walkRel(r.HashJoin.GetLeft(), seen, tables)
		walkRel(r.HashJoin.GetRight(), seen, tables)

	case *pb.Rel_MergeJoin:
		walkRel(r.MergeJoin.GetLeft(), seen, tables)
		walkRel(r.MergeJoin.GetRight(), seen, tables)

	case *pb.Rel_Set:
		for _, input := range r.Set.GetInputs() {
			walkRel(input, seen, tables)
		}

	case *pb.Rel_ExtensionSingle:
		walkRel(r.ExtensionSingle.GetInput(), seen, tables)

	case *pb.Rel_ExtensionMulti:
		for _, input := range r.ExtensionMulti.GetInputs() {
			walkRel(input, seen, tables)
		}
	}
}
