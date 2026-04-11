package declarative

import (
	"fmt"
	"strings"
)

// LoadNotebookResources loads and validates only notebook resources from a declarative root.
func LoadNotebookResources(root string) ([]NotebookResource, error) {
	return LoadNotebookResourcesWithOptions(root, LoadOptions{})
}

// LoadNotebookResourcesWithOptions loads and validates only notebook resources from a declarative root.
func LoadNotebookResourcesWithOptions(root string, opts LoadOptions) ([]NotebookResource, error) {
	state, err := LoadDirectoryWithOptions(root, opts)
	if err != nil {
		return nil, err
	}

	errs := Validate(state)
	if len(errs) == 0 {
		return state.Notebooks, nil
	}

	msgs := make([]string, 0, len(errs))
	for _, validationErr := range errs {
		msgs = append(msgs, validationErr.Error())
	}
	return nil, fmt.Errorf("validate notebooks: %s", strings.Join(msgs, "; "))
}
