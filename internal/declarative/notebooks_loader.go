package declarative

import (
	"fmt"
	"os"
	"strings"
)

// LoadNotebookResources loads and validates only notebook resources from a declarative root.
func LoadNotebookResources(root string) ([]NotebookResource, error) {
	return LoadNotebookResourcesWithOptions(root, LoadOptions{})
}

// LoadNotebookResourcesWithOptions loads and validates only notebook resources from a declarative root.
func LoadNotebookResourcesWithOptions(root string, opts LoadOptions) ([]NotebookResource, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("config directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("config directory: %s is not a directory", root)
	}

	state := &DesiredState{}
	if err := loadNotebooks(root, state, opts); err != nil {
		return nil, err
	}

	errs := Validate(&DesiredState{Notebooks: state.Notebooks})
	if len(errs) == 0 {
		return state.Notebooks, nil
	}

	msgs := make([]string, 0, len(errs))
	for _, validationErr := range errs {
		msgs = append(msgs, validationErr.Error())
	}
	return nil, fmt.Errorf("validate notebooks: %s", strings.Join(msgs, "; "))
}
