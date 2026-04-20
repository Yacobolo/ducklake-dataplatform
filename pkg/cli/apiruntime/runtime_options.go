package apiruntime

import cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"

// CommandSpec is the canonical generated CLI execution model.
type CommandSpec = cobraruntime.CommandSpec

// RuntimeOptions customizes generated command construction for a specific CLI.
type RuntimeOptions = cobraruntime.RuntimeOptions

// CommandGroup configures a top-level Cobra group for generated commands.
type CommandGroup = cobraruntime.CommandGroup
