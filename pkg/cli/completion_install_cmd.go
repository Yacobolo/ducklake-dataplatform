package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

const completionMarkerStart = "# >>> quack completion >>>"
const completionMarkerEnd = "# <<< quack completion <<<"

func newCompletionInstallCmd() *cobra.Command {
	var shell string

	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install shell completion into your shell profile",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolvedShell, err := resolveCompletionShell(shell)
			if err != nil {
				return err
			}

			target, err := completionTargetFile(resolvedShell)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("create completion directory: %w", err)
			}

			existing, err := os.ReadFile(target)
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("read %s: %w", target, err)
			}
			if strings.Contains(string(existing), completionMarkerStart) {
				return printCompletionStatus(cmd, resolvedShell, target, "installed")
			}

			block := "\n" + completionMarkerStart + "\n" + completionSnippet(resolvedShell) + "\n" + completionMarkerEnd + "\n"
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return fmt.Errorf("open %s: %w", target, err)
			}
			defer f.Close()
			if _, err := f.WriteString(block); err != nil {
				return fmt.Errorf("write %s: %w", target, err)
			}

			return printCompletionStatus(cmd, resolvedShell, target, "installed")
		},
	}

	cmd.Flags().StringVar(&shell, "shell", "", "Shell type: bash, zsh, fish, powershell")
	return cmd
}

func newCompletionStatusCmd() *cobra.Command {
	var shell string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show shell completion installation status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolvedShell, err := resolveCompletionShell(shell)
			if err != nil {
				return err
			}
			target, err := completionTargetFile(resolvedShell)
			if err != nil {
				return err
			}
			existing, err := os.ReadFile(target)
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("read %s: %w", target, err)
			}
			status := "not installed"
			if strings.Contains(string(existing), completionMarkerStart) {
				status = "installed"
			}
			return printCompletionStatus(cmd, resolvedShell, target, status)
		},
	}

	cmd.Flags().StringVar(&shell, "shell", "", "Shell type: bash, zsh, fish, powershell")
	return cmd
}

func newCompletionUninstallCmd() *cobra.Command {
	var shell string

	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove shell completion from your shell profile",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resolvedShell, err := resolveCompletionShell(shell)
			if err != nil {
				return err
			}
			target, err := completionTargetFile(resolvedShell)
			if err != nil {
				return err
			}
			existing, err := os.ReadFile(target)
			if err != nil {
				if os.IsNotExist(err) {
					return printCompletionStatus(cmd, resolvedShell, target, "not installed")
				}
				return fmt.Errorf("read %s: %w", target, err)
			}

			updated := removeCompletionBlock(string(existing))
			if updated == string(existing) {
				return printCompletionStatus(cmd, resolvedShell, target, "not installed")
			}
			if err := os.WriteFile(target, []byte(updated), 0o600); err != nil {
				return fmt.Errorf("write %s: %w", target, err)
			}
			return printCompletionStatus(cmd, resolvedShell, target, "not installed")
		},
	}

	cmd.Flags().StringVar(&shell, "shell", "", "Shell type: bash, zsh, fish, powershell")
	return cmd
}

func resolveCompletionShell(shell string) (string, error) {
	resolved := strings.TrimSpace(strings.ToLower(shell))
	if resolved == "" {
		resolved = strings.TrimPrefix(filepath.Base(os.Getenv("SHELL")), "/")
	}
	switch resolved {
	case "bash", "zsh", "fish", "powershell", "pwsh":
		if resolved == "pwsh" {
			return "powershell", nil
		}
		return resolved, nil
	default:
		return "", fmt.Errorf("unsupported shell %q", resolved)
	}
}

func completionTargetFile(shell string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	switch shell {
	case "bash":
		return filepath.Join(home, ".bashrc"), nil
	case "zsh":
		return filepath.Join(home, ".zshrc"), nil
	case "fish":
		return filepath.Join(home, ".config", "fish", "config.fish"), nil
	case "powershell":
		return filepath.Join(home, ".config", "powershell", "Microsoft.PowerShell_profile.ps1"), nil
	default:
		return "", fmt.Errorf("unsupported shell %q", shell)
	}
}

func completionSnippet(shell string) string {
	switch shell {
	case "bash":
		return `source <(quack completion bash)`
	case "zsh":
		return "autoload -U compinit && compinit\nsource <(quack completion zsh)"
	case "fish":
		return `quack completion fish | source`
	case "powershell":
		return `quack completion powershell | Out-String | Invoke-Expression`
	default:
		return ""
	}
}

func removeCompletionBlock(content string) string {
	start := strings.Index(content, completionMarkerStart)
	if start == -1 {
		return content
	}
	end := strings.Index(content[start:], completionMarkerEnd)
	if end == -1 {
		return content
	}
	end += start + len(completionMarkerEnd)
	if end < len(content) && content[end] == '\n' {
		end++
	}
	return strings.TrimRight(content[:start]+content[end:], "\n") + "\n"
}

func printCompletionStatus(cmd *cobra.Command, shell, filePath, status string) error {
	if getOutputFormat(cmd) == "json" {
		return apiruntime.PrintJSON(os.Stdout, map[string]string{
			"shell":  shell,
			"file":   filePath,
			"status": status,
		})
	}
	_, _ = fmt.Fprintf(os.Stdout, "Shell:   %s\n", shell)
	_, _ = fmt.Fprintf(os.Stdout, "File:    %s\n", filePath)
	_, _ = fmt.Fprintf(os.Stdout, "Status:  %s\n", status)
	return nil
}
