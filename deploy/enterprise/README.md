# Enterprise Deployment Foundation

This directory contains the first enterprise deployment foundation for QuackStack.

Goals of this foundation:

- keep a single product surface across Compose and enterprise deployments
- package enterprise installs with Helm
- keep provider-specific deployment details in values files and templates
- preserve a provider-neutral control-plane contract in application code

Current state:

- the Helm chart in `helm/quackstack` provides a baseline control-plane deployment shape
- provider-specific overrides live in `values-azure.yaml`
- the application now understands deployment profiles, control-plane DB driver selection, and platform provider selection
- enterprise runtime support for a full Postgres-backed control plane and managed lifecycle controller is still a follow-up implementation

## Install

Example:

```bash
helm upgrade --install quackstack deploy/enterprise/helm/quackstack \
  --namespace quackstack \
  --create-namespace \
  -f deploy/enterprise/helm/quackstack/values.yaml \
  -f deploy/enterprise/helm/quackstack/values-azure.yaml
```

Provide real secrets before install:

- `CONTROL_DB_DSN`
- `ENCRYPTION_KEY`
- `JWT_SECRET` when used
- cloud-specific credentials when workload identity is not yet in place

## Profiles

- simple mode remains the Compose-based deployment path
- enterprise mode uses:
  - `DEPLOYMENT_PROFILE=enterprise`
  - `CONTROL_DB_DRIVER=postgres`
  - `PLATFORM_PROVIDER=azure`

## Design Notes

- QuackStack defines the deployment contract
- providers implement that contract
- Helm packages the deployment, but is not the abstraction boundary
- user-facing APIs remain about endpoints, routing, and health rather than AKS-specific concepts

