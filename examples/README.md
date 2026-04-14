# Examples

Standalone showcase examples have been retired.

The canonical end-to-end declarative example now lives in [`/duck-config`](../duck-config), which also powers `task dev:seeded`.

Recommended local flow:

```bash
task dev:seeded
./bin/duck validate --config-dir duck-config
./bin/duck plan --config-dir duck-config
```

Automated verification:

- `task examples:test`: runs the remaining example-oriented integration coverage. It skips cleanly when there are no standalone example config directories.
- `task examples:validate`: validates any standalone configs under `examples/*/config` if present.
