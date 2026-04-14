# Examples

Standalone showcase examples have been retired.

The canonical end-to-end declarative example now lives in [`/quackstack-config`](../quackstack-config), which also powers `task dev:seeded`.

Recommended local flow:

```bash
task dev:seeded
./bin/quack validate --config-dir quackstack-config
./bin/quack plan --config-dir quackstack-config
```

Automated verification:

- `task examples:test`: runs the remaining example-oriented integration coverage. It skips cleanly when there are no standalone example config directories.
- `task examples:validate`: validates any standalone configs under `examples/*/config` if present.
