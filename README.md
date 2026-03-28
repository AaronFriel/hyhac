hyhac
=====

A HyperDex Haskell Client

What is HyperDex? [HyperDex.org](http://hyperdex.org) describes it so:

> HyperDex is a next generation key-value store with a wide array of features. HyperDex may be a good fit for you if your application needs to:
> 
> * Store rich datatypes such as strings, integers, floats, lists, maps, and sets.
> * Search and retrieve objects efficiently by secondary attributes.
> * Ensure that your data is up to date at all times with strong consistency guarantees.
> * Protect your data against a configurable number of simultaneous failures.
> * Perform atomic transactions over multiple objects, in a fast, scalable NoSQL data store.
> 
> HyperDex's key features -- namely its rich API, strong consistency, and fault tolerance -- provide strong guarantees to applications that are not matched by other NoSQL systems.

Authors
-------

[Aaron Friel](https://github.com/aaronfriel)
[Niklas Hambüchen](https://github.com/nh2)

Building
--------

This repository now builds with modern `ghcup` + `cabal` tooling. After installing
GHCup and sourcing `~/.ghcup/env`, use the checked-in wrapper so Cabal sees the
HyperDex build tree and the local `libgmp.so` shim it may need on hosts without
the `libgmp-dev` linker symlink:

```bash
scripts/cabal.sh build -f tests lib:hyhac test:tests
```

By default the wrapper expects the sibling HyperDex checkout at `../HyperDex`.
If your HyperDex build is somewhere else, set `HYPERDEX_ROOT=/path/to/HyperDex`
before invoking the script.

Testing
-------

The test suite depends on a local HyperDex coordinator and daemon. Run the
deterministic wrapper below instead of starting daemons by hand:

```bash
scripts/test-with-hyperdex.sh
```

That wrapper:

- starts a private one-node HyperDex cluster in a temporary directory;
- exports the in-tree HyperDex runtime variables needed by an uninstalled build;
- waits for the daemon to show up as `AVAILABLE` before it runs Haskell tests;
- runs `cabal test -f tests test:tests` with `--test-seed=1` for repeatable property-test input;
- shuts the cluster down automatically and removes temporary state on success.

You can override the fixed property-test seed with `HYPERDEX_TEST_SEED=<n>`.

For a fast deterministic smoke test that does not require a running HyperDex
daemon, you can run just the pure `CBString` property group:

```bash
scripts/cabal.sh test -f tests test:tests \
  --test-show-details=direct \
  --test-option=--plain \
  --test-option=--test-seed=1 \
  --test-option=--select-tests='hyhac-tests/CBString API Tests*'
```
