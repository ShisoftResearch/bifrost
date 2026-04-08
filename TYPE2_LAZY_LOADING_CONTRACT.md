# Type-2 Lazy Loading Contract

This document defines the contract when Bifrost Type-1 does not own any Type-2 catalog or inventory metadata.

## Scope

In this model:

- Type-1 is only the raft group for the Type-1 plane.
- Type-1 does not store which Type-2 planes should exist.
- Type-2 planes are materialized only when a caller explicitly addresses a `plane_id`.
- Persisted Type-2 state is recovered locally from each host's per-plane storage directory.

## What Bifrost Guarantees

- `RaftService::plane(plane_id)` and `RaftService::ensure_plane(plane_id)` can lazily materialize a persisted or explicitly created Type-2 runtime.
- A missing Type-2 plane does not fall back to Type-1 handlers.
- `RaftService::loaded_type2_planes()` reports only the Type-2 runtimes currently materialized in memory on the local host.

## What Bifrost Does Not Guarantee

- Bifrost cannot enumerate the authoritative set of Type-2 planes for a cluster.
- Bifrost cannot derive the logical mapping from database, tenant, shard, or partition to `plane_id`.
- Bifrost cannot infer the desired Type-2 member set unless that information is supplied during bootstrap or recovered from that plane's own persisted state.

## Required Upper-Layer Inputs

If Type-1 owns no Type-2 metadata, the upper layer must provide all of the following:

1. A stable `plane_id` for every logical Type-2 plane.
2. The logical mapping from upper-layer objects to `plane_id`.
3. The initial Type-2 member addresses when a plane is first created.
4. The decision of when a plane should be opened, created, retried, or forgotten.
5. Any lifecycle versioning or generation rules if a logical plane can be recreated.

## Consequences For Nebuchadnezzar And Morpheus

Nebuchadnezzar or Morpheus must act as the control plane for Type-2 discovery. In practice, that means they must:

1. Resolve the correct `plane_id` before calling Bifrost.
2. Provide the intended Type-2 member set during first-plane bootstrap.
3. Re-open Type-2 planes by `plane_id` during restart or recovery.
4. Treat `loaded_type2_planes()` as a local observability API, not as cluster inventory.

## Design Rule

If authoritative Type-2 discovery is needed inside Bifrost, that is a different design: it requires a Type-1 plane catalog. Without that catalog, lazy loading is valid, but discovery must remain outside Type-1.