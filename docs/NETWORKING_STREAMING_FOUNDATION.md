# Networking streaming foundation

This document describes the production constraints and public contracts added to
`dicom-toolkit-net` for large Query/Retrieve workloads. It is intentionally
explicit about what is implemented and what remains unsafe to claim.

## Scope

The implemented path targets a DICOM gateway that retrieves encoded instances
from external storage and serves them to desktop viewers through C-GET or C-MOVE.
The important invariant is that an encoded instance does not need to be loaded
into memory before it is sent.

The networking crate does not transcode datasets. A provider must declare the
actual Transfer Syntax UID of every `RetrieveItem`, and the corresponding
presentation context must be accepted by the peer.

## Outgoing dataset sources

`DatasetSource` supports two source types:

- `Bytes` for small immutable payloads and tests;
- `FileDataset` for an exact file region or from an offset through EOF.

A file source contains the encoded DIMSE dataset only. A DICOM Part 10 preamble
and File Meta Information must not be sent by C-STORE, so callers using Part 10
files must determine the dataset offset and pass that region.

The association opens the file lazily, seeks to the declared offset, validates
the region against the current file length, and reuses one PDU-sized buffer.
Payload bytes are written directly after the P-DATA/PDV header; there is no
second payload-sized PDU allocation.

```rust
use dicom_toolkit_net::DatasetSource;

let source = DatasetSource::file_region(
    "/var/cache/instances/1.2.3.dcm",
    512,
    900 * 1024 * 1024,
);
```

## Lazy provider contract

C-FIND providers return a `FindResponseStream`. C-GET and C-MOVE providers
return a `RetrievePlan` containing:

- the exact number of C-STORE sub-operations, limited by the DIMSE `u16`
  counters;
- all required SOP Class/Transfer Syntax presentation-context pairs;
- a lazy `RetrieveItemStream`.

C-MOVE needs the context list before consuming the stream because it must
negotiate the destination association first. The stream must yield exactly the
declared number of items. An early end, an extra item, an invalid UID, or a
stream error becomes a final failure response instead of silently corrupting
the sub-operation counters.

`DicomServerBuilder::move_destination_config` configures outbound C-MOVE
associations separately from inbound SCP associations. This separation matters
because role selections and asynchronous-window proposals are directional. If
it is omitted, the server copies the inbound limits/timeouts but clears outbound
role selections.

`RetrievePlan::from_items` remains available for small finite lists. Large
studies should use `RetrievePlan::new` with a genuinely lazy stream.

## API migration

This foundation intentionally changes public networking contracts and therefore
requires a semver-compatible breaking release before publication:

- `StoreRequest::dataset_bytes: Vec<u8>` is replaced by
  `StoreRequest::dataset: DatasetSource`;
- `RetrieveItem::dataset: Vec<u8>` is replaced by `DatasetSource`, and
  `transfer_syntax_uid` is now mandatory;
- C-FIND providers return `DcmResult<FindResponseStream>` instead of a
  `Vec<DataSet>`;
- C-GET and C-MOVE providers return `DcmResult<RetrievePlan>` instead of a
  `Vec<RetrieveItem>`;
- `handle_move_rq` receives a dedicated outbound `AssociationConfig`.

`Vec<u8>` and `Bytes` convert directly into `DatasetSource`, while
`find_responses` and `RetrievePlan::from_items` are migration conveniences for
small in-memory workloads. These breaking networking contracts are first
available in the `0.6.0-rc.1` workspace release candidate.

## Transfer syntax and roles

Presentation-context lookup for retrieval matches both SOP Class UID and
Transfer Syntax UID. C-MOVE creates one destination context per unique pair.
No implicit fallback to Explicit VR Little Endian occurs.

SCP/SCU Role Selection (`0x54`) is encoded, decoded, validated, and enforced.
C-GET requestors must propose the SCP role for each Storage SOP Class they want
to receive. Registering a C-GET provider on `DicomServer` enables acceptance of
the requestor-SCP role; low-level association users configure this explicitly.

The Asynchronous Operations Window (`0x53`) is also encoded, decoded, and
validated. The ready-made `DicomServer` currently accepts only the synchronous
window `(1, 1)`. It fails at build time for a larger configured window rather
than advertising concurrency that its dispatcher does not implement.

## Resource bounds

`AssociationConfig` separates three limits:

- `max_pdu_length`: preferred Maximum Length Received value;
- `maximum_incoming_pdu_length`: hard pre-allocation ceiling for received PDU
  variable fields;
- `maximum_outgoing_pdu_length`: local fragmentation and streaming-buffer
  ceiling, including when a peer advertises zero (unlimited).

Incoming PDU lengths are checked before allocating the body. Outgoing DIMSE
streams use even-sized fragments and reject odd total encoded lengths.

`DicomServer::max_associations` bounds concurrently active associations. Each
association is handled by one Tokio task, and file I/O uses Tokio's asynchronous
file API. Memory for a file-backed outgoing instance is therefore bounded by
the effective PDU size per active association, excluding transport/runtime
overhead and provider-owned state.

Server shutdown stops accepting new sockets, drains tracked association tasks
for the builder's configurable `graceful_shutdown_timeout`, and aborts any tasks
that remain after the deadline. Association tasks are not detached.

## Correct concurrency model for a gateway

DICOM PS3.8 prohibits interleaving fragments from different messages. Hundreds
of cloud downloads therefore must not be translated into hundreds of
interleaved DIMSE datasets on one association.

The gateway application should use two independently bounded concurrency
layers:

1. a download/cache scheduler that fetches objects concurrently and atomically
   publishes complete cache files;
2. the DICOM server association limit, which controls how many viewer
   connections can stream concurrently.

A retrieval provider should yield an item only after its cache file is ready.
Backpressure then propagates naturally from the viewer through the item stream
without retaining all datasets in memory.

## Known gaps before claiming full high-concurrency DIMSE support

The following are deliberately not represented as complete:

- C-CANCEL is not yet processed while a provider or a large outgoing dataset is
  active;
- the ready-made server does not dispatch multiple outstanding operations on a
  single association;
- incoming C-STORE still materializes and decodes the complete dataset before
  calling `StoreServiceProvider`;
- interoperability tests currently use Rust loopback peers; validation against
  DCMTK and the target viewers is still required;
- a file-backed source requires the caller to provide the Part 10 dataset
  offset; a bounded metadata-only Part 10 inspector is not yet exposed.

The next networking milestone should introduce an association actor with
independent reader and writer halves, a Message ID keyed operation registry,
bounded writer queues, and cancellation tokens. Only after that dispatcher is
tested should `DicomServer` negotiate asynchronous windows larger than one.
