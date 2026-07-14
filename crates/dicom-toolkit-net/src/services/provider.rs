//! SCP provider traits for DIMSE services.
//!
//! Implement these traits to plug your own storage, query, and retrieval
//! logic into a [`DicomServer`](crate::server::DicomServer).  The server
//! handles all DICOM protocol mechanics; your provider only sees clean Rust
//! types.
//!
//! # Design
//!
//! * **One trait per DIMSE service** — `StoreServiceProvider`,
//!   `FindServiceProvider`, `GetServiceProvider`, `MoveServiceProvider`.
//! * All methods are `async` and take `&self` so providers can hold
//!   shared state (database pools, in-memory stores, …).
//! * All traits require `Send + Sync + 'static` so they can be shared
//!   across tokio tasks.
//! * The separate [`DestinationLookup`] trait maps AE titles to network
//!   addresses for C-MOVE sub-associations.

use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;

use dicom_toolkit_core::error::{DcmError, DcmResult};
use dicom_toolkit_core::uid::Uid;
use dicom_toolkit_data::DataSet;
use futures_core::Stream;
use futures_util::stream;

use crate::dataset_source::DatasetSource;

// ── Status codes ──────────────────────────────────────────────────────────────

/// DIMSE status: success.
pub const STATUS_SUCCESS: u16 = 0x0000;
/// DIMSE status: pending (C-FIND / C-GET / C-MOVE).
pub const STATUS_PENDING: u16 = 0xFF00;
/// DIMSE status: out-of-resources failure.
pub const STATUS_OUT_OF_RESOURCES: u16 = 0xA700;
/// Query/Retrieve status: unable to perform C-STORE sub-operations.
pub const STATUS_UNABLE_TO_PERFORM_SUBOPERATIONS: u16 = 0xA702;
/// DIMSE status: dataset does not match SOP class.
pub const STATUS_DATASET_MISMATCH: u16 = 0xA900;
/// DIMSE status: processing failure (generic).
pub const STATUS_PROCESSING_FAILURE: u16 = 0x0110;
/// Query/Retrieve status: unable to process the request.
pub const STATUS_UNABLE_TO_PROCESS: u16 = 0xC000;
/// DIMSE status: unrecognised operation.
pub const STATUS_UNRECOGNISED_OPERATION: u16 = 0x0211;
/// DIMSE status: refused; move destination unknown.
pub const STATUS_MOVE_DESTINATION_UNKNOWN: u16 = 0xA801;
/// DIMSE status: sub-operations completed with one or more failures/warnings.
pub const STATUS_WARNING: u16 = 0xB000;

/// A boxed asynchronous stream of C-FIND response identifiers.
pub type FindResponseStream = Pin<Box<dyn Stream<Item = DcmResult<DataSet>> + Send + 'static>>;

/// A boxed asynchronous stream of C-GET or C-MOVE instances.
pub type RetrieveItemStream = Pin<Box<dyn Stream<Item = DcmResult<RetrieveItem>> + Send + 'static>>;

/// Convert an in-memory list into a C-FIND response stream.
///
/// This is convenient for small result sets. Providers handling large queries
/// should return a genuinely lazy stream instead.
pub fn find_responses(responses: Vec<DataSet>) -> FindResponseStream {
    Box::pin(stream::iter(responses.into_iter().map(Ok)))
}

// ── C-STORE provider ──────────────────────────────────────────────────────────

/// Contextual information delivered to a [`StoreServiceProvider`].
#[derive(Debug, Clone)]
pub struct StoreEvent {
    /// AE title of the calling SCU.
    pub calling_ae: String,
    /// SOP Class UID of the instance being stored.
    pub sop_class_uid: String,
    /// SOP Instance UID of the instance being stored.
    pub sop_instance_uid: String,
    /// The decoded DICOM dataset.
    pub dataset: DataSet,
}

/// Result returned by a [`StoreServiceProvider`] callback.
#[derive(Debug, Clone)]
pub struct StoreResult {
    /// DIMSE status code to return to the SCU.
    ///
    /// Use [`STATUS_SUCCESS`] (0x0000) on success or one of the
    /// `STATUS_*` constants (or a custom code) on failure.
    pub status: u16,
}

impl StoreResult {
    /// Convenience constructor for a successful store.
    pub fn success() -> Self {
        Self {
            status: STATUS_SUCCESS,
        }
    }

    /// Convenience constructor for a processing-failure response.
    pub fn failure(status: u16) -> Self {
        Self { status }
    }
}

/// Trait implemented by SCP back-ends that handle C-STORE requests.
///
/// # Example
///
/// ```rust,no_run
/// use dicom_toolkit_net::services::provider::{StoreEvent, StoreResult, StoreServiceProvider};
///
/// struct MemoryStore {
///     instances: std::sync::Mutex<Vec<dicom_toolkit_data::DataSet>>,
/// }
///
/// impl StoreServiceProvider for MemoryStore {
///     async fn on_store(&self, event: StoreEvent) -> StoreResult {
///         self.instances.lock().unwrap().push(event.dataset);
///         StoreResult::success()
///     }
/// }
/// ```
pub trait StoreServiceProvider: Send + Sync + 'static {
    /// Called when the server receives a C-STORE-RQ.
    ///
    /// The returned status is forwarded to the SCU in the C-STORE-RSP.
    fn on_store(&self, event: StoreEvent) -> impl std::future::Future<Output = StoreResult> + Send;
}

// ── C-FIND provider ───────────────────────────────────────────────────────────

/// Contextual information delivered to a [`FindServiceProvider`].
#[derive(Debug, Clone)]
pub struct FindEvent {
    /// AE title of the calling SCU.
    pub calling_ae: String,
    /// SOP Class UID (identifies which query model is requested).
    pub sop_class_uid: String,
    /// The query identifier dataset supplied by the SCU.
    pub identifier: DataSet,
}

/// Trait implemented by SCP back-ends that handle C-FIND requests.
///
/// Return a lazy stream of matching result identifiers. An empty stream
/// results in a final C-FIND-RSP with status `0x0000` (success, no matches).
pub trait FindServiceProvider: Send + Sync + 'static {
    /// Called when the server receives a C-FIND-RQ.
    ///
    /// Each item in the returned stream is sent as a pending C-FIND-RSP; a
    /// final success response is appended automatically. Provider and stream
    /// errors are converted to a final processing-failure response.
    fn on_find(
        &self,
        event: FindEvent,
    ) -> impl std::future::Future<Output = DcmResult<FindResponseStream>> + Send;
}

// ── C-GET provider ────────────────────────────────────────────────────────────

/// A single instance to be retrieved during a C-GET or C-MOVE sub-operation.
#[derive(Debug, Clone)]
pub struct RetrieveItem {
    /// SOP Class UID.
    pub sop_class_uid: String,
    /// SOP Instance UID.
    pub sop_instance_uid: String,
    /// Transfer Syntax used by the encoded dataset.
    pub transfer_syntax_uid: String,
    /// Encoded dataset source, excluding Part 10 File Meta Information.
    pub dataset: DatasetSource,
}

/// A storage presentation context required by a retrieval plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RetrievePresentationContext {
    /// Storage SOP Class UID.
    pub sop_class_uid: String,
    /// Transfer Syntax UID of the encoded instances.
    pub transfer_syntax_uid: String,
}

impl RetrievePresentationContext {
    /// Create and validate a storage presentation context.
    pub fn new(
        sop_class_uid: impl Into<String>,
        transfer_syntax_uid: impl Into<String>,
    ) -> DcmResult<Self> {
        let sop_class_uid = sop_class_uid.into();
        let transfer_syntax_uid = transfer_syntax_uid.into();
        Uid::new(sop_class_uid.clone())?;
        Uid::new(transfer_syntax_uid.clone())?;
        Ok(Self {
            sop_class_uid,
            transfer_syntax_uid,
        })
    }
}

/// Metadata and lazy item stream for a C-GET or C-MOVE operation.
///
/// The total is declared before streaming because DIMSE sub-operation counters
/// are unsigned 16-bit values. C-MOVE also needs all storage presentation
/// contexts before it opens the destination association.
pub struct RetrievePlan {
    total_suboperations: u16,
    presentation_contexts: Vec<RetrievePresentationContext>,
    items: RetrieveItemStream,
}

impl fmt::Debug for RetrievePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievePlan")
            .field("total_suboperations", &self.total_suboperations)
            .field("presentation_contexts", &self.presentation_contexts)
            .finish_non_exhaustive()
    }
}

impl RetrievePlan {
    /// Create a retrieval plan from explicit metadata and a lazy item stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid UIDs, more than 128 distinct presentation
    /// contexts, or a non-zero total without any presentation context.
    pub fn new(
        total_suboperations: u16,
        mut presentation_contexts: Vec<RetrievePresentationContext>,
        items: RetrieveItemStream,
    ) -> DcmResult<Self> {
        for context in &presentation_contexts {
            Uid::new(context.sop_class_uid.clone())?;
            Uid::new(context.transfer_syntax_uid.clone())?;
        }

        presentation_contexts.sort();
        presentation_contexts.dedup();

        if presentation_contexts.len() > 128 {
            return Err(DcmError::Other(format!(
                "retrieval plan requires {} presentation contexts; DICOM allows at most 128",
                presentation_contexts.len()
            )));
        }
        if total_suboperations > 0 && presentation_contexts.is_empty() {
            return Err(DcmError::Other(
                "a non-empty retrieval plan requires at least one presentation context".into(),
            ));
        }

        Ok(Self {
            total_suboperations,
            presentation_contexts,
            items,
        })
    }

    /// Convert a finite in-memory list into a retrieval plan.
    ///
    /// Large providers should use [`RetrievePlan::new`] with a lazy stream so
    /// item metadata and dataset handles are not retained for the whole study.
    pub fn from_items(items: Vec<RetrieveItem>) -> DcmResult<Self> {
        let total_suboperations = u16::try_from(items.len()).map_err(|_| {
            DcmError::Other(format!(
                "retrieval contains {} items; DIMSE counters support at most {}",
                items.len(),
                u16::MAX
            ))
        })?;

        let mut unique_contexts = HashSet::new();
        for item in &items {
            Uid::new(item.sop_class_uid.clone())?;
            Uid::new(item.sop_instance_uid.clone())?;
            Uid::new(item.transfer_syntax_uid.clone())?;
            unique_contexts.insert(RetrievePresentationContext {
                sop_class_uid: item.sop_class_uid.clone(),
                transfer_syntax_uid: item.transfer_syntax_uid.clone(),
            });
        }

        Self::new(
            total_suboperations,
            unique_contexts.into_iter().collect(),
            Box::pin(stream::iter(items.into_iter().map(Ok))),
        )
    }

    /// Return the declared number of C-STORE sub-operations.
    pub fn total_suboperations(&self) -> u16 {
        self.total_suboperations
    }

    /// Return the storage presentation contexts required by this plan.
    pub fn presentation_contexts(&self) -> &[RetrievePresentationContext] {
        &self.presentation_contexts
    }

    pub(crate) fn into_parts(self) -> (u16, Vec<RetrievePresentationContext>, RetrieveItemStream) {
        (
            self.total_suboperations,
            self.presentation_contexts,
            self.items,
        )
    }
}

/// Contextual information delivered to a [`GetServiceProvider`].
#[derive(Debug, Clone)]
pub struct GetEvent {
    /// AE title of the calling SCU.
    pub calling_ae: String,
    /// SOP Class UID (identifies which query/retrieve model is requested).
    pub sop_class_uid: String,
    /// The query identifier dataset supplied by the SCU.
    pub identifier: DataSet,
}

/// Trait implemented by SCP back-ends that handle C-GET requests.
///
/// Return a retrieval plan whose instances are sent back to the SCU via
/// C-STORE sub-operations on the **same** association.
pub trait GetServiceProvider: Send + Sync + 'static {
    /// Called when the server receives a C-GET-RQ.
    fn on_get(
        &self,
        event: GetEvent,
    ) -> impl std::future::Future<Output = DcmResult<RetrievePlan>> + Send;
}

// ── C-MOVE provider ───────────────────────────────────────────────────────────

/// Contextual information delivered to a [`MoveServiceProvider`].
#[derive(Debug, Clone)]
pub struct MoveEvent {
    /// AE title of the calling SCU.
    pub calling_ae: String,
    /// AE title of the move destination.
    pub destination: String,
    /// SOP Class UID (identifies which query/retrieve model is requested).
    pub sop_class_uid: String,
    /// The query identifier dataset supplied by the SCU.
    pub identifier: DataSet,
}

/// Trait implemented by SCP back-ends that handle C-MOVE requests.
///
/// Return a retrieval plan whose instances are forwarded to the move
/// destination via a sub-association.
pub trait MoveServiceProvider: Send + Sync + 'static {
    /// Called when the server receives a C-MOVE-RQ.
    fn on_move(
        &self,
        event: MoveEvent,
    ) -> impl std::future::Future<Output = DcmResult<RetrievePlan>> + Send;
}

// ── Destination lookup ────────────────────────────────────────────────────────

/// Maps an AE title to a `"host:port"` address string for C-MOVE
/// sub-associations.
///
/// Implement this trait to manage your AE title registry (config file,
/// database, …).
pub trait DestinationLookup: Send + Sync + 'static {
    /// Return the network address for the given AE title, or `None` if
    /// the destination is unknown (causes the server to reply with
    /// `STATUS_MOVE_DESTINATION_UNKNOWN`).
    fn lookup(&self, ae_title: &str) -> Option<String>;
}

/// A fixed in-memory AE title registry.
///
/// # Example
///
/// ```rust
/// use dicom_toolkit_net::services::provider::StaticDestinationLookup;
///
/// let lookup = StaticDestinationLookup::new(vec![
///     ("STORESCP".to_string(), "127.0.0.1:4242".to_string()),
/// ]);
/// ```
pub struct StaticDestinationLookup {
    entries: Vec<(String, String)>,
}

impl StaticDestinationLookup {
    /// Create a lookup table from a list of `(ae_title, host:port)` pairs.
    pub fn new(entries: Vec<(String, String)>) -> Self {
        Self { entries }
    }
}

impl DestinationLookup for StaticDestinationLookup {
    fn lookup(&self, ae_title: &str) -> Option<String> {
        let upper = ae_title.trim().to_uppercase();
        self.entries
            .iter()
            .find(|(ae, _)| ae.trim().to_uppercase() == upper)
            .map(|(_, addr)| addr.clone())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_destination_lookup_found() {
        let lookup = StaticDestinationLookup::new(vec![
            ("STORESCP".to_string(), "127.0.0.1:4242".to_string()),
            ("ARCHIVE".to_string(), "10.0.0.1:11112".to_string()),
        ]);
        assert_eq!(
            lookup.lookup("STORESCP"),
            Some("127.0.0.1:4242".to_string())
        );
        assert_eq!(lookup.lookup("ARCHIVE"), Some("10.0.0.1:11112".to_string()));
    }

    #[test]
    fn static_destination_lookup_not_found() {
        let lookup = StaticDestinationLookup::new(vec![]);
        assert_eq!(lookup.lookup("UNKNOWN"), None);
    }

    #[test]
    fn static_destination_lookup_case_insensitive() {
        let lookup = StaticDestinationLookup::new(vec![(
            "StOreScp".to_string(),
            "127.0.0.1:4242".to_string(),
        )]);
        assert_eq!(
            lookup.lookup("storescp"),
            Some("127.0.0.1:4242".to_string())
        );
    }

    #[test]
    fn store_result_success() {
        let r = StoreResult::success();
        assert_eq!(r.status, STATUS_SUCCESS);
    }

    #[test]
    fn store_result_failure() {
        let r = StoreResult::failure(STATUS_OUT_OF_RESOURCES);
        assert_eq!(r.status, STATUS_OUT_OF_RESOURCES);
    }

    #[test]
    fn retrieve_plan_deduplicates_presentation_contexts() {
        let items = vec![
            RetrieveItem {
                sop_class_uid: "1.2.840.10008.5.1.4.1.1.2".to_string(),
                sop_instance_uid: "1.2.3.1".to_string(),
                transfer_syntax_uid: "1.2.840.10008.1.2.1".to_string(),
                dataset: Vec::new().into(),
            },
            RetrieveItem {
                sop_class_uid: "1.2.840.10008.5.1.4.1.1.2".to_string(),
                sop_instance_uid: "1.2.3.2".to_string(),
                transfer_syntax_uid: "1.2.840.10008.1.2.1".to_string(),
                dataset: Vec::new().into(),
            },
        ];

        let plan = RetrievePlan::from_items(items).expect("valid retrieval plan");

        assert_eq!(plan.total_suboperations(), 2);
        assert_eq!(plan.presentation_contexts().len(), 1);
    }

    #[test]
    fn retrieve_plan_rejects_invalid_uids() {
        let item = RetrieveItem {
            sop_class_uid: "not-a-uid".to_string(),
            sop_instance_uid: "1.2.3".to_string(),
            transfer_syntax_uid: "1.2.840.10008.1.2.1".to_string(),
            dataset: Vec::new().into(),
        };

        let result = RetrievePlan::from_items(vec![item]);

        assert!(matches!(result, Err(DcmError::InvalidUid { .. })));
    }
}
