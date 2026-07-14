//! Association configuration.
//!
//! Mirrors DCMTK's `DcmAssociationConfiguration` / `T_ASC_Parameters`.

use crate::pdu::{AsynchronousOperationsWindow, ScpScuRoleSelection};

// ── AssociationConfig ─────────────────────────────────────────────────────────

/// Configuration for both SCU (outbound) and SCP (inbound) associations.
#[derive(Debug, Clone)]
pub struct AssociationConfig {
    /// AE title advertised as the local application entity.
    pub local_ae_title: String,

    /// Maximum PDU length we are willing to receive (bytes).
    ///
    /// This is advertised in the DICOM Maximum Length Received sub-item. A
    /// value of `0` selects [`maximum_incoming_pdu_length`](Self::maximum_incoming_pdu_length)
    /// instead of advertising an unbounded value. Defaults to 65 536.
    pub max_pdu_length: u32,

    /// Absolute resource limit for any received PDU variable field.
    ///
    /// This protects association negotiation from attacker-controlled length
    /// fields and caps the effective value advertised by `max_pdu_length`.
    /// Defaults to 16 MiB.
    pub maximum_incoming_pdu_length: u32,

    /// Local fragmentation limit for outgoing P-DATA-TF PDU variable fields.
    ///
    /// The effective limit is the smaller of this value and the peer's
    /// Maximum Length Received value. It therefore also bounds the reusable
    /// streaming buffer when a peer advertises an unlimited value. Defaults
    /// to 65 536 bytes.
    pub maximum_outgoing_pdu_length: u32,

    /// Seconds to wait for a response during association negotiation and
    /// DIMSE operations before returning a timeout error.
    pub dimse_timeout_secs: u64,

    /// If `true`, the SCP accepts any transfer syntax offered by the SCU
    /// without checking against a preferred list.
    pub accept_all_transfer_syntaxes: bool,

    /// Transfer syntax UIDs this SCP is willing to accept explicitly.
    ///
    /// When empty and `accept_all_transfer_syntaxes` is `false`, negotiation
    /// falls back to Explicit VR Little Endian and then Implicit VR Little Endian.
    pub accepted_transfer_syntaxes: Vec<String>,

    /// Transfer syntax UIDs this SCP prefers, in descending priority order.
    ///
    /// When empty, accepted transfer syntaxes are chosen in the order they were
    /// offered by the peer.
    pub preferred_transfer_syntaxes: Vec<String>,

    /// Implementation Class UID advertised in the User Information sub-item.
    pub implementation_class_uid: String,

    /// Implementation Version Name advertised in the User Information sub-item.
    pub implementation_version_name: String,

    /// Abstract Syntax UIDs this SCP is willing to accept.
    ///
    /// An empty list means **accept all** (useful for testing / generic SCPs).
    pub accepted_abstract_syntaxes: Vec<String>,

    /// Asynchronous operations window proposed on outbound associations.
    ///
    /// `None` uses the mandatory synchronous default of one operation in each
    /// direction.
    pub requested_asynchronous_operations_window: Option<AsynchronousOperationsWindow>,

    /// Maximum asynchronous window accepted for inbound associations.
    ///
    /// These values describe the local acceptor. The negotiated response is
    /// also constrained by the requestor's proposal.
    pub maximum_asynchronous_operations_window: AsynchronousOperationsWindow,

    /// SCP/SCU role selections proposed on outbound associations.
    pub requested_role_selections: Vec<ScpScuRoleSelection>,

    /// Whether an inbound association requestor may negotiate the SCU role.
    pub accept_requestor_scu_role: bool,

    /// Whether an inbound association requestor may negotiate the SCP role.
    ///
    /// This is required for C-GET because the association acceptor sends
    /// C-STORE sub-operations as an SCU on the same association.
    pub accept_requestor_scp_role: bool,
}

impl Default for AssociationConfig {
    fn default() -> Self {
        Self {
            local_ae_title: "DCMTKRS".to_string(),
            max_pdu_length: 65_536,
            maximum_incoming_pdu_length: 16 * 1024 * 1024,
            maximum_outgoing_pdu_length: 65_536,
            dimse_timeout_secs: 30,
            accept_all_transfer_syntaxes: false,
            accepted_transfer_syntaxes: Vec::new(),
            preferred_transfer_syntaxes: Vec::new(),
            implementation_class_uid: "1.3.6.1.4.1.30071.8.1".to_string(),
            implementation_version_name: "DCMTK_RS_010".to_string(),
            accepted_abstract_syntaxes: Vec::new(),
            requested_asynchronous_operations_window: None,
            maximum_asynchronous_operations_window: AsynchronousOperationsWindow::default(),
            requested_role_selections: Vec::new(),
            accept_requestor_scu_role: true,
            accept_requestor_scp_role: false,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_sensible() {
        let cfg = AssociationConfig::default();
        assert_eq!(cfg.max_pdu_length, 65_536);
        assert_eq!(cfg.maximum_incoming_pdu_length, 16 * 1024 * 1024);
        assert_eq!(cfg.maximum_outgoing_pdu_length, 65_536);
        assert_eq!(cfg.dimse_timeout_secs, 30);
        assert!(!cfg.accept_all_transfer_syntaxes);
        assert!(cfg.accepted_transfer_syntaxes.is_empty());
        assert!(cfg.preferred_transfer_syntaxes.is_empty());
        assert!(!cfg.implementation_class_uid.is_empty());
        assert_eq!(
            cfg.maximum_asynchronous_operations_window,
            AsynchronousOperationsWindow::default()
        );
        assert!(cfg.requested_role_selections.is_empty());
        assert!(cfg.accept_requestor_scu_role);
        assert!(!cfg.accept_requestor_scp_role);
    }
}
