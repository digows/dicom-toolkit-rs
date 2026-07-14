//! DICOM association state machine.
//!
//! Implements the SCU (request) and SCP (accept) sides of an association
//! as defined in PS3.8 §7 and §9.

use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use dicom_toolkit_core::error::{DcmError, DcmResult};
use dicom_toolkit_data::DataSet;

use crate::config::AssociationConfig;
use crate::dataset_source::{DatasetSource, FileDataset};
use crate::dimse;
use crate::pdu::{
    self, AAbort, AssociateAc, AssociateRq, AsynchronousOperationsWindow, Pdu, Pdv,
    PresentationContextAcItem, PresentationContextRqItem, ScpScuRoleSelection,
};
use crate::presentation::{PcResult, PresentationContextAc, PresentationContextRq};

// ── Well-known transfer syntax UIDs ──────────────────────────────────────────

const TS_IMPLICIT_VR_LE: &str = "1.2.840.10008.1.2";
const TS_EXPLICIT_VR_LE: &str = "1.2.840.10008.1.2.1";
const APP_CONTEXT_UID: &str = "1.2.840.10008.3.1.1.1";

// ── AssociationState ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssociationState {
    #[allow(dead_code)]
    Idle,
    #[allow(dead_code)]
    RequestSent,
    Established,
    ReleaseRequested,
    Closed,
}

// ── Association ───────────────────────────────────────────────────────────────

/// An established DICOM association over a TCP connection.
pub struct Association {
    stream: TcpStream,
    state: AssociationState,
    /// AE title of the remote called entity.
    pub called_ae: String,
    /// AE title of the initiating (calling) entity.
    pub calling_ae: String,
    /// Negotiated presentation contexts.
    pub presentation_contexts: Vec<PresentationContextAc>,
    /// Maximum PDU length the peer advertised for inbound PDUs we send.
    pub max_pdu_length: u32,
    /// Effective local fragmentation limit for outbound P-DATA-TF PDUs.
    maximum_outgoing_pdu_length: u32,
    /// Effective limit for received P-DATA-TF PDU variable fields.
    maximum_incoming_pdu_length: u32,
    /// Remote socket address.
    pub peer_addr: SocketAddr,
    /// Negotiated asynchronous operation limits from the local AE's perspective.
    pub asynchronous_operations_window: AsynchronousOperationsWindow,
    /// Negotiated role selections, expressed as association-requestor roles.
    pub role_selections: Vec<ScpScuRoleSelection>,
    /// Whether the local AE initiated this association.
    is_requestor: bool,
    /// Buffered PDVs from the most recently read P-DATA-TF PDU.
    ///
    /// DICOM PS3.8 §9.3.4 allows multiple PDVs per P-DATA-TF.
    /// The original DCMTK C++ buffers all PDVs in the DUL layer
    /// (`PRIVATE_ASSOCIATIONKEY::pdvIndex/pdvCount`). This queue
    /// replicates that behaviour so that no PDVs are silently lost.
    pdv_queue: std::collections::VecDeque<pdu::Pdv>,
}

impl Association {
    // ── SCU side ─────────────────────────────────────────────────────────────

    /// Connect to `addr` and perform the A-ASSOCIATE-RQ / AC handshake.
    ///
    /// `addr` may be a `"host:port"` string accepted by `TcpStream::connect`.
    pub async fn request(
        addr: &str,
        called_ae: &str,
        calling_ae: &str,
        contexts: &[PresentationContextRq],
        config: &AssociationConfig,
    ) -> DcmResult<Self> {
        validate_pdu_limits(config)?;
        validate_requested_role_selections(contexts, &config.requested_role_selections)?;
        let stream = TcpStream::connect(addr).await?;
        let peer_addr = stream.peer_addr()?;
        let mut stream = stream;

        let rq = AssociateRq {
            called_ae_title: called_ae.to_string(),
            calling_ae_title: calling_ae.to_string(),
            application_context: APP_CONTEXT_UID.to_string(),
            presentation_contexts: contexts
                .iter()
                .map(|pc| PresentationContextRqItem {
                    id: pc.id,
                    abstract_syntax: pc.abstract_syntax.clone(),
                    transfer_syntaxes: pc.transfer_syntaxes.clone(),
                })
                .collect(),
            max_pdu_length: effective_incoming_pdu_length(config),
            implementation_class_uid: config.implementation_class_uid.clone(),
            implementation_version_name: config.implementation_version_name.clone(),
            asynchronous_operations_window: config.requested_asynchronous_operations_window,
            role_selections: config.requested_role_selections.clone(),
        };

        stream.write_all(&pdu::encode_associate_rq(&rq)).await?;

        let response = timeout(
            Duration::from_secs(config.dimse_timeout_secs),
            pdu::read_pdu_with_limit(&mut stream, config.maximum_incoming_pdu_length),
        )
        .await
        .map_err(|_| DcmError::Timeout {
            seconds: config.dimse_timeout_secs,
        })??;

        match response {
            Pdu::AssociateAc(ac) => {
                validate_accepted_role_selections(
                    &config.requested_role_selections,
                    &ac.role_selections,
                )?;
                let asynchronous_operations_window = requestor_window_from_acceptance(
                    config.requested_asynchronous_operations_window,
                    ac.asynchronous_operations_window,
                )?;
                // Map raw AC items back to our typed PresentationContextAc,
                // joining with the original abstract syntaxes from the RQ.
                let pcs = ac
                    .presentation_contexts
                    .iter()
                    .map(|ac_item| {
                        let abs = contexts
                            .iter()
                            .find(|rq| rq.id == ac_item.id)
                            .map(|rq| rq.abstract_syntax.clone())
                            .unwrap_or_default();
                        PresentationContextAc {
                            id: ac_item.id,
                            result: PcResult::from_u8(ac_item.result),
                            transfer_syntax: ac_item.transfer_syntax.clone(),
                            abstract_syntax: abs,
                        }
                    })
                    .collect();

                Ok(Association {
                    stream,
                    state: AssociationState::Established,
                    called_ae: called_ae.to_string(),
                    calling_ae: calling_ae.to_string(),
                    presentation_contexts: pcs,
                    max_pdu_length: ac.max_pdu_length,
                    maximum_outgoing_pdu_length: effective_outgoing_pdu_length(
                        ac.max_pdu_length,
                        config,
                    ),
                    maximum_incoming_pdu_length: effective_incoming_pdu_length(config),
                    peer_addr,
                    asynchronous_operations_window,
                    role_selections: ac.role_selections,
                    is_requestor: true,
                    pdv_queue: std::collections::VecDeque::new(),
                })
            }
            Pdu::AssociateRj(rj) => Err(DcmError::AssociationRejected {
                reason: format!(
                    "result={}, source={}, reason={}",
                    rj.result, rj.source, rj.reason
                ),
            }),
            _ => Err(DcmError::Other(
                "unexpected PDU type during association negotiation".into(),
            )),
        }
    }

    // ── SCP side ─────────────────────────────────────────────────────────────

    /// Accept an incoming TCP connection and complete the A-ASSOCIATE-AC
    /// handshake according to `config`.
    pub async fn accept(stream: TcpStream, config: &AssociationConfig) -> DcmResult<Self> {
        validate_pdu_limits(config)?;
        let peer_addr = stream.peer_addr()?;
        let mut stream = stream;

        let incoming = timeout(
            Duration::from_secs(config.dimse_timeout_secs),
            pdu::read_pdu_with_limit(&mut stream, config.maximum_incoming_pdu_length),
        )
        .await
        .map_err(|_| DcmError::Timeout {
            seconds: config.dimse_timeout_secs,
        })??;

        let rq = match incoming {
            Pdu::AssociateRq(rq) => rq,
            _ => {
                return Err(DcmError::Other(
                    "expected A-ASSOCIATE-RQ as first PDU".into(),
                ))
            }
        };
        validate_incoming_role_selections(&rq.presentation_contexts, &rq.role_selections)?;

        // Negotiate each proposed presentation context
        let mut accepted_pcs: Vec<PresentationContextAc> = Vec::new();
        let mut ac_items: Vec<PresentationContextAcItem> = Vec::new();

        for pc in &rq.presentation_contexts {
            let (result_byte, ts) = negotiate_pc(pc, config);
            ac_items.push(PresentationContextAcItem {
                id: pc.id,
                result: result_byte,
                transfer_syntax: ts.clone(),
            });
            if result_byte == 0 {
                accepted_pcs.push(PresentationContextAc {
                    id: pc.id,
                    result: PcResult::Acceptance,
                    transfer_syntax: ts,
                    abstract_syntax: pc.abstract_syntax.clone(),
                });
            }
        }

        let app_ctx = if rq.application_context.is_empty() {
            APP_CONTEXT_UID.to_string()
        } else {
            rq.application_context.clone()
        };

        let role_selections = negotiate_role_selections(&rq.role_selections, &accepted_pcs, config);
        let asynchronous_operations_window = rq
            .asynchronous_operations_window
            .map(|requested| negotiate_asynchronous_operations_window(requested, config));

        let ac = AssociateAc {
            called_ae_title: rq.called_ae_title.clone(),
            calling_ae_title: rq.calling_ae_title.clone(),
            application_context: app_ctx,
            presentation_contexts: ac_items,
            max_pdu_length: effective_incoming_pdu_length(config),
            implementation_class_uid: config.implementation_class_uid.clone(),
            implementation_version_name: config.implementation_version_name.clone(),
            asynchronous_operations_window,
            role_selections: role_selections.clone(),
        };

        stream.write_all(&pdu::encode_associate_ac(&ac)).await?;

        Ok(Association {
            stream,
            state: AssociationState::Established,
            called_ae: rq.called_ae_title,
            calling_ae: rq.calling_ae_title,
            presentation_contexts: accepted_pcs,
            max_pdu_length: rq.max_pdu_length,
            maximum_outgoing_pdu_length: effective_outgoing_pdu_length(rq.max_pdu_length, config),
            maximum_incoming_pdu_length: effective_incoming_pdu_length(config),
            peer_addr,
            asynchronous_operations_window: asynchronous_operations_window.unwrap_or_default(),
            role_selections,
            is_requestor: false,
            pdv_queue: std::collections::VecDeque::new(),
        })
    }

    // ── P-DATA transfer ───────────────────────────────────────────────────────

    /// Send data as one or more P-DATA-TF PDUs.
    ///
    /// Large payloads are automatically fragmented to fit within
    /// `max_pdu_length`.  The `is_last` flag is set on the final fragment.
    pub async fn send_pdata(
        &mut self,
        context_id: u8,
        data: &[u8],
        is_command: bool,
        is_last: bool,
    ) -> DcmResult<()> {
        self.ensure_established()?;

        if data.len() % 2 != 0 {
            return Err(DcmError::Other(
                "DICOM command and dataset fragment streams must have an even byte length".into(),
            ));
        }

        let max_data = max_pdv_data_length(self.maximum_outgoing_pdu_length, data.len());

        if data.is_empty() {
            let mut control = u8::from(is_command);
            if is_last {
                control |= 0x02;
            }
            return pdu::write_pdata_fragment(&mut self.stream, context_id, control, &[]).await;
        }

        let chunk_count = data.len().div_ceil(max_data);
        for (index, chunk) in data.chunks(max_data).enumerate() {
            let last_fragment = is_last && index == chunk_count - 1;
            // DICOM PS3.8 §9.3.1: bit 0 = command, bit 1 = last
            let mut ctrl: u8 = 0;
            if is_command {
                ctrl |= 0x01;
            }
            if last_fragment {
                ctrl |= 0x02;
            }
            pdu::write_pdata_fragment(&mut self.stream, context_id, ctrl, chunk).await?;
        }
        Ok(())
    }

    /// Receive the next P-DATA PDV.
    ///
    /// Returns `(context_id, is_command, is_last, data)`.
    /// Handles A-ABORT and A-RELEASE-RQ from the peer transparently.
    ///
    /// When a P-DATA-TF contains multiple PDVs (allowed by DICOM PS3.8 §9.3.4),
    /// the remaining PDVs are buffered internally and returned by subsequent
    /// calls without additional network I/O — matching the DCMTK C++ DUL layer
    /// behaviour (`DUL_NextPDV` / `DUL_ReadPDVs`).
    pub async fn recv_pdata(&mut self) -> DcmResult<(u8, bool, bool, Vec<u8>)> {
        self.ensure_established()?;

        self.fill_pdv_queue().await?;

        if let Some(pdv) = self.pdv_queue.pop_front() {
            return Ok((pdv.context_id, pdv.is_command(), pdv.is_last(), pdv.data));
        }

        Err(DcmError::Other(
            "expected a P-DATA-TF PDU but none was available".into(),
        ))
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// Gracefully finish the association release handshake.
    ///
    /// The association requestor sends A-RELEASE-RQ. The acceptor waits for
    /// that request and replies with A-RELEASE-RP, as required by DICOM PS3.8.
    pub async fn release(&mut self) -> DcmResult<()> {
        if self.state != AssociationState::Established {
            return Ok(());
        }
        if !self.is_requestor {
            return self.wait_for_release_request().await;
        }
        self.state = AssociationState::ReleaseRequested;
        self.stream.write_all(&pdu::encode_release_rq()).await?;

        let incoming = timeout(
            Duration::from_secs(30),
            pdu::read_pdu_with_limit(&mut self.stream, self.maximum_incoming_pdu_length),
        )
        .await
        .map_err(|_| DcmError::Timeout { seconds: 30 })??;

        self.state = AssociationState::Closed;

        match incoming {
            Pdu::ReleaseRp => Ok(()),
            Pdu::AAbort(abort) => Err(DcmError::AssociationAborted {
                abort_source: abort.source.to_string(),
                reason: abort.reason.to_string(),
            }),
            _ => Err(DcmError::Other(
                "expected A-RELEASE-RP after sending A-RELEASE-RQ".into(),
            )),
        }
    }

    async fn wait_for_release_request(&mut self) -> DcmResult<()> {
        let incoming = timeout(
            Duration::from_secs(30),
            pdu::read_pdu_with_limit(&mut self.stream, self.maximum_incoming_pdu_length),
        )
        .await
        .map_err(|_| DcmError::Timeout { seconds: 30 })??;

        match incoming {
            Pdu::ReleaseRq => {
                self.stream.write_all(&pdu::encode_release_rp()).await?;
                self.state = AssociationState::Closed;
                Ok(())
            }
            Pdu::AAbort(abort) => {
                self.state = AssociationState::Closed;
                Err(DcmError::AssociationAborted {
                    abort_source: abort.source.to_string(),
                    reason: abort.reason.to_string(),
                })
            }
            _ => Err(DcmError::Other(
                "expected A-RELEASE-RQ while finishing an acceptor association".into(),
            )),
        }
    }

    /// Immediately abort the association by sending an A-ABORT PDU.
    pub async fn abort(&mut self) -> DcmResult<()> {
        let _ = self
            .stream
            .write_all(&pdu::encode_a_abort(&AAbort {
                source: 0,
                reason: 0,
            }))
            .await;
        self.state = AssociationState::Closed;
        Ok(())
    }

    // ── Presentation context lookup ───────────────────────────────────────────

    /// Find an accepted presentation context by its Abstract Syntax UID.
    pub fn find_context(&self, abstract_syntax: &str) -> Option<&PresentationContextAc> {
        self.presentation_contexts
            .iter()
            .find(|pc| pc.result.is_accepted() && pc.abstract_syntax == abstract_syntax)
    }

    /// Find an accepted presentation context matching both SOP Class and Transfer Syntax.
    pub fn find_context_with_transfer_syntax(
        &self,
        abstract_syntax: &str,
        transfer_syntax: &str,
    ) -> Option<&PresentationContextAc> {
        self.presentation_contexts.iter().find(|context| {
            context.result.is_accepted()
                && context.abstract_syntax == abstract_syntax
                && context.transfer_syntax == transfer_syntax
        })
    }

    /// Find an accepted presentation context on which the local AE may act as SCU.
    pub fn find_context_for_scu_with_transfer_syntax(
        &self,
        abstract_syntax: &str,
        transfer_syntax: &str,
    ) -> Option<&PresentationContextAc> {
        self.local_scu_role(abstract_syntax)
            .then(|| self.find_context_with_transfer_syntax(abstract_syntax, transfer_syntax))
            .flatten()
    }

    /// Return whether the local AE negotiated the SCU role for a SOP Class.
    pub fn local_scu_role(&self, sop_class_uid: &str) -> bool {
        let selection = self
            .role_selections
            .iter()
            .find(|selection| selection.sop_class_uid == sop_class_uid);

        match (self.is_requestor, selection) {
            (true, Some(selection)) => selection.scu_role,
            (false, Some(selection)) => selection.scp_role,
            (true, None) => true,
            (false, None) => false,
        }
    }

    /// Return whether the local AE negotiated the SCP role for a SOP Class.
    pub fn local_scp_role(&self, sop_class_uid: &str) -> bool {
        let selection = self
            .role_selections
            .iter()
            .find(|selection| selection.sop_class_uid == sop_class_uid);

        match (self.is_requestor, selection) {
            (true, Some(selection)) => selection.scp_role,
            (false, Some(selection)) => selection.scu_role,
            (true, None) => false,
            (false, None) => true,
        }
    }

    /// Find a presentation context by its context ID.
    pub fn context_by_id(&self, id: u8) -> Option<&PresentationContextAc> {
        self.presentation_contexts.iter().find(|pc| pc.id == id)
    }

    // ── DIMSE helpers ─────────────────────────────────────────────────────────

    /// Encode and send a DIMSE command dataset as command PDVs.
    pub async fn send_dimse_command(&mut self, context_id: u8, command: &DataSet) -> DcmResult<()> {
        let bytes = dimse::encode_command_dataset(command);
        self.send_pdata(context_id, &bytes, true, true).await
    }

    /// Send pre-encoded DIMSE data (e.g. an SOP instance) as data PDVs.
    pub async fn send_dimse_data(&mut self, context_id: u8, data: &[u8]) -> DcmResult<()> {
        self.send_pdata(context_id, data, false, true).await
    }

    /// Send an encoded dataset from memory or a bounded file region.
    pub async fn send_dimse_data_source(
        &mut self,
        context_id: u8,
        source: &DatasetSource,
    ) -> DcmResult<()> {
        match source {
            DatasetSource::Bytes(bytes) => self.send_dimse_data(context_id, bytes).await,
            DatasetSource::File(file) => self.send_dimse_file(context_id, file).await,
        }
    }

    async fn send_dimse_file(&mut self, context_id: u8, source: &FileDataset) -> DcmResult<()> {
        let mut file = tokio::fs::File::open(source.path()).await?;
        let file_length = file.metadata().await?.len();
        let offset = source.offset();

        if offset > file_length {
            return Err(DcmError::InvalidFile {
                reason: format!(
                    "dataset offset {offset} exceeds file length {file_length} for {}",
                    source.path().display()
                ),
            });
        }

        let available = file_length - offset;
        let length = source.length().unwrap_or(available);
        if length > available {
            return Err(DcmError::InvalidFile {
                reason: format!(
                    "dataset region length {length} exceeds {available} available bytes for {}",
                    source.path().display()
                ),
            });
        }

        file.seek(std::io::SeekFrom::Start(offset)).await?;
        self.send_pdata_reader(context_id, &mut file, length, false)
            .await
    }

    /// Stream exactly `length` bytes from an async reader as DIMSE data PDVs.
    pub async fn send_pdata_reader<R: AsyncRead + Unpin>(
        &mut self,
        context_id: u8,
        reader: &mut R,
        length: u64,
        is_command: bool,
    ) -> DcmResult<()> {
        self.ensure_established()?;

        if length % 2 != 0 {
            return Err(DcmError::Other(
                "DICOM command and dataset fragment streams must have an even byte length".into(),
            ));
        }

        let maximum_fragment_length =
            max_pdv_data_length(self.maximum_outgoing_pdu_length, usize::MAX);
        let mut buffer = vec![0u8; maximum_fragment_length];
        let mut remaining = length;

        if remaining == 0 {
            let control = u8::from(is_command) | 0x02;
            return pdu::write_pdata_fragment(&mut self.stream, context_id, control, &[]).await;
        }

        while remaining > 0 {
            let requested = usize::try_from(remaining.min(maximum_fragment_length as u64))
                .map_err(|_| DcmError::Other("DIMSE fragment length conversion failed".into()))?;
            reader.read_exact(&mut buffer[..requested]).await?;
            remaining -= requested as u64;

            let mut control = u8::from(is_command);
            if remaining == 0 {
                control |= 0x02;
            }
            pdu::write_pdata_fragment(&mut self.stream, context_id, control, &buffer[..requested])
                .await?;
        }

        Ok(())
    }

    /// Collect command PDVs until the last fragment and decode them.
    ///
    /// Returns `(context_id, command_dataset)`.
    pub async fn recv_dimse_command(&mut self) -> DcmResult<(u8, DataSet)> {
        let mut all_data: Vec<u8> = Vec::new();
        // Initialised to 0; overwritten by the first command PDV received.
        #[allow(unused_assignments)]
        let mut ctx_id = 0u8;

        loop {
            let (cid, is_cmd, is_last, data) = self.recv_pdata().await?;
            if is_cmd {
                ctx_id = cid;
                all_data.extend_from_slice(&data);
                if is_last {
                    break;
                }
            }
            // Non-command PDVs are skipped — they should not appear before the
            // command is complete, but we handle them defensively.
        }

        let ds = dimse::decode_command_dataset(&all_data)?;
        Ok((ctx_id, ds))
    }

    /// Collect data PDVs until the last fragment and return the raw bytes.
    pub async fn recv_dimse_data(&mut self) -> DcmResult<Vec<u8>> {
        let mut all_data: Vec<u8> = Vec::new();

        loop {
            let (_, is_cmd, is_last, data) = self.recv_pdata().await?;
            if !is_cmd {
                all_data.extend_from_slice(&data);
                if is_last {
                    break;
                }
            }
        }
        Ok(all_data)
    }

    /// Collect data PDVs if present, but tolerate peers that immediately send the
    /// next DIMSE command instead of a dataset PDV.
    ///
    /// Returns:
    /// - `Ok(Some(bytes))` if one or more data PDVs were received
    /// - `Ok(None)` if the next queued PDV was another DIMSE command
    pub async fn recv_optional_dimse_data(&mut self) -> DcmResult<Option<Vec<u8>>> {
        self.ensure_established()?;
        let mut all_data = Vec::new();
        let mut saw_data_pdv = false;

        loop {
            self.fill_pdv_queue().await?;

            if self.pdv_queue.front().is_some_and(Pdv::is_command) {
                return if saw_data_pdv {
                    Ok(Some(all_data))
                } else {
                    Ok(None)
                };
            }

            let Some(pdv) = self.pdv_queue.pop_front() else {
                return if saw_data_pdv {
                    Ok(Some(all_data))
                } else {
                    Ok(None)
                };
            };

            saw_data_pdv = true;
            all_data.extend_from_slice(&pdv.data);
            if pdv.is_last() {
                return Ok(Some(all_data));
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn ensure_established(&self) -> DcmResult<()> {
        if self.state != AssociationState::Established {
            return Err(DcmError::Other(
                "operation requires an established association".into(),
            ));
        }
        Ok(())
    }

    async fn fill_pdv_queue(&mut self) -> DcmResult<()> {
        self.ensure_established()?;
        if !self.pdv_queue.is_empty() {
            return Ok(());
        }

        loop {
            let pdu = pdu::read_pdu_with_limit(&mut self.stream, self.maximum_incoming_pdu_length)
                .await?;

            match pdu {
                Pdu::PDataTf(pd) => {
                    if pd.pdvs.is_empty() {
                        continue;
                    }
                    self.pdv_queue.extend(pd.pdvs);
                    return Ok(());
                }
                Pdu::AAbort(abort) => {
                    self.state = AssociationState::Closed;
                    return Err(DcmError::AssociationAborted {
                        abort_source: abort.source.to_string(),
                        reason: abort.reason.to_string(),
                    });
                }
                Pdu::ReleaseRq => {
                    let _ = self.stream.write_all(&pdu::encode_release_rp()).await;
                    self.state = AssociationState::Closed;
                    return Err(DcmError::Other("association released by peer".into()));
                }
                _ => {}
            }
        }
    }
}

// ── SCP negotiation helpers ───────────────────────────────────────────────────

/// Decide whether to accept a proposed presentation context and which TS to use.
///
/// Returns `(result_byte, accepted_transfer_syntax)`.
fn negotiate_pc(pc: &PresentationContextRqItem, config: &AssociationConfig) -> (u8, String) {
    // Check abstract syntax acceptability
    if !config.accepted_abstract_syntaxes.is_empty()
        && !config
            .accepted_abstract_syntaxes
            .iter()
            .any(|a| a == &pc.abstract_syntax)
    {
        return (3, TS_IMPLICIT_VR_LE.to_string()); // abstract syntax not supported
    }

    let ts = choose_ts(&pc.transfer_syntaxes, config);
    match ts {
        Some(t) => (0, t),
        None => (4, TS_IMPLICIT_VR_LE.to_string()), // transfer syntaxes not supported
    }
}

/// Choose the best transfer syntax from an offered list based on config policy.
fn choose_ts(offered: &[String], config: &AssociationConfig) -> Option<String> {
    if config.accept_all_transfer_syntaxes {
        return choose_preferred_ts(offered, &config.preferred_transfer_syntaxes)
            .or_else(|| offered.first().cloned());
    }

    let allowed: Vec<&String> = if config.accepted_transfer_syntaxes.is_empty() {
        offered.iter().collect()
    } else {
        offered
            .iter()
            .filter(|ts| {
                config
                    .accepted_transfer_syntaxes
                    .iter()
                    .any(|allowed| allowed == *ts)
            })
            .collect()
    };

    if allowed.is_empty() {
        return None;
    }

    choose_preferred_ts_refs(&allowed, &config.preferred_transfer_syntaxes).or_else(|| {
        if config.accepted_transfer_syntaxes.is_empty() {
            choose_default_uncompressed_ts(&allowed)
        } else {
            allowed.first().map(|ts| (*ts).clone())
        }
    })
}

fn choose_preferred_ts(offered: &[String], preferred: &[String]) -> Option<String> {
    preferred.iter().find_map(|candidate| {
        offered
            .iter()
            .find(|offered_ts| *offered_ts == candidate)
            .cloned()
    })
}

fn choose_preferred_ts_refs(offered: &[&String], preferred: &[String]) -> Option<String> {
    preferred.iter().find_map(|candidate| {
        offered
            .iter()
            .find(|offered_ts| ***offered_ts == *candidate)
            .map(|ts| (*ts).clone())
    })
}

fn max_pdv_data_length(max_pdu_length: u32, data_len: usize) -> usize {
    const PDV_OVERHEAD: usize = 6;
    let available = (max_pdu_length as usize)
        .saturating_sub(PDV_OVERHEAD)
        .max(2);
    let even_available = available - (available % 2);
    even_available.min(data_len.max(1))
}

fn validate_pdu_limits(config: &AssociationConfig) -> DcmResult<()> {
    const MINIMUM_PDU_VARIABLE_FIELD_LENGTH: u32 = 8;

    if config.maximum_incoming_pdu_length < MINIMUM_PDU_VARIABLE_FIELD_LENGTH {
        return Err(DcmError::Other(format!(
            "maximum_incoming_pdu_length must be at least {MINIMUM_PDU_VARIABLE_FIELD_LENGTH} bytes"
        )));
    }
    if config.maximum_outgoing_pdu_length < MINIMUM_PDU_VARIABLE_FIELD_LENGTH {
        return Err(DcmError::Other(format!(
            "maximum_outgoing_pdu_length must be at least {MINIMUM_PDU_VARIABLE_FIELD_LENGTH} bytes"
        )));
    }
    Ok(())
}

fn effective_incoming_pdu_length(config: &AssociationConfig) -> u32 {
    if config.max_pdu_length == 0 {
        config.maximum_incoming_pdu_length
    } else {
        config
            .max_pdu_length
            .min(config.maximum_incoming_pdu_length)
    }
}

fn effective_outgoing_pdu_length(peer_maximum_pdu_length: u32, config: &AssociationConfig) -> u32 {
    if peer_maximum_pdu_length == 0 {
        config.maximum_outgoing_pdu_length
    } else {
        peer_maximum_pdu_length.min(config.maximum_outgoing_pdu_length)
    }
}

fn validate_requested_role_selections(
    contexts: &[PresentationContextRq],
    role_selections: &[ScpScuRoleSelection],
) -> DcmResult<()> {
    let mut seen_sop_classes = std::collections::HashSet::new();
    for role in role_selections {
        dicom_toolkit_core::uid::Uid::new(role.sop_class_uid.clone())?;
        if !role.scu_role && !role.scp_role {
            return Err(DcmError::Other(format!(
                "role selection for {} proposes neither SCU nor SCP role",
                role.sop_class_uid
            )));
        }
        if !contexts
            .iter()
            .any(|context| context.abstract_syntax == role.sop_class_uid)
        {
            return Err(DcmError::Other(format!(
                "role selection SOP Class {} has no proposed presentation context",
                role.sop_class_uid
            )));
        }
        if !seen_sop_classes.insert(&role.sop_class_uid) {
            return Err(DcmError::Other(format!(
                "duplicate role selection for SOP Class {}",
                role.sop_class_uid
            )));
        }
    }
    Ok(())
}

fn validate_accepted_role_selections(
    proposed: &[ScpScuRoleSelection],
    accepted: &[ScpScuRoleSelection],
) -> DcmResult<()> {
    let mut seen_sop_classes = std::collections::HashSet::new();
    for role in accepted {
        let proposed_role = proposed
            .iter()
            .find(|proposal| proposal.sop_class_uid == role.sop_class_uid)
            .ok_or_else(|| {
                DcmError::Other(format!(
                    "peer returned an unrequested role selection for {}",
                    role.sop_class_uid
                ))
            })?;
        if role.scu_role && !proposed_role.scu_role {
            return Err(DcmError::Other(format!(
                "peer accepted an unproposed SCU role for {}",
                role.sop_class_uid
            )));
        }
        if role.scp_role && !proposed_role.scp_role {
            return Err(DcmError::Other(format!(
                "peer accepted an unproposed SCP role for {}",
                role.sop_class_uid
            )));
        }
        if !seen_sop_classes.insert(&role.sop_class_uid) {
            return Err(DcmError::Other(format!(
                "peer returned duplicate role selections for {}",
                role.sop_class_uid
            )));
        }
    }
    Ok(())
}

fn validate_incoming_role_selections(
    contexts: &[PresentationContextRqItem],
    role_selections: &[ScpScuRoleSelection],
) -> DcmResult<()> {
    let mut seen_sop_classes = std::collections::HashSet::new();
    for role in role_selections {
        dicom_toolkit_core::uid::Uid::new(role.sop_class_uid.clone())?;
        if !role.scu_role && !role.scp_role {
            return Err(DcmError::Other(format!(
                "peer role selection for {} proposes neither SCU nor SCP role",
                role.sop_class_uid
            )));
        }
        if !contexts
            .iter()
            .any(|context| context.abstract_syntax == role.sop_class_uid)
        {
            return Err(DcmError::Other(format!(
                "peer role selection SOP Class {} has no presentation context",
                role.sop_class_uid
            )));
        }
        if !seen_sop_classes.insert(&role.sop_class_uid) {
            return Err(DcmError::Other(format!(
                "peer proposed duplicate role selections for {}",
                role.sop_class_uid
            )));
        }
    }
    Ok(())
}

fn negotiate_role_selections(
    proposed: &[ScpScuRoleSelection],
    accepted_contexts: &[PresentationContextAc],
    config: &AssociationConfig,
) -> Vec<ScpScuRoleSelection> {
    proposed
        .iter()
        .filter(|role| {
            accepted_contexts
                .iter()
                .any(|context| context.abstract_syntax == role.sop_class_uid)
        })
        .map(|role| ScpScuRoleSelection {
            sop_class_uid: role.sop_class_uid.clone(),
            scu_role: role.scu_role && config.accept_requestor_scu_role,
            scp_role: role.scp_role && config.accept_requestor_scp_role,
        })
        .collect()
}

fn negotiate_asynchronous_operations_window(
    requestor: AsynchronousOperationsWindow,
    config: &AssociationConfig,
) -> AsynchronousOperationsWindow {
    let acceptor = config.maximum_asynchronous_operations_window;
    AsynchronousOperationsWindow {
        invoked: negotiate_window_value(acceptor.invoked, requestor.performed),
        performed: negotiate_window_value(acceptor.performed, requestor.invoked),
    }
}

fn requestor_window_from_acceptance(
    requested: Option<AsynchronousOperationsWindow>,
    accepted: Option<AsynchronousOperationsWindow>,
) -> DcmResult<AsynchronousOperationsWindow> {
    let Some(requested) = requested else {
        return Ok(AsynchronousOperationsWindow::default());
    };
    let Some(accepted) = accepted else {
        return Ok(AsynchronousOperationsWindow::default());
    };

    if exceeds_window(accepted.performed, requested.invoked)
        || exceeds_window(accepted.invoked, requested.performed)
    {
        return Err(DcmError::Other(
            "peer returned an asynchronous operations window larger than proposed".into(),
        ));
    }

    Ok(AsynchronousOperationsWindow {
        invoked: accepted.performed,
        performed: accepted.invoked,
    })
}

fn negotiate_window_value(local: u16, remote: u16) -> u16 {
    match (local, remote) {
        (0, 0) => 0,
        (0, remote) => remote,
        (local, 0) => local,
        (local, remote) => local.min(remote),
    }
}

fn exceeds_window(accepted: u16, proposed: u16) -> bool {
    proposed != 0 && (accepted == 0 || accepted > proposed)
}

fn choose_default_uncompressed_ts(offered: &[&String]) -> Option<String> {
    for preferred in &[TS_EXPLICIT_VR_LE, TS_IMPLICIT_VR_LE] {
        if let Some(ts) = offered
            .iter()
            .find(|offered_ts| ***offered_ts == *preferred)
        {
            return Some((*ts).clone());
        }
    }
    None
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dimse;
    use crate::pdu::{self, AssociateRq, Pdu, Pdv, PresentationContextRqItem};
    use dicom_toolkit_core::uid::sop_class;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        sync::oneshot,
    };

    #[test]
    fn negotiate_pc_accept_all() {
        let config = AssociationConfig {
            accept_all_transfer_syntaxes: true,
            ..Default::default()
        };

        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec!["1.2.840.10008.1.2".to_string()],
        };
        let (result, ts) = negotiate_pc(&pc, &config);
        assert_eq!(result, 0);
        assert_eq!(ts, "1.2.840.10008.1.2");
    }

    #[test]
    fn negotiate_pc_prefer_explicit_le() {
        let config = AssociationConfig::default();
        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec![TS_IMPLICIT_VR_LE.to_string(), TS_EXPLICIT_VR_LE.to_string()],
        };
        let (result, ts) = negotiate_pc(&pc, &config);
        assert_eq!(result, 0);
        assert_eq!(ts, TS_EXPLICIT_VR_LE);
    }

    #[test]
    fn negotiate_pc_unsupported_ts() {
        let config = AssociationConfig::default(); // accept_all = false
        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec!["1.2.840.10008.1.2.4.50".to_string()], // JPEG Baseline only
        };
        let (result, _) = negotiate_pc(&pc, &config);
        assert_eq!(result, 4); // transfer syntaxes not supported
    }

    #[test]
    fn negotiate_pc_respects_accepted_transfer_syntaxes() {
        let config = AssociationConfig {
            accepted_transfer_syntaxes: vec!["1.2.840.10008.1.2.4.50".to_string()],
            ..Default::default()
        };
        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec![
                TS_EXPLICIT_VR_LE.to_string(),
                "1.2.840.10008.1.2.4.50".to_string(),
            ],
        };
        let (result, ts) = negotiate_pc(&pc, &config);
        assert_eq!(result, 0);
        assert_eq!(ts, "1.2.840.10008.1.2.4.50");
    }

    #[test]
    fn negotiate_pc_prefers_custom_transfer_syntax_order() {
        let config = AssociationConfig {
            accepted_transfer_syntaxes: vec![
                TS_EXPLICIT_VR_LE.to_string(),
                "1.2.840.10008.1.2.4.50".to_string(),
            ],
            preferred_transfer_syntaxes: vec![
                "1.2.840.10008.1.2.4.50".to_string(),
                TS_EXPLICIT_VR_LE.to_string(),
            ],
            ..Default::default()
        };
        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec![
                TS_EXPLICIT_VR_LE.to_string(),
                "1.2.840.10008.1.2.4.50".to_string(),
            ],
        };
        let (result, ts) = negotiate_pc(&pc, &config);
        assert_eq!(result, 0);
        assert_eq!(ts, "1.2.840.10008.1.2.4.50");
    }

    #[test]
    fn negotiate_pc_unsupported_abstract_syntax() {
        let config = AssociationConfig {
            accepted_abstract_syntaxes: vec!["1.2.840.10008.1.1".to_string()],
            ..Default::default()
        };

        let pc = PresentationContextRqItem {
            id: 1,
            abstract_syntax: "1.2.840.10008.5.1.4.1.1.2".to_string(), // CT Image Storage
            transfer_syntaxes: vec![TS_EXPLICIT_VR_LE.to_string()],
        };
        let (result, _) = negotiate_pc(&pc, &config);
        assert_eq!(result, 3); // abstract syntax not supported
    }

    // ── Loopback integration test ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_echo_loopback() {
        use crate::services::echo::c_echo;
        use dicom_toolkit_dict::tags;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // ── SCP task ────────────────────────────────────────────────────────
        let scp_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let config = AssociationConfig {
                accept_all_transfer_syntaxes: true,
                ..Default::default()
            };

            let mut assoc = Association::accept(stream, &config).await.unwrap();

            // Receive C-ECHO-RQ
            let (ctx_id, cmd) = assoc.recv_dimse_command().await.unwrap();
            let msg_id = cmd.get_u16(tags::MESSAGE_ID).unwrap_or(1);

            // Build and send C-ECHO-RSP
            let mut rsp = DataSet::new();
            rsp.set_uid(tags::AFFECTED_SOP_CLASS_UID, "1.2.840.10008.1.1");
            rsp.set_u16(tags::COMMAND_FIELD, 0x8030); // C-ECHO-RSP
            rsp.set_u16(tags::MESSAGE_ID_BEING_RESPONDED_TO, msg_id);
            rsp.set_u16(tags::COMMAND_DATA_SET_TYPE, 0x0101);
            rsp.set_u16(tags::STATUS, 0x0000);
            assoc.send_dimse_command(ctx_id, &rsp).await.unwrap();

            // Absorb the incoming A-RELEASE-RQ and respond automatically
            let _ = assoc.recv_pdata().await;
        });

        // ── SCU side ────────────────────────────────────────────────────────
        let config = AssociationConfig::default();
        let contexts = vec![PresentationContextRq {
            id: 1,
            abstract_syntax: "1.2.840.10008.1.1".to_string(),
            transfer_syntaxes: vec![TS_EXPLICIT_VR_LE.to_string()],
        }];

        let mut assoc = Association::request(&addr.to_string(), "SCP", "SCU", &contexts, &config)
            .await
            .unwrap();

        let ctx_id = assoc
            .find_context("1.2.840.10008.1.1")
            .expect("context not found")
            .id;

        c_echo(&mut assoc, ctx_id).await.unwrap();
        assoc.release().await.unwrap();

        scp_handle.await.unwrap();
    }

    fn find_context_item() -> PresentationContextRqItem {
        PresentationContextRqItem {
            id: 1,
            abstract_syntax: sop_class::PATIENT_ROOT_QR_FIND.to_string(),
            transfer_syntaxes: vec![TS_EXPLICIT_VR_LE.to_string()],
        }
    }

    fn associate_rq(max_pdu_length: u32) -> AssociateRq {
        AssociateRq {
            called_ae_title: "SCP".into(),
            calling_ae_title: "SCU".into(),
            application_context: APP_CONTEXT_UID.to_string(),
            presentation_contexts: vec![find_context_item()],
            max_pdu_length,
            implementation_class_uid: "1.2.826.0.1.3680043.8.498".into(),
            implementation_version_name: "TEST".into(),
            asynchronous_operations_window: None,
            role_selections: Vec::new(),
        }
    }

    fn find_command(command_data_set_type: u16) -> DataSet {
        use dicom_toolkit_dict::tags;

        let mut cmd = DataSet::new();
        cmd.set_uid(
            tags::AFFECTED_SOP_CLASS_UID,
            sop_class::PATIENT_ROOT_QR_FIND,
        );
        cmd.set_u16(tags::COMMAND_FIELD, 0x0020);
        cmd.set_u16(tags::MESSAGE_ID, 1);
        cmd.set_u16(tags::PRIORITY, 0);
        cmd.set_u16(tags::COMMAND_DATA_SET_TYPE, command_data_set_type);
        cmd
    }

    fn echo_command() -> DataSet {
        use dicom_toolkit_dict::tags;

        let mut cmd = DataSet::new();
        cmd.set_uid(tags::AFFECTED_SOP_CLASS_UID, "1.2.840.10008.1.1");
        cmd.set_u16(tags::COMMAND_FIELD, 0x0030);
        cmd.set_u16(tags::MESSAGE_ID, 2);
        cmd.set_u16(tags::COMMAND_DATA_SET_TYPE, 0x0101);
        cmd
    }

    async fn connect_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.expect("connect") });
        let (server, _) = listener.accept().await.expect("accept");
        let client = client.await.expect("join client task");
        (server, client)
    }

    #[test]
    fn max_pdv_data_length_honors_effective_limit() {
        assert_eq!(max_pdv_data_length(16_384, 32_768), 16_378);
        assert_eq!(max_pdv_data_length(8, 64), 2);
        assert_eq!(max_pdv_data_length(65_536, 128), 128);
    }

    #[tokio::test]
    async fn accept_uses_requestor_max_pdu_length_for_outbound_limit() {
        let (server_stream, mut client_stream) = connect_pair().await;
        let (done_tx, done_rx) = oneshot::channel();

        tokio::spawn(async move {
            let assoc = Association::accept(server_stream, &AssociationConfig::default())
                .await
                .expect("accept association");
            done_tx
                .send(assoc.max_pdu_length)
                .expect("send negotiated max pdu");
        });

        client_stream
            .write_all(&pdu::encode_associate_rq(&associate_rq(16_384)))
            .await
            .expect("send associate-rq");
        match pdu::read_pdu(&mut client_stream)
            .await
            .expect("read associate-ac")
        {
            Pdu::AssociateAc(_) => {}
            other => panic!("expected AssociateAc, got {other:?}"),
        }

        assert_eq!(done_rx.await.expect("receive max pdu"), 16_384);
    }

    #[tokio::test]
    async fn recv_optional_dimse_data_keeps_next_command_queued() {
        let (server_stream, mut client_stream) = connect_pair().await;

        let (done_tx, done_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut assoc = Association::accept(server_stream, &AssociationConfig::default())
                .await
                .expect("accept association");

            let (ctx_id, find_cmd) = assoc.recv_dimse_command().await.expect("receive command");
            assert_eq!(ctx_id, 1);
            assert_eq!(
                find_cmd.get_u16(dicom_toolkit_dict::tags::COMMAND_FIELD),
                Some(0x0020)
            );

            let query_bytes = assoc
                .recv_optional_dimse_data()
                .await
                .expect("receive optional query data");
            assert!(query_bytes.is_none());

            let (_, next_cmd) = assoc
                .recv_dimse_command()
                .await
                .expect("receive queued follow-up command");
            done_tx
                .send(next_cmd.get_u16(dicom_toolkit_dict::tags::COMMAND_FIELD))
                .expect("send command field");
        });

        client_stream
            .write_all(&pdu::encode_associate_rq(&associate_rq(16_384)))
            .await
            .expect("send associate-rq");
        match pdu::read_pdu(&mut client_stream)
            .await
            .expect("read associate-ac")
        {
            Pdu::AssociateAc(_) => {}
            other => panic!("expected AssociateAc, got {other:?}"),
        }

        let pdus = pdu::encode_p_data_tf(&[
            Pdv {
                context_id: 1,
                msg_control: 0x03,
                data: dimse::encode_command_dataset(&find_command(0x0000)),
            },
            Pdv {
                context_id: 1,
                msg_control: 0x03,
                data: dimse::encode_command_dataset(&echo_command()),
            },
        ]);
        client_stream
            .write_all(&pdus)
            .await
            .expect("send back-to-back commands");

        assert_eq!(done_rx.await.expect("receive next command"), Some(0x0030));
    }

    #[tokio::test]
    async fn recv_optional_dimse_data_tolerates_empty_data_pdv_before_next_command() {
        let (server_stream, mut client_stream) = connect_pair().await;

        let (done_tx, done_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut assoc = Association::accept(server_stream, &AssociationConfig::default())
                .await
                .expect("accept association");

            let (_, store_cmd) = assoc.recv_dimse_command().await.expect("receive command");
            assert_eq!(
                store_cmd.get_u16(dicom_toolkit_dict::tags::COMMAND_FIELD),
                Some(0x0001)
            );

            let data = assoc
                .recv_optional_dimse_data()
                .await
                .expect("receive optional store data");
            let (_, next_cmd) = assoc
                .recv_dimse_command()
                .await
                .expect("receive queued follow-up command");

            done_tx
                .send((
                    data,
                    next_cmd.get_u16(dicom_toolkit_dict::tags::COMMAND_FIELD),
                ))
                .expect("send result");
        });

        client_stream
            .write_all(&pdu::encode_associate_rq(&associate_rq(16_384)))
            .await
            .expect("send associate-rq");
        match pdu::read_pdu(&mut client_stream)
            .await
            .expect("read associate-ac")
        {
            Pdu::AssociateAc(_) => {}
            other => panic!("expected AssociateAc, got {other:?}"),
        }

        let mut store_cmd = DataSet::new();
        store_cmd.set_uid(
            dicom_toolkit_dict::tags::AFFECTED_SOP_CLASS_UID,
            sop_class::CT_IMAGE_STORAGE,
        );
        store_cmd.set_u16(dicom_toolkit_dict::tags::COMMAND_FIELD, 0x0001);
        store_cmd.set_u16(dicom_toolkit_dict::tags::MESSAGE_ID, 1);
        store_cmd.set_u16(dicom_toolkit_dict::tags::PRIORITY, 0);
        store_cmd.set_u16(dicom_toolkit_dict::tags::COMMAND_DATA_SET_TYPE, 0x0000);
        store_cmd.set_uid(
            dicom_toolkit_dict::tags::AFFECTED_SOP_INSTANCE_UID,
            "1.2.3.4.5",
        );

        let pdus = pdu::encode_p_data_tf(&[
            Pdv {
                context_id: 1,
                msg_control: 0x03,
                data: dimse::encode_command_dataset(&store_cmd),
            },
            Pdv {
                context_id: 1,
                msg_control: 0x00,
                data: Vec::new(),
            },
            Pdv {
                context_id: 1,
                msg_control: 0x03,
                data: dimse::encode_command_dataset(&echo_command()),
            },
        ]);
        client_stream
            .write_all(&pdus)
            .await
            .expect("send store command, empty data PDV, then next command");

        let (data, next_command_field) = done_rx.await.expect("receive result");
        assert_eq!(data, Some(Vec::new()));
        assert_eq!(next_command_field, Some(0x0030));
    }

    #[tokio::test]
    async fn file_dataset_streams_exact_region_with_bounded_pdus() {
        use std::io::Write;

        let prefix = vec![0xAA; 12];
        let payload = (0u8..100).collect::<Vec<_>>();
        let mut source_file = tempfile::NamedTempFile::new().expect("create source file");
        source_file
            .as_file_mut()
            .write_all(&prefix)
            .expect("write prefix");
        source_file
            .as_file_mut()
            .write_all(&payload)
            .expect("write payload");

        let source = DatasetSource::file_region(
            source_file.path(),
            prefix.len() as u64,
            payload.len() as u64,
        );
        let (server_stream, mut client_stream) = connect_pair().await;

        let server_task = tokio::spawn(async move {
            let mut association = Association::accept(server_stream, &AssociationConfig::default())
                .await
                .expect("accept association");
            association
                .send_dimse_data_source(1, &source)
                .await
                .expect("stream file dataset");
        });

        client_stream
            .write_all(&pdu::encode_associate_rq(&associate_rq(32)))
            .await
            .expect("send associate-rq");
        assert!(matches!(
            pdu::read_pdu(&mut client_stream)
                .await
                .expect("read associate-ac"),
            Pdu::AssociateAc(_)
        ));

        let mut received = Vec::new();
        loop {
            let pdu = pdu::read_pdu_with_limit(&mut client_stream, 32)
                .await
                .expect("read bounded P-DATA-TF");
            let Pdu::PDataTf(pdata) = pdu else {
                panic!("expected P-DATA-TF");
            };
            let variable_field_length = pdata
                .pdvs
                .iter()
                .map(|pdv| pdv.data.len() + 6)
                .sum::<usize>();
            assert!(variable_field_length <= 32);

            let mut last = false;
            for pdv in pdata.pdvs {
                assert_eq!(pdv.context_id, 1);
                assert!(!pdv.is_command());
                assert_eq!(pdv.data.len() % 2, 0);
                last = pdv.is_last();
                received.extend_from_slice(&pdv.data);
            }
            if last {
                break;
            }
        }

        server_task.await.expect("server task");
        assert_eq!(received, payload);
    }
}
