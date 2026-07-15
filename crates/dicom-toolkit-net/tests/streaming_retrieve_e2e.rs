use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dicom_toolkit_core::error::DcmResult;
use dicom_toolkit_core::uid::sop_class;
use dicom_toolkit_data::{DataSet, DicomReader, DicomWriter};
use dicom_toolkit_dict::{tags, Tag, Vr};
use dicom_toolkit_net::PresentationContextRq;
use dicom_toolkit_net::{
    c_get, Association, AssociationConfig, AssociationOptions, DatasetSource, DicomServer,
    GetEvent, GetRequest, GetRetrievePlan, MoveEvent, MoveRetrievePlan,
    RetrievePresentationContext, RetrieveSubOperation, ScpScuRoleSelection,
    StaticDestinationLookup, StreamingGetServiceProvider, StreamingMoveServiceProvider,
    StreamingRetrieveItem, StreamingRetrieveItemStream, STATUS_CANCEL, STATUS_WARNING,
};
use futures_util::stream;

const TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN: &str = "1.2.840.10008.1.2.1";
const TRANSFER_SYNTAX_IMPLICIT_VR_LITTLE_ENDIAN: &str = "1.2.840.10008.1.2";

fn loopback_address(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V4(address) if address.ip().is_unspecified() => {
            SocketAddr::from((Ipv4Addr::LOCALHOST, address.port()))
        }
        SocketAddr::V6(address) if address.ip().is_unspecified() => {
            SocketAddr::from((Ipv6Addr::LOCALHOST, address.port()))
        }
        _ => address,
    }
}

fn query_retrieve_get_context(identifier: u8) -> PresentationContextRq {
    PresentationContextRq {
        id: identifier,
        abstract_syntax: sop_class::PATIENT_ROOT_QR_GET.to_string(),
        transfer_syntaxes: vec![TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string()],
    }
}

fn storage_context(identifier: u8) -> PresentationContextRq {
    PresentationContextRq {
        id: identifier,
        abstract_syntax: sop_class::CT_IMAGE_STORAGE.to_string(),
        transfer_syntaxes: vec![TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string()],
    }
}

fn query_retrieve_move_context(identifier: u8) -> PresentationContextRq {
    PresentationContextRq {
        id: identifier,
        abstract_syntax: sop_class::PATIENT_ROOT_QR_MOVE.to_string(),
        transfer_syntaxes: vec![TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string()],
    }
}

fn requestor_options() -> AssociationOptions {
    AssociationOptions {
        requested_role_selections: vec![ScpScuRoleSelection {
            sop_class_uid: sop_class::CT_IMAGE_STORAGE.to_string(),
            scu_role: false,
            scp_role: true,
        }],
        ..Default::default()
    }
}

fn encode_dataset(dataset: &DataSet) -> Vec<u8> {
    let mut encoded = Vec::new();
    DicomWriter::new(&mut encoded)
        .write_dataset(dataset, TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN)
        .expect("encode dataset");
    encoded
}

fn encoded_instance(sop_instance_uid: &str) -> Vec<u8> {
    let mut dataset = DataSet::new();
    dataset.set_string(tags::SOP_CLASS_UID, Vr::UI, sop_class::CT_IMAGE_STORAGE);
    dataset.set_string(tags::SOP_INSTANCE_UID, Vr::UI, sop_instance_uid);
    encode_dataset(&dataset)
}

fn query_identifier() -> Vec<u8> {
    let mut dataset = DataSet::new();
    dataset.set_string(tags::PATIENT_ID, Vr::LO, "STREAMING-TEST");
    encode_dataset(&dataset)
}

fn get_request_command(message_id: u16) -> DataSet {
    let mut command = DataSet::new();
    command.set_uid(tags::AFFECTED_SOP_CLASS_UID, sop_class::PATIENT_ROOT_QR_GET);
    command.set_u16(tags::COMMAND_FIELD, 0x0010);
    command.set_u16(tags::MESSAGE_ID, message_id);
    command.set_u16(tags::PRIORITY, 0);
    command.set_u16(tags::COMMAND_DATA_SET_TYPE, 0x0000);
    command
}

fn cancel_command(message_id_being_responded_to: u16) -> DataSet {
    let mut command = DataSet::new();
    command.set_u16(tags::COMMAND_FIELD, 0x0FFF);
    command.set_u16(
        tags::MESSAGE_ID_BEING_RESPONDED_TO,
        message_id_being_responded_to,
    );
    command.set_u16(tags::COMMAND_DATA_SET_TYPE, 0x0101);
    command
}

fn move_request_command(message_id: u16, destination: &str) -> DataSet {
    let mut command = DataSet::new();
    command.set_uid(
        tags::AFFECTED_SOP_CLASS_UID,
        sop_class::PATIENT_ROOT_QR_MOVE,
    );
    command.set_u16(tags::COMMAND_FIELD, 0x0021);
    command.set_u16(tags::MESSAGE_ID, message_id);
    command.set_u16(tags::PRIORITY, 0);
    command.set_u16(tags::COMMAND_DATA_SET_TYPE, 0x0000);
    command.set_string(tags::MOVE_DESTINATION, Vr::AE, destination);
    command
}

struct FixedStreamingGetProvider {
    items: Vec<StreamingRetrieveItem>,
}

impl StreamingGetServiceProvider for FixedStreamingGetProvider {
    async fn on_get_stream(&self, _event: GetEvent) -> DcmResult<GetRetrievePlan> {
        GetRetrievePlan::from_items(self.items.clone())
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PendingStreamingGetProvider {
    stream_dropped: Arc<AtomicBool>,
}

struct PendingStreamingMoveProvider {
    stream_dropped: Arc<AtomicBool>,
}

impl StreamingMoveServiceProvider for PendingStreamingMoveProvider {
    async fn on_move_stream(&self, _event: MoveEvent) -> DcmResult<MoveRetrievePlan> {
        let drop_signal = DropSignal(Arc::clone(&self.stream_dropped));
        let items: StreamingRetrieveItemStream =
            Box::pin(stream::unfold(drop_signal, |drop_signal| async move {
                let _drop_signal = drop_signal;
                std::future::pending::<Option<(DcmResult<RetrieveSubOperation>, DropSignal)>>()
                    .await
            }));
        MoveRetrievePlan::new(
            3,
            vec![RetrievePresentationContext::new(
                sop_class::CT_IMAGE_STORAGE,
                TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN,
            )?],
            items,
        )
    }
}

impl StreamingGetServiceProvider for PendingStreamingGetProvider {
    async fn on_get_stream(&self, _event: GetEvent) -> DcmResult<GetRetrievePlan> {
        let drop_signal = DropSignal(Arc::clone(&self.stream_dropped));
        let items: StreamingRetrieveItemStream =
            Box::pin(stream::unfold(drop_signal, |drop_signal| async move {
                let _drop_signal = drop_signal;
                std::future::pending::<Option<(DcmResult<RetrieveSubOperation>, DropSignal)>>()
                    .await
            }));
        Ok(GetRetrievePlan::new(3, items))
    }
}

#[tokio::test]
async fn late_bound_get_continues_after_an_unnegotiated_transfer_syntax() {
    let successful_instance_uids = ["1.2.826.0.1.1", "1.2.826.0.1.3"];
    let failed_instance_uid = "1.2.826.0.1.2";
    let server = DicomServer::builder()
        .ae_title("GETSCP")
        .port(0)
        .streaming_get_provider(FixedStreamingGetProvider {
            items: vec![
                StreamingRetrieveItem {
                    sop_class_uid: sop_class::CT_IMAGE_STORAGE.to_string(),
                    sop_instance_uid: successful_instance_uids[0].to_string(),
                    transfer_syntax_uid: TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
                    dataset: DatasetSource::bytes(encoded_instance(successful_instance_uids[0])),
                },
                StreamingRetrieveItem {
                    sop_class_uid: sop_class::CT_IMAGE_STORAGE.to_string(),
                    sop_instance_uid: failed_instance_uid.to_string(),
                    transfer_syntax_uid: TRANSFER_SYNTAX_IMPLICIT_VR_LITTLE_ENDIAN.to_string(),
                    dataset: DatasetSource::bytes(encoded_instance(failed_instance_uid)),
                },
                StreamingRetrieveItem {
                    sop_class_uid: sop_class::CT_IMAGE_STORAGE.to_string(),
                    sop_instance_uid: successful_instance_uids[1].to_string(),
                    transfer_syntax_uid: TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
                    dataset: DatasetSource::bytes(encoded_instance(successful_instance_uids[1])),
                },
            ],
        })
        .build()
        .await
        .expect("build server");
    let address = loopback_address(server.local_addr().expect("local address"));
    let cancellation_token = server.cancellation_token();
    let server_task = tokio::spawn(async move { server.run().await });

    let association_config = AssociationConfig::default();
    let mut association = Association::request_with_options(
        &address.to_string(),
        "GETSCP",
        "GETSCU",
        &[query_retrieve_get_context(1), storage_context(3)],
        &association_config,
        &requestor_options(),
    )
    .await
    .expect("associate");
    let query_context_id = association
        .find_context(sop_class::PATIENT_ROOT_QR_GET)
        .expect("query context")
        .id;

    let result = c_get(
        &mut association,
        GetRequest {
            sop_class_uid: sop_class::PATIENT_ROOT_QR_GET.to_string(),
            query: query_identifier(),
            context_id: query_context_id,
            priority: 0,
        },
    )
    .await
    .expect("C-GET");

    let received_instance_uids = result
        .instances
        .iter()
        .map(|instance| instance.sop_instance_uid.as_str())
        .collect::<Vec<_>>();
    assert_eq!(received_instance_uids, successful_instance_uids);
    let final_response = result.responses.last().expect("final response");
    assert_eq!(final_response.status, STATUS_WARNING);
    assert_eq!(final_response.completed, Some(2));
    assert_eq!(final_response.failed, Some(1));
    let failed_identifier = final_response.dataset.as_ref().expect("failed UID list");
    let failed_identifier = DicomReader::new(failed_identifier.as_slice())
        .read_dataset(TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN)
        .expect("decode failed UID list");
    assert_eq!(
        failed_identifier.get_string(Tag::new(0x0008, 0x0058)),
        Some(failed_instance_uid)
    );

    association.release().await.expect("release");
    cancellation_token.cancel();
    server_task.await.expect("server task").expect("server run");
}

#[tokio::test]
async fn matching_cancel_drops_a_pending_provider_stream_and_ignores_a_mismatch() {
    let stream_dropped = Arc::new(AtomicBool::new(false));
    let server = DicomServer::builder()
        .ae_title("GETSCP")
        .port(0)
        .streaming_get_provider(PendingStreamingGetProvider {
            stream_dropped: Arc::clone(&stream_dropped),
        })
        .build()
        .await
        .expect("build server");
    let address = loopback_address(server.local_addr().expect("local address"));
    let cancellation_token = server.cancellation_token();
    let server_task = tokio::spawn(async move { server.run().await });

    let association_config = AssociationConfig::default();
    let mut association = Association::request_with_options(
        &address.to_string(),
        "GETSCP",
        "GETSCU",
        &[query_retrieve_get_context(1), storage_context(3)],
        &association_config,
        &requestor_options(),
    )
    .await
    .expect("associate");
    let query_context_id = association
        .find_context(sop_class::PATIENT_ROOT_QR_GET)
        .expect("query context")
        .id;
    let retrieve_message_id = 41;
    association
        .send_dimse_command(query_context_id, &get_request_command(retrieve_message_id))
        .await
        .expect("send C-GET-RQ");
    association
        .send_dimse_data(query_context_id, &query_identifier())
        .await
        .expect("send identifier");
    association
        .send_dimse_command(query_context_id, &cancel_command(retrieve_message_id + 1))
        .await
        .expect("send mismatched C-CANCEL-RQ");
    association
        .send_dimse_command(query_context_id, &cancel_command(retrieve_message_id))
        .await
        .expect("send matching C-CANCEL-RQ");

    let (_, final_response) = association
        .recv_dimse_command()
        .await
        .expect("receive final C-GET-RSP");
    assert_eq!(final_response.get_u16(tags::COMMAND_FIELD), Some(0x8010));
    assert_eq!(final_response.get_u16(tags::STATUS), Some(STATUS_CANCEL));
    assert_eq!(
        final_response.get_u16(tags::NUMBER_OF_REMAINING_SUB_OPERATIONS),
        Some(3)
    );
    assert_eq!(
        final_response.get_u16(tags::NUMBER_OF_COMPLETED_SUB_OPERATIONS),
        Some(0)
    );
    assert!(stream_dropped.load(Ordering::SeqCst));
    if final_response.get_u16(tags::COMMAND_DATA_SET_TYPE) != Some(0x0101) {
        association
            .recv_dimse_data()
            .await
            .expect("receive cancel identifier");
    }

    association.release().await.expect("release");
    cancellation_token.cancel();
    server_task.await.expect("server task").expect("server run");
}

#[tokio::test]
async fn matching_move_cancel_drops_a_pending_provider_stream() {
    let destination_server = DicomServer::builder()
        .ae_title("DESTINATION")
        .port(0)
        .build()
        .await
        .expect("build destination server");
    let destination_address = loopback_address(
        destination_server
            .local_addr()
            .expect("destination address"),
    );
    let destination_cancellation_token = destination_server.cancellation_token();
    let destination_server_task = tokio::spawn(async move { destination_server.run().await });

    let stream_dropped = Arc::new(AtomicBool::new(false));
    let query_retrieve_server = DicomServer::builder()
        .ae_title("MOVESCP")
        .port(0)
        .streaming_move_provider(PendingStreamingMoveProvider {
            stream_dropped: Arc::clone(&stream_dropped),
        })
        .move_destination_lookup(StaticDestinationLookup::new(vec![(
            "DESTINATION".to_string(),
            destination_address.to_string(),
        )]))
        .build()
        .await
        .expect("build query/retrieve server");
    let query_retrieve_address = loopback_address(
        query_retrieve_server
            .local_addr()
            .expect("query/retrieve address"),
    );
    let query_retrieve_cancellation_token = query_retrieve_server.cancellation_token();
    let query_retrieve_server_task = tokio::spawn(async move { query_retrieve_server.run().await });

    let association_config = AssociationConfig::default();
    let mut association = Association::request(
        &query_retrieve_address.to_string(),
        "MOVESCP",
        "MOVESCU",
        &[query_retrieve_move_context(1)],
        &association_config,
    )
    .await
    .expect("associate");
    let query_context_id = association
        .find_context(sop_class::PATIENT_ROOT_QR_MOVE)
        .expect("query context")
        .id;
    let retrieve_message_id = 43;
    association
        .send_dimse_command(
            query_context_id,
            &move_request_command(retrieve_message_id, "DESTINATION"),
        )
        .await
        .expect("send C-MOVE-RQ");
    association
        .send_dimse_data(query_context_id, &query_identifier())
        .await
        .expect("send identifier");
    association
        .send_dimse_command(query_context_id, &cancel_command(retrieve_message_id))
        .await
        .expect("send C-CANCEL-RQ");

    let (_, final_response) = association
        .recv_dimse_command()
        .await
        .expect("receive final C-MOVE-RSP");
    assert_eq!(final_response.get_u16(tags::COMMAND_FIELD), Some(0x8021));
    assert_eq!(final_response.get_u16(tags::STATUS), Some(STATUS_CANCEL));
    assert_eq!(
        final_response.get_u16(tags::NUMBER_OF_REMAINING_SUB_OPERATIONS),
        Some(3)
    );
    assert!(stream_dropped.load(Ordering::SeqCst));
    if final_response.get_u16(tags::COMMAND_DATA_SET_TYPE) != Some(0x0101) {
        association
            .recv_dimse_data()
            .await
            .expect("receive cancel identifier");
    }

    association.release().await.expect("release");
    query_retrieve_cancellation_token.cancel();
    destination_cancellation_token.cancel();
    query_retrieve_server_task
        .await
        .expect("query/retrieve server task")
        .expect("query/retrieve server run");
    destination_server_task
        .await
        .expect("destination server task")
        .expect("destination server run");
}

#[tokio::test]
async fn cancel_during_a_large_file_stream_finishes_the_pdv_and_stops_early() {
    const DATASET_LENGTH: u64 = 32 * 1024 * 1024;
    let temporary_file = tempfile::NamedTempFile::new().expect("create temporary file");
    temporary_file
        .as_file()
        .set_len(DATASET_LENGTH)
        .expect("create sparse dataset");
    let sop_instance_uid = "1.2.826.0.1.4";
    let server = DicomServer::builder()
        .ae_title("GETSCP")
        .port(0)
        .streaming_get_provider(FixedStreamingGetProvider {
            items: vec![StreamingRetrieveItem {
                sop_class_uid: sop_class::CT_IMAGE_STORAGE.to_string(),
                sop_instance_uid: sop_instance_uid.to_string(),
                transfer_syntax_uid: TRANSFER_SYNTAX_EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
                dataset: DatasetSource::file_region(temporary_file.path(), 0, DATASET_LENGTH),
            }],
        })
        .build()
        .await
        .expect("build server");
    let address = loopback_address(server.local_addr().expect("local address"));
    let cancellation_token = server.cancellation_token();
    let server_task = tokio::spawn(async move { server.run().await });

    let association_config = AssociationConfig::default();
    let mut association = Association::request_with_options(
        &address.to_string(),
        "GETSCP",
        "GETSCU",
        &[query_retrieve_get_context(1), storage_context(3)],
        &association_config,
        &requestor_options(),
    )
    .await
    .expect("associate");
    let query_context_id = association
        .find_context(sop_class::PATIENT_ROOT_QR_GET)
        .expect("query context")
        .id;
    let retrieve_message_id = 42;
    association
        .send_dimse_command(query_context_id, &get_request_command(retrieve_message_id))
        .await
        .expect("send C-GET-RQ");
    association
        .send_dimse_data(query_context_id, &query_identifier())
        .await
        .expect("send identifier");

    let (_, store_request) = association
        .recv_dimse_command()
        .await
        .expect("receive C-STORE-RQ");
    assert_eq!(store_request.get_u16(tags::COMMAND_FIELD), Some(0x0001));
    association
        .send_dimse_command(query_context_id, &cancel_command(retrieve_message_id))
        .await
        .expect("send C-CANCEL-RQ");
    let partial_dataset = association
        .recv_dimse_data()
        .await
        .expect("receive syntactically complete partial dataset stream");
    assert!(
        partial_dataset.len() < DATASET_LENGTH as usize,
        "cancellation must stop the current dataset before reading the complete file"
    );

    let (_, final_response) = association
        .recv_dimse_command()
        .await
        .expect("receive final C-GET-RSP");
    assert_eq!(final_response.get_u16(tags::STATUS), Some(STATUS_CANCEL));
    assert_eq!(
        final_response.get_u16(tags::NUMBER_OF_FAILED_SUB_OPERATIONS),
        Some(1)
    );
    if final_response.get_u16(tags::COMMAND_DATA_SET_TYPE) != Some(0x0101) {
        association
            .recv_dimse_data()
            .await
            .expect("receive failed UID list");
    }

    association.release().await.expect("release");
    cancellation_token.cancel();
    server_task.await.expect("server task").expect("server run");
}
