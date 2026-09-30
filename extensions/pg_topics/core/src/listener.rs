use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    AlterConfigsRequest, ApiKey, CreateTopicsRequest, DeleteGroupsRequest, DeleteTopicsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest, FetchRequest,
    FindCoordinatorRequest, HeartbeatRequest, IncrementalAlterConfigsRequest,
    InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
    ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, SyncGroupRequest,
};
use kafka_protocol::protocol::{decode_request_header_from_buffer, Decodable, Encodable, StrBytes};
use kafka_protocol::ResponseError;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use crate::versions::supported;
use crate::{admin, groups, handlers};

const MAX_PENDING: usize = 64;
const MAX_PENDING_PER_HOST: usize = 16;
const MAX_DEFERRED: usize = 5;
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_SLACK: usize = 64 * 1024;
const PREAUTH_FRAME_MAX: usize = 64 * 1024;
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

pub struct Config {
    pub port: u16,
    pub pg_port: u16,
    pub database: String,
    pub advertised_host: String,
    pub max_clients: usize,
    pub max_message_bytes: usize,
    pub tls: Arc<ServerConfig>,
}

pub fn tls_config(cert_file: &str, key_file: &str) -> Result<Arc<ServerConfig>, String> {
    let certs = CertificateDer::pem_file_iter(cert_file)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("{cert_file}: {e}"))?;
    let key = PrivateKeyDer::from_pem_file(key_file).map_err(|e| format!("{key_file}: {e}"))?;
    ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .and_then(|b| b.with_no_client_auth().with_single_cert(certs, key))
        .map(Arc::new)
        .map_err(|e| e.to_string())
}

struct Shared {
    cfg: Config,
    pending: AtomicUsize,
    clients: AtomicUsize,
    hosts: Hosts,
}

struct Slot(Arc<Shared>, bool);

impl Slot {
    fn take(shared: &Arc<Shared>, client: bool, max: usize) -> Option<Slot> {
        let slot = Slot(shared.clone(), client);
        (slot.counter().fetch_add(1, Ordering::SeqCst) < max).then_some(slot)
    }

    fn counter(&self) -> &AtomicUsize {
        if self.1 {
            &self.0.clients
        } else {
            &self.0.pending
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.counter().fetch_sub(1, Ordering::SeqCst);
    }
}

type Hosts = Arc<Mutex<HashMap<IpAddr, usize>>>;

struct HostSlot(Hosts, IpAddr);

impl HostSlot {
    fn take(hosts: &Hosts, host: IpAddr) -> Option<HostSlot> {
        let mut counts = hosts.lock().unwrap_or_else(PoisonError::into_inner);
        let n = counts.entry(host).or_insert(0);
        (*n < MAX_PENDING_PER_HOST).then(|| {
            *n += 1;
            HostSlot(hosts.clone(), host)
        })
    }
}

impl Drop for HostSlot {
    fn drop(&mut self) {
        let mut counts = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = counts.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                counts.remove(&self.1);
            }
        }
    }
}

pub fn frame_len(prefix: [u8; 4], limit: usize) -> io::Result<usize> {
    let len = i32::from_be_bytes(prefix);
    usize::try_from(len)
        .ok()
        .filter(|&n| n <= limit)
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("a frame of {len} bytes is outside the limit of {limit} bytes"),
            )
        })
}

fn read_frame(stream: &mut impl Read, limit: usize) -> io::Result<Option<Bytes>> {
    let mut prefix = [0; 4];
    if let Err(e) = stream.read_exact(&mut prefix) {
        return match e.kind() {
            ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(e),
        };
    }
    let mut frame = vec![0; frame_len(prefix, limit)?];
    stream.read_exact(&mut frame)?;
    Ok(Some(frame.into()))
}

struct Deadlined {
    tcp: TcpStream,
    deadline: Option<Instant>,
}

impl Deadlined {
    fn left(&self) -> io::Result<Option<Duration>> {
        let Some(deadline) = self.deadline else {
            return Ok(Some(IDLE_TIMEOUT));
        };
        match deadline.saturating_duration_since(Instant::now()) {
            left if left.is_zero() => Err(io::Error::new(
                ErrorKind::TimedOut,
                "the client did not authenticate in 10 s",
            )),
            left => Ok(Some(left)),
        }
    }
}

impl Read for Deadlined {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.tcp.set_read_timeout(self.left()?)?;
        self.tcp.read(buf)
    }
}

impl Write for Deadlined {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tcp.set_write_timeout(self.left()?)?;
        self.tcp.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tcp.flush()
    }
}

fn response<R: Encodable>(
    correlation_id: i32,
    key: ApiKey,
    version: i16,
    body: &R,
) -> anyhow::Result<BytesMut> {
    let mut out = BytesMut::new();
    out.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut out, key.response_header_version(version))?;
    body.encode(&mut out, version)?;
    let len = i32::try_from(out.len() - 4)?;
    out[..4].copy_from_slice(&len.to_be_bytes());
    Ok(out)
}

fn tune(tcp: &TcpStream) -> io::Result<()> {
    tcp.set_nodelay(true)?;
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10));
    socket2::SockRef::from(tcp).set_tcp_keepalive(&keepalive)
}

type Deferred = VecDeque<(i32, i16, handlers::Produced)>;

fn ready(stream: &mut StreamOwned<ServerConnection, Deadlined>) -> io::Result<bool> {
    let state = stream
        .conn
        .process_new_packets()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
    if state.plaintext_bytes_to_read() > 0 {
        return Ok(true);
    }
    stream.sock.tcp.set_nonblocking(true)?;
    let peeked = stream.sock.tcp.peek(&mut [0; 1]);
    stream.sock.tcp.set_nonblocking(false)?;
    match peeked {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(e) => Err(e),
    }
}

fn answer(
    stream: &mut impl Write,
    db: &mut postgres::Client,
    deferred: &mut Deferred,
) -> anyhow::Result<()> {
    if let Some((id, version, produced)) = deferred.pop_front() {
        let body = handlers::finish_produce(db, produced)?;
        stream.write_all(&response(id, ApiKey::Produce, version, &body)?)?;
        stream.flush()?;
    }
    Ok(())
}

fn session(shared: &Arc<Shared>, pending: (Slot, HostSlot), tcp: TcpStream) -> anyhow::Result<()> {
    let cfg = &shared.cfg;
    tune(&tcp)?;
    let sock = Deadlined {
        tcp,
        deadline: Some(Instant::now() + AUTH_TIMEOUT),
    };
    let mut stream = StreamOwned::new(ServerConnection::new(cfg.tls.clone())?, sock);
    let mut pending = Some(pending);
    let mut handshake = false;
    let mut db = None;
    let mut deferred = Deferred::new();
    loop {
        if let Some((client, _)) = db.as_mut() {
            while let Some((_, _, produced)) = deferred.front_mut() {
                if !handlers::produce_ready(client, produced)? {
                    break;
                }
                answer(&mut stream, client, &mut deferred)?;
            }
            while deferred.len() >= MAX_DEFERRED || (!deferred.is_empty() && !ready(&mut stream)?) {
                answer(&mut stream, client, &mut deferred)?;
            }
        }
        let limit = match db.is_some() {
            true => cfg.max_message_bytes + FRAME_SLACK,
            false => PREAUTH_FRAME_MAX,
        };
        let Some(mut frame) = read_frame(&mut stream, limit)? else {
            return Ok(());
        };
        if frame.len() < 8 {
            bail!("a request of {} bytes is too short", frame.len());
        }
        let header = decode_request_header_from_buffer(&mut frame)?;
        let key = ApiKey::try_from(header.request_api_key)
            .map_err(|_| anyhow!("API key {} is not known", header.request_api_key))?;
        let version = header.request_api_version;
        let id = header.correlation_id;
        if let (true, Some((client, _))) = (key != ApiKey::Produce, db.as_mut()) {
            while !deferred.is_empty() {
                answer(&mut stream, client, &mut deferred)?;
            }
        }
        let mut close = None;
        let out = match (key, db.as_mut()) {
            (ApiKey::ApiVersions, _) => match supported(key, version) {
                true => Some(response(id, key, version, &handlers::api_versions(0))?),
                false => Some(response(
                    id,
                    key,
                    0,
                    &handlers::api_versions(ResponseError::UnsupportedVersion.code()),
                )?),
            },
            _ if !supported(key, version) => bail!("{key:?} v{version} is not supported"),
            (ApiKey::SaslHandshake, None) => {
                let req = SaslHandshakeRequest::decode(&mut frame, version)?;
                handshake = req.mechanism.as_str() == "PLAIN";
                if !handshake {
                    close = Some(format!(
                        "SASL mechanism {} is not supported",
                        req.mechanism.as_str()
                    ));
                }
                let body = SaslHandshakeResponse::default()
                    .with_error_code(match handshake {
                        true => 0,
                        false => ResponseError::UnsupportedSaslMechanism.code(),
                    })
                    .with_mechanisms(vec![StrBytes::from_static_str("PLAIN")]);
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::SaslAuthenticate, None) if handshake => {
                let req = SaslAuthenticateRequest::decode(&mut frame, version)?;
                let result = match Slot::take(shared, true, cfg.max_clients) {
                    None => Err("the listener has max_clients authenticated clients".to_string()),
                    Some(slot) => {
                        handlers::authenticate(cfg.pg_port, &cfg.database, &req.auth_bytes)
                            .map(|client| (client, slot))
                    }
                };
                let body = match result {
                    Ok(authed) => {
                        db = Some(authed);
                        pending.take();
                        stream.sock.deadline = None;
                        SaslAuthenticateResponse::default()
                    }
                    Err(reason) => {
                        close = Some(format!("authentication failed: {reason}"));
                        SaslAuthenticateResponse::default()
                            .with_error_code(ResponseError::SaslAuthenticationFailed.code())
                            .with_error_message(Some(StrBytes::from_string(format!(
                                "authentication failed: {reason}"
                            ))))
                    }
                };
                Some(response(id, key, version, &body)?)
            }
            (_, None) => bail!("{key:?} came before authentication"),
            (ApiKey::Metadata, Some((client, _))) => {
                let req = MetadataRequest::decode(&mut frame, version)?;
                let body =
                    handlers::metadata(client, &cfg.advertised_host, cfg.port, &req, version)?;
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::Produce, Some((client, _))) => {
                let req = ProduceRequest::decode(&mut frame, version)?;
                let produced = handlers::produce(client, &req, cfg.max_message_bytes)?;
                if req.acks != 0 {
                    deferred.push_back((id, version, produced));
                }
                None
            }
            (ApiKey::InitProducerId, Some((client, _))) => {
                let req = InitProducerIdRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &handlers::init_producer_id(client, &req)?,
                )?)
            }
            (ApiKey::Fetch, Some((client, _))) => {
                let req = FetchRequest::decode(&mut frame, version)?;
                Some(response(id, key, version, &handlers::fetch(client, &req)?)?)
            }
            (ApiKey::ListOffsets, Some((client, _))) => {
                let req = ListOffsetsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &handlers::list_offsets(client, &req)?,
                )?)
            }
            (ApiKey::FindCoordinator, Some(_)) => {
                let req = FindCoordinatorRequest::decode(&mut frame, version)?;
                let body = groups::find_coordinator(&req, version, &cfg.advertised_host, cfg.port);
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::JoinGroup, Some((client, _))) => {
                let req = JoinGroupRequest::decode(&mut frame, version)?;
                let client_id = header.client_id.as_ref().map_or("", |c| c.as_str());
                let body = groups::join_group(client, &req, version, client_id)?;
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::SyncGroup, Some((client, _))) => {
                let req = SyncGroupRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &groups::sync_group(client, &req)?,
                )?)
            }
            (ApiKey::Heartbeat, Some((client, _))) => {
                let req = HeartbeatRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &groups::heartbeat(client, &req)?,
                )?)
            }
            (ApiKey::LeaveGroup, Some((client, _))) => {
                let req = LeaveGroupRequest::decode(&mut frame, version)?;
                let body = groups::leave_group(client, &req, version)?;
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::OffsetCommit, Some((client, _))) => {
                let req = OffsetCommitRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &groups::offset_commit(client, &req)?,
                )?)
            }
            (ApiKey::OffsetFetch, Some((client, _))) => {
                let req = OffsetFetchRequest::decode(&mut frame, version)?;
                let body = groups::offset_fetch(client, &req, version)?;
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::CreateTopics, Some((client, _))) => {
                let req = CreateTopicsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::create_topics(client, &req)?,
                )?)
            }
            (ApiKey::DeleteTopics, Some((client, _))) => {
                let req = DeleteTopicsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::delete_topics(client, &req)?,
                )?)
            }
            (ApiKey::DescribeConfigs, Some((client, _))) => {
                let req = DescribeConfigsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::describe_configs(client, &req)?,
                )?)
            }
            (ApiKey::AlterConfigs, Some((client, _))) => {
                let req = AlterConfigsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::alter_configs(client, &req)?,
                )?)
            }
            (ApiKey::IncrementalAlterConfigs, Some((client, _))) => {
                let req = IncrementalAlterConfigsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::incremental_alter_configs(client, &req)?,
                )?)
            }
            (ApiKey::DeleteGroups, Some((client, _))) => {
                let req = DeleteGroupsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::delete_groups(client, &req)?,
                )?)
            }
            (ApiKey::DescribeCluster, Some((client, _))) => {
                DescribeClusterRequest::decode(&mut frame, version)?;
                let body = admin::describe_cluster(client, &cfg.advertised_host, cfg.port)?;
                Some(response(id, key, version, &body)?)
            }
            (ApiKey::ListGroups, Some((client, _))) => {
                let req = ListGroupsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::list_groups(client, &req)?,
                )?)
            }
            (ApiKey::DescribeGroups, Some((client, _))) => {
                let req = DescribeGroupsRequest::decode(&mut frame, version)?;
                Some(response(
                    id,
                    key,
                    version,
                    &admin::describe_groups(client, &req)?,
                )?)
            }
            (_, Some(_)) => bail!("{key:?} came after authentication"),
        };
        if let Some(out) = out {
            stream.write_all(&out)?;
            stream.flush()?;
        }
        if let Some(reason) = close {
            bail!(reason);
        }
    }
}

pub fn run(listener: TcpListener, cfg: Config) {
    let shared = Arc::new(Shared {
        cfg,
        pending: AtomicUsize::new(0),
        clients: AtomicUsize::new(0),
        hosts: Hosts::default(),
    });
    let mut refused_at: Option<Instant> = None;
    for tcp in listener.incoming() {
        let tcp = match tcp {
            Ok(tcp) => tcp,
            Err(e) => {
                eprintln!("pg_topics listener: accept failed: {e}");
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        let host = match tcp.peer_addr() {
            Ok(addr) => addr.ip(),
            Err(e) => {
                eprintln!("pg_topics listener: a new connection has no peer address: {e}");
                continue;
            }
        };
        let pending = HostSlot::take(&shared.hosts, host)
            .and_then(|h| Some((Slot::take(&shared, false, MAX_PENDING)?, h)));
        let Some(pending) = pending else {
            if refused_at.is_none_or(|at| at.elapsed() >= Duration::from_secs(10)) {
                eprintln!(
                    "pg_topics listener: refused a connection from {host}: too many connections from it, or {MAX_PENDING} in all, have not authenticated yet"
                );
                refused_at = Some(Instant::now());
            }
            continue;
        };
        let conn = shared.clone();
        let spawned = thread::Builder::new()
            .name("pg_topics client".into())
            .spawn(move || {
                if let Err(e) = session(&conn, pending, tcp) {
                    eprintln!("pg_topics listener: a client connection closed: {e:#}");
                }
            });
        if let Err(e) = spawned {
            eprintln!("pg_topics listener: cannot start a client thread: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_len_refuses_a_length_above_the_limit() {
        assert_eq!(frame_len(100i32.to_be_bytes(), 100).unwrap(), 100);
        assert!(frame_len(101i32.to_be_bytes(), 100).is_err());
        assert!(frame_len((-1i32).to_be_bytes(), 100).is_err());
        assert!(frame_len(i32::MAX.to_be_bytes(), 1 << 20).is_err());
    }

    #[test]
    fn read_frame_refuses_before_it_reads_the_body() {
        let mut wire: &[u8] = &[0x7f, 0xff, 0xff, 0xff, 1, 2, 3];
        let err = read_frame(&mut wire, 1024).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(wire, &[1, 2, 3]);
    }

    #[test]
    fn a_passed_deadline_fails_every_read_and_write_at_once() {
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (tcp, _) = server.accept().unwrap();
        let mut sock = Deadlined {
            tcp,
            deadline: Some(Instant::now()),
        };
        assert_eq!(
            sock.read(&mut [0; 1]).unwrap_err().kind(),
            ErrorKind::TimedOut
        );
        assert_eq!(sock.write(b"x").unwrap_err().kind(), ErrorKind::TimedOut);
        drop(client);
    }

    #[test]
    fn after_auth_the_socket_has_an_idle_limit_and_keepalive() {
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(server.local_addr().unwrap()).unwrap();
        let (tcp, _) = server.accept().unwrap();
        tune(&tcp).unwrap();
        assert!(socket2::SockRef::from(&tcp).keepalive().unwrap());
        let sock = Deadlined {
            tcp,
            deadline: None,
        };
        assert_eq!(sock.left().unwrap(), Some(IDLE_TIMEOUT));
        assert_eq!(IDLE_TIMEOUT, Duration::from_secs(600));
    }

    #[test]
    fn one_host_gets_at_most_its_share_of_pending_slots() {
        let hosts = Hosts::default();
        let (a, b) = (IpAddr::from([10, 0, 0, 1]), IpAddr::from([10, 0, 0, 2]));
        let taken: Vec<_> = (0..MAX_PENDING_PER_HOST)
            .map(|_| HostSlot::take(&hosts, a).unwrap())
            .collect();
        assert!(HostSlot::take(&hosts, a).is_none());
        assert!(HostSlot::take(&hosts, b).is_some());
        drop(taken);
        assert!(HostSlot::take(&hosts, a).is_some());
        assert!(hosts.lock().unwrap().is_empty());
    }

    #[test]
    fn a_huge_array_count_is_a_decode_error() {
        let mut body =
            Bytes::from_static(&[0xff, 0xff, 0, 1, 0, 0, 0x75, 0x30, 0x7f, 0xff, 0xff, 0xff]);
        assert!(ProduceRequest::decode(&mut body, 3).is_err());
    }
}
