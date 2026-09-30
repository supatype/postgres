use std::cell::Cell;
use std::io::Read;

use anyhow::{bail, Context};
use bytes::{Buf, Bytes};
use kafka_protocol::compression::{Decompressor, Snappy};
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder};

const SNAPPY_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

#[derive(Debug)]
pub enum DecodeError {
    TooLarge,
    Corrupt(anyhow::Error),
}

pub struct Budget {
    remaining: Cell<usize>,
    exhausted: Cell<bool>,
}

impl Budget {
    pub fn new(total: usize) -> Self {
        Budget {
            remaining: Cell::new(total),
            exhausted: Cell::new(false),
        }
    }

    fn charge(&self, used: usize) -> anyhow::Result<()> {
        match self.remaining.get().checked_sub(used) {
            Some(rest) => {
                self.remaining.set(rest);
                Ok(())
            }
            None => {
                self.exhausted.set(true);
                bail!("the request expands past its decompression budget")
            }
        }
    }
}

pub fn finish_fetch_batch(batch: &mut [u8]) {
    batch[22] |= 0x08;
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes());
    let crc = crc32c::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
}

fn snappy_len(data: &[u8]) -> anyhow::Result<usize> {
    let Some(mut rest) = data.strip_prefix(SNAPPY_MAGIC) else {
        return Ok(snap::raw::decompress_len(data)?);
    };
    let mut total = 0usize;
    while !rest.is_empty() {
        let (len, tail) = rest
            .split_at_checked(4)
            .context("short snappy block length")?;
        let len = u32::from_be_bytes(len.try_into()?) as usize;
        let (block, tail) = tail.split_at_checked(len).context("short snappy block")?;
        total = total.saturating_add(snap::raw::decompress_len(block)?);
        rest = tail;
    }
    Ok(total)
}

fn inflate(data: &mut Bytes, compression: Compression, budget: &Budget) -> anyhow::Result<Bytes> {
    let reader: Box<dyn Read + '_> = match compression {
        Compression::None => {
            let out = data.split_to(data.remaining());
            budget.charge(out.len())?;
            return Ok(out);
        }
        Compression::Snappy => {
            budget.charge(snappy_len(data)?)?;
            return Snappy::decompress(data, |out: &mut Bytes| Ok(out.split_to(out.len())));
        }
        Compression::Gzip => Box::new(flate2::read::GzDecoder::new(&data[..])),
        Compression::Lz4 => Box::new(lz4::Decoder::new(&data[..])?),
        Compression::Zstd => Box::new(zstd::stream::read::Decoder::new(&data[..])?),
    };
    let cap = budget.remaining.get();
    let mut out = Vec::new();
    reader.take(cap as u64 + 1).read_to_end(&mut out)?;
    budget.charge(out.len())?;
    Ok(out.into())
}

pub fn decode_produce(mut records: Bytes, budget: &Budget) -> Result<Vec<Record>, DecodeError> {
    let mut out = Vec::new();
    while records.has_remaining() {
        let set = RecordBatchDecoder::decode_with_custom_compression(
            &mut records,
            Some(|data: &mut Bytes, compression| inflate(data, compression, budget)),
        )
        .map_err(|e| match budget.exhausted.get() {
            true => DecodeError::TooLarge,
            false => DecodeError::Corrupt(e),
        })?;
        out.extend(set.records);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::records::{RecordBatchEncoder, RecordEncodeOptions, TimestampType};

    fn record(offset: i64) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::LogAppend,
            offset,
            sequence: offset as i32,
            timestamp: 1_700_000_000_000 + offset,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"{\"a\": 1}")),
            headers: Vec::new(),
        }
    }

    fn encode(records: &[Record], compression: Compression) -> BytesMut {
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        buf
    }

    #[test]
    fn finish_fetch_batch_sets_log_append_time_and_a_valid_crc() {
        let mut buf = encode(&[record(7), record(8), record(9)], Compression::None);
        finish_fetch_batch(&mut buf);
        let info = RecordBatchDecoder::decode_batch_info(&mut buf.clone().freeze()).unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].timestamp_type, TimestampType::LogAppend);
        assert_eq!(info[0].base_sequence, -1);
        assert_eq!(info[0].min_offset, 7);
        let set = RecordBatchDecoder::decode(&mut buf.freeze()).unwrap();
        assert_eq!(set.records.len(), 3);
        assert_eq!(set.records[2].offset, 9);
    }

    #[test]
    fn finish_fetch_batch_crc_covers_the_attributes() {
        let mut buf = encode(&[record(0)], Compression::None);
        finish_fetch_batch(&mut buf);
        buf[22] ^= 0x08;
        let err = RecordBatchDecoder::decode(&mut buf.freeze()).unwrap_err();
        assert!(err.to_string().contains("Cyclic redundancy check failed"));
    }

    fn too_large(e: DecodeError) -> bool {
        matches!(e, DecodeError::TooLarge)
    }

    #[test]
    fn decode_produce_reads_every_codec() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let buf = encode(&[record(0), record(1)], compression);
            let records = decode_produce(buf.freeze(), &Budget::new(1 << 20)).unwrap();
            assert_eq!(records.len(), 2, "{compression:?}");
            assert_eq!(records[1].value.as_deref(), Some(&b"{\"a\": 1}"[..]));
        }
    }

    #[test]
    fn decode_produce_keeps_every_header_in_order() {
        let mut r = record(0);
        r.headers = [
            ("a".into(), Some(Bytes::from_static(b"1"))),
            ("a".into(), Some(Bytes::from_static(b"2"))),
            ("bin".into(), Some(Bytes::from_static(b"\xff\x00\x01"))),
            ("n".into(), None),
            ("e".into(), Some(Bytes::new())),
        ]
        .into();
        let buf = encode(&[r], Compression::None);
        let records = decode_produce(buf.freeze(), &Budget::new(1 << 20)).unwrap();
        let headers: Vec<(&str, Option<&[u8]>)> = records[0]
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_deref()))
            .collect();
        assert_eq!(
            headers,
            vec![
                ("a", Some(&b"1"[..])),
                ("a", Some(&b"2"[..])),
                ("bin", Some(&b"\xff\x00\x01"[..])),
                ("n", None),
                ("e", Some(&b""[..])),
            ]
        );
    }

    #[test]
    fn decode_produce_refuses_a_batch_that_expands_past_the_budget() {
        let mut big = record(0);
        big.value = Some(Bytes::from(vec![b'x'; 100_000]));
        for compression in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let buf = encode(std::slice::from_ref(&big), compression);
            let err = decode_produce(buf.freeze(), &Budget::new(10_000)).unwrap_err();
            assert!(too_large(err), "{compression:?}");
        }
    }

    #[test]
    fn decode_produce_shares_one_budget_over_many_small_bomb_batches() {
        let mut big = record(0);
        big.value = Some(Bytes::from(vec![b'x'; 100_000]));
        let one = encode(std::slice::from_ref(&big), Compression::Zstd);
        let mut request = BytesMut::new();
        for _ in 0..500 {
            request.extend_from_slice(&one);
        }
        assert!(request.len() < 1 << 20);
        let budget = Budget::new(16 * (1 << 20));
        let err = decode_produce(request.freeze(), &budget).unwrap_err();
        assert!(too_large(err));
    }

    #[test]
    fn decode_produce_reports_a_corrupt_batch_apart_from_a_budget_overrun() {
        let mut buf = encode(&[record(0)], Compression::None);
        buf[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
        let crc = crc32c::crc32c(&buf[21..]);
        buf[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            decode_produce(buf.freeze(), &Budget::new(1 << 20)),
            Err(DecodeError::Corrupt(_))
        ));
    }
}
