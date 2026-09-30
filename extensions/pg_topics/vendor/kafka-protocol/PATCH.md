# Patch against kafka-protocol 0.18.0

This copy is the crates.io release `kafka-protocol` 0.18.0 with the patch below. The patch does three things:

1. `types.rs` caps the first allocation of an array at the bytes left in the buffer. A malformed length can then not make the decoder allocate gigabytes.
2. `records.rs` keeps record headers as a list, in order. Upstream keeps them in a map, which drops repeated names.
3. `Cargo.toml` drops the upstream test target and its `testcontainers` dependency. `snappy.rs` changes its tests to the new header type.

To check the copy, unpack the crate from crates.io and run the diff command below. The output must equal this patch.

```sh
diff -ruN -x target -x .github -x .gitignore -x AUTHORS -x CHANGELOG.md -x rust-toolchain.toml \
  -x Cargo.toml.orig -x .cargo_vcs_info.json -x .cargo-ok -x Cargo.lock -x tests -x benches -x examples \
  -x PATCH.md kafka-protocol-0.18.0 vendor/kafka-protocol
```

```diff
diff kafka-protocol-0.18.0/Cargo.toml vendor/kafka-protocol/Cargo.toml
--- kafka-protocol-0.18.0/Cargo.toml
+++ vendor/kafka-protocol/Cargo.toml
@@ -60,9 +60,6 @@
 name = "kafka_protocol"
 path = "src/lib.rs"
 
-[[test]]
-name = "tests"
-path = "tests/tests.rs"
 
 [dependencies.anyhow]
 version = "1.0.80"
@@ -97,10 +94,3 @@
 [dependencies.zstd]
 version = "0.13"
 optional = true
-
-[dev-dependencies.testcontainers]
-version = "0.28.0"
-features = [
-    "blocking",
-    "watchdog",
-]
diff kafka-protocol-0.18.0/src/compression/snappy.rs vendor/kafka-protocol/src/compression/snappy.rs
--- kafka-protocol-0.18.0/src/compression/snappy.rs
+++ vendor/kafka-protocol/src/compression/snappy.rs
@@ -125,7 +125,6 @@
 #[cfg(test)]
 mod tests {
     use bytes::{Buf as _, Bytes, BytesMut};
-    use indexmap::IndexMap;
 
     use crate::records::{
         Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
@@ -149,7 +148,7 @@
             timestamp: Default::default(),
             key: None,
             value: Some(Bytes::from_static(b"sdfdsf")),
-            headers: IndexMap::default(),
+            headers: Vec::new(),
         };
 
         // The module doesn't expose record encode/decode directly so we have to put everything
@@ -212,7 +211,7 @@
             timestamp: Default::default(),
             key: None,
             value: Some(Bytes::from_static(b"sdfdsf")),
-            headers: IndexMap::default(),
+            headers: Vec::new(),
         };
         RecordBatchEncoder::encode(
             &mut expected_bytes,
@@ -257,7 +256,7 @@
             timestamp: Default::default(),
             key: None,
             value: Some(Bytes::from_static(b"sdfdsf")),
-            headers: IndexMap::default(),
+            headers: Vec::new(),
         };
         RecordBatchEncoder::encode(
             &mut expected_bytes,
diff kafka-protocol-0.18.0/src/protocol/types.rs vendor/kafka-protocol/src/protocol/types.rs
--- kafka-protocol-0.18.0/src/protocol/types.rs
+++ vendor/kafka-protocol/src/protocol/types.rs
@@ -985,7 +985,7 @@
         match Int32.decode(buf)? {
             -1 => Ok(None),
             n if n >= 0 => {
-                let mut result = Vec::with_capacity(n as usize);
+                let mut result = Vec::with_capacity((n as usize).min(bytes::Buf::remaining(buf)));
                 for _ in 0..n {
                     result.push(self.0.decode(buf)?);
                 }
@@ -1093,7 +1093,7 @@
         match UnsignedVarInt.decode(buf)? {
             0 => Ok(None),
             n => {
-                let mut result = Vec::with_capacity((n - 1) as usize);
+                let mut result = Vec::with_capacity(((n - 1) as usize).min(bytes::Buf::remaining(buf)));
                 for _ in 1..n {
                     result.push(self.0.decode(buf)?);
                 }
diff kafka-protocol-0.18.0/src/records.rs vendor/kafka-protocol/src/records.rs
--- kafka-protocol-0.18.0/src/records.rs
+++ vendor/kafka-protocol/src/records.rs
@@ -42,7 +42,6 @@
 use bytes::{Bytes, BytesMut};
 use crc::{Crc, CRC_32_ISO_HDLC};
 use crc32c::crc32c;
-use indexmap::IndexMap;
 
 use crate::protocol::{
     buf::{gap, ByteBuf, ByteBufMut},
@@ -181,7 +180,7 @@
     /// The payload of the record.
     pub value: Option<Bytes>,
     /// Headers associated with the record's payload.
-    pub headers: IndexMap<StrBytes, Option<Bytes>>,
+    pub headers: Vec<(StrBytes, Option<Bytes>)>,
 }
 
 const MAGIC_BYTE_OFFSET: usize = 16;
@@ -514,7 +513,7 @@
         version: i8,
         records: &mut Vec<Record>,
     ) -> Result<()> {
-        records.reserve(batch_decode_info.record_count as usize);
+        records.reserve((batch_decode_info.record_count as usize).min(bytes::Buf::remaining(buf)));
         for _ in 0..batch_decode_info.record_count {
             records.push(Record::decode_new(buf, batch_decode_info, version)?);
         }
@@ -893,7 +892,7 @@
         }
         let num_headers = num_headers as usize;
 
-        let mut headers = IndexMap::with_capacity(num_headers);
+        let mut headers = Vec::with_capacity(num_headers.min(bytes::Buf::remaining(buf)));
         for _ in 0..num_headers {
             // Key len
             let key_len: i32 = types::VarInt.decode(buf)?;
@@ -916,7 +915,7 @@
                 Ordering::Greater => Some(buf.try_get_bytes(value_len as usize)?),
             };
 
-            headers.insert(key, value);
+            headers.push((key, value));
         }
 
         Ok(Self {
@@ -1029,8 +1028,8 @@
             Bytes::from("some-value"),
             record
                 .headers
-                // This relies on `impl Borrow<[u8]> for StrBytes`
-                .get("some-key".as_bytes())
+                .iter()
+                .find_map(|(k, v)| (k.as_bytes() == b"some-key").then_some(v))
                 .expect("key exists in headers")
                 .as_ref()
                 .expect("value is present")
```
