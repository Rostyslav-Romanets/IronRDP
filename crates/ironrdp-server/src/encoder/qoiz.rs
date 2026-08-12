//! Zstd compression for the QOIZ bitmap codec.
//!
//! Two backends are available:
//!
//! - `zrip`, a pure Rust zstd implementation, used by default.
//! - `zstd-safe`, bindings to the reference C implementation, used when the
//!   `qoiz-zstd` feature is enabled.
//!
//! Both emit standard zstd frames, so the choice of backend is invisible on the
//! wire and either side of a connection may use either one.

// The `zstd` and `zrip` backends use nearly identical parameter sets at the compression level 3.
const COMPRESSION_LEVEL: i32 = 3;

// Compiled event when the `qoiz-zstd` feature is enabled, so tests can use both backends.
#[cfg_attr(all(feature = "qoiz-zstd", not(feature = "__test")), allow(dead_code))]
pub(crate) mod zrip_backend {
    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    use crate::{ServerError, ServerErrorExt as _, ServerResult};
    use crate::encoder::qoiz::COMPRESSION_LEVEL;

    /// Sink handing the encoder's output back to the caller.
    ///
    /// [`zrip::FrameEncoder`] owns its writer and exposes no accessor for it, so
    /// the buffer is shared rather than borrowed back.
    #[derive(Clone, Debug, Default)]
    struct SharedSink(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut sink = self.0.lock().map_err(|_| std::io::Error::other("poisoned sink"))?;
            sink.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Streaming compressor for the QOIZ codec.
    pub struct Compressor {
        encoder: zrip::FrameEncoder<SharedSink>,
        sink: SharedSink,
    }

    impl Compressor {
        pub fn new() -> ServerResult<Self> {
            // Long distance matching is what lets an update match against
            // earlier ones. Without it, repeated content is re-emitted in full.
            let options = zrip::Options::default().ldm(true);
            let sink = SharedSink::default();
            let encoder = zrip::FrameEncoder::with_options(sink.clone(), COMPRESSION_LEVEL, &options)
                .map_err(|error| ServerError::reason("qoiz init", format!("failed to create zstd encoder: {error}")))?;

            Ok(Self { encoder, sink })
        }

        /// Compresses `input` and returns the bytes produced for this update.
        pub fn compress(&mut self, input: &[u8]) -> ServerResult<Vec<u8>> {
            self.encoder.write_all(input).map_err(|error| ServerError::reason(
                "qoiz compress",
            format!("failed to write input data to the encoder: {error}")))?;
            self.encoder.flush().map_err(|error| ServerError::reason("qoiz compress", format!("failed to flush the encoder: {error}")))?;

            let mut sink = self
                .sink
                .0
                .lock()
                .map_err(|_| ServerError::reason("qoiz compress", "poisoned sink"))?;

            Ok(core::mem::take(&mut *sink))
        }
    }

    impl core::fmt::Debug for Compressor {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("Compressor").field("backend", &"zrip").finish()
        }
    }
}

#[cfg(feature = "qoiz-zstd")]
pub(crate) mod zstd_backend {
    use crate::{ServerError, ServerErrorExt, ServerResult};
    use crate::encoder::qoiz::COMPRESSION_LEVEL;

    /// Streaming compressor for the QOIZ codec.
    pub struct Compressor {
        zctxt: zstd_safe::CCtx<'static>,
    }

    impl Compressor {
        pub fn new() -> ServerResult<Self> {
            let mut zctxt = zstd_safe::CCtx::default();

            zctxt
                .set_parameter(zstd_safe::CParameter::CompressionLevel(COMPRESSION_LEVEL))
                .map_err(|code| {
                    ServerError::reason(
                        "qoiz init",
                        format!("failed to set zstd compression level: {}",
                        zstd_safe::get_error_name(code))
                    )
                })?;
            zctxt
                .set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(true))
                .map_err(|code| {
                    ServerError::reason(
                        "qoiz init",
                        format!(
                        "failed to set zstd enable long distance matching: {}",
                        zstd_safe::get_error_name(code))
                    )
                })?;

            Ok(Self { zctxt })
        }

        /// Compresses `input` and returns the bytes produced for this update.
        pub fn compress(&mut self, input: &[u8]) -> ServerResult<Vec<u8>> {
            let mut inb = zstd_safe::InBuffer::around(input);
            let mut data = vec![0; input.len()];
            let mut outb;
            let mut pos = 0;

            loop {
                outb = zstd_safe::OutBuffer::around_pos(data.as_mut_slice(), pos);
                let res = self
                    .zctxt
                    .compress_stream2(
                        &mut outb,
                        &mut inb,
                        zstd_safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_flush,
                    )
                    .map_err(|code| ServerError::reason("qoiz compress", format!("failed to Zstd compress: {}", zstd_safe::get_error_name(code))))?;
                if res == 0 {
                    break;
                }
                pos = outb.pos();
                data.resize(data.len() + res, 0);
            }

            let len = outb.pos();
            data.truncate(len);

            Ok(data)
        }
    }

    impl core::fmt::Debug for Compressor {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("Compressor").field("backend", &"zstd-safe").finish()
        }
    }
}

#[cfg(not(feature = "qoiz-zstd"))]
pub(crate) use zrip_backend::Compressor;
#[cfg(feature = "qoiz-zstd")]
pub(crate) use zstd_backend::Compressor;
