//! Zstd decompression backend for the QOIZ bitmap codec.
//!
//! Two backends are available:
//!
//! - `zrip`, a pure Rust zstd implementation, used by default.
//! - `zstd-safe`, bindings to the reference C implementation, used when the
//!   `qoiz-zstd` feature is enabled.
//!
//! Both consume standard zstd frames, so the choice of backend is invisible on
//! the wire and either side of a connection may use either one.

#[cfg_attr(all(feature = "qoiz-zstd", not(feature = "__test")), allow(dead_code))]
pub(crate) mod zrip_backend {
    use std::io::Read as _;
    use std::sync::{Arc, Mutex};

    /// Size of the chunks pulled out of the decoder at a time.
    const READ_CHUNK: usize = 16 * 1024;

    /// Source feeding one update at a time to the decoder.
    #[derive(Clone, Debug, Default)]
    struct SharedSource(Arc<Mutex<Vec<u8>>>);

    impl std::io::Read for SharedSource {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let mut source = self.0.lock().map_err(|_| std::io::Error::other("poisoned source"))?;
            let len = buf.len().min(source.len());
            buf[..len].copy_from_slice(&source[..len]);
            source.drain(..len);
            Ok(len)
        }
    }

    /// Streaming decompressor for the QOIZ codec.
    ///
    /// Must be fed every update of a session, in order: an update is decoded
    /// against the history of the ones before it.
    pub struct Decompressor {
        decoder: zrip::FrameDecoder<SharedSource>,
        source: SharedSource,
    }

    impl Decompressor {
        pub fn new() -> Self {
            let source = SharedSource::default();

            Self {
                decoder: zrip::FrameDecoder::new(source.clone()),
                source,
            }
        }

        /// Decompresses the payload of a single surface command.
        pub fn decompress(&mut self, input: &[u8]) -> std::io::Result<Vec<u8>> {
            {
                let mut source = self
                    .source
                    .0
                    .lock()
                    .map_err(|_| std::io::Error::other("poisoned source"))?;
                source.extend_from_slice(input);
            }

            let mut data = Vec::new();
            let mut buf = [0; READ_CHUNK];

            loop {
                match self.decoder.read(&mut buf) {
                    Ok(0) => break,
                    Ok(len) => data.extend_from_slice(&buf[..len]),
                    // The server flushes after every update, so an update always
                    // ends on a block boundary. EOF means that this update is fully
                    // decoded, and the decoder picks the stream back up when the next one arrives.
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
            }

            Ok(data)
        }
    }

    impl core::fmt::Debug for Decompressor {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("Decompressor").field("backend", &"zrip").finish()
        }
    }
}

#[cfg(feature = "qoiz-zstd")]
pub(crate) mod zstd_backend {
    /// Streaming decompressor for the QOIZ codec.
    ///
    /// Must be fed every update of a session, in order: an update is decoded
    /// against the history of the ones before it.
    pub struct Decompressor {
        zdctx: zstd_safe::DCtx<'static>,
    }

    impl Decompressor {
        pub fn new() -> Self {
            Self {
                zdctx: zstd_safe::DCtx::default(),
            }
        }

        /// Decompresses the payload of a single surface command.
        pub fn decompress(&mut self, input: &[u8]) -> std::io::Result<Vec<u8>> {
            let mut source = zstd_safe::InBuffer::around(input);
            let mut data = vec![0; input.len() * 4];
            let mut pos = 0;

            loop {
                let mut output = zstd_safe::OutBuffer::around_pos(data.as_mut_slice(), pos);
                self.zdctx
                    .decompress_stream(&mut output, &mut source)
                    .map_err(zstd_safe::get_error_name)
                    .map_err(std::io::Error::other)?;
                pos = output.pos();
                if pos == output.capacity() {
                    data.resize(data.capacity() * 2, 0);
                } else {
                    break;
                }
            }

            data.truncate(pos);

            Ok(data)
        }
    }

    impl core::fmt::Debug for Decompressor {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("Decompressor").field("backend", &"zstd-safe").finish()
        }
    }
}

#[cfg(not(feature = "qoiz-zstd"))]
pub(crate) use zrip_backend::Decompressor;
#[cfg(feature = "qoiz-zstd")]
pub(crate) use zstd_backend::Decompressor;
