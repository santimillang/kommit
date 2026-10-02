use std::io;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Kafka's default `socket.request.max.bytes` is 100 MiB.
pub const MAX_FRAME: usize = 100 * 1024 * 1024;

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<Bytes>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = i32::from_be_bytes(len);
    if len < 0 || len as usize > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad frame length {len}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(Some(Bytes::from(buf)))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    w.write_all(&(payload.len() as i32).to_be_bytes()).await?;
    w.write_all(payload).await?;
    w.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_roundtrip_and_eof_is_clean() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_frame(&mut a, b"hello").await.unwrap();
        drop(a);
        assert_eq!(
            read_frame(&mut b).await.unwrap().unwrap(),
            Bytes::from_static(b"hello")
        );
        assert!(read_frame(&mut b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn negative_length_is_rejected() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        a.write_all(&(-5i32).to_be_bytes()).await.unwrap();
        assert!(read_frame(&mut b).await.is_err());
    }
}
