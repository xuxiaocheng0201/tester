// #[tokio::test]
// async fn main() -> anyhow::Result<()> {
//     const ALPN: &[u8] = b"test";
//     let (tx_addr, rx_addr) = tokio::sync::oneshot::channel();
//     let (tx_cancel, rx_cancel) = tokio::sync::oneshot::channel();
//     let server = tokio::spawn(async move {
//         let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
//             .relay_mode(iroh::endpoint::RelayMode::Disabled)
//             .alpns(vec![ALPN.to_vec()])
//             .bind().await?;
//         tx_addr.send(endpoint.addr()).unwrap();
//
//         let conn = endpoint.accept().await.unwrap().await?;
//         let (mut send, mut recv) = conn.accept_bi().await?;
//         tokio::io::copy(&mut recv, &mut send).await?;
//         send.finish()?;
//         conn.closed().await;
//
//         rx_cancel.await?;
//         anyhow::Ok(())
//     });
//     let addr = rx_addr.await?;
//     println!("{addr:?}");
//     let client = tokio::spawn(async move {
//         let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
//             .relay_mode(iroh::endpoint::RelayMode::Disabled)
//             .bind().await?;
//         let conn = endpoint.connect(addr, ALPN).await?;
//
//         let (mut send, mut recv) = conn.open_bi().await?;
//         send.write_all(b"Hello, world!").await?;
//         send.finish()?;
//         let response = recv.read_to_end(100).await?;
//         assert_eq!(&response, b"Hello, world!");
//         conn.close(0u32.into(), b"bye!");
//
//         endpoint.close().await;
//         tx_cancel.send(()).unwrap();
//         anyhow::Ok(())
//     });
//     server.await??;
//     client.await??;
//     Ok(())
// }

use anyhow::Result;
use iroh::endpoint::{RecvStream, SendStream};
use iroh::Endpoint;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::spawn;

static ALPN: &[u8] = b"x-wlist-v1";

#[must_use]
pub struct QuicStream {
    send: BufWriter<SendStream>,
    recv: BufReader<RecvStream>,
}

impl QuicStream {
    #[inline]
    pub(crate) fn new(send: SendStream, recv: RecvStream) -> Self {
        Self {
            send: BufWriter::new(send),
            recv: BufReader::new(recv),
        }
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.send.write_all(&data.len().to_le_bytes()).await?;
        self.send.write_all(data).await?;
        self.send.flush().await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let mut ident = [0; (usize::BITS / 8) as usize];
        self.recv.read_exact(&mut ident).await?;
        let len = usize::from_le_bytes(ident);
        let mut data = vec![0; len];
        self.recv.read_exact(&mut data).await?;
        Ok(data)
    }
}


#[tokio::test]
async fn main() -> Result<()> {
    let server_endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
        .relay_mode(iroh::endpoint::RelayMode::Disabled)
        .alpns(vec![ALPN.to_vec()])
        .bind().await?;
    let target = server_endpoint.addr();
    let client_endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
        .relay_mode(iroh::endpoint::RelayMode::Disabled)
        .bind().await?;
    let server = spawn(async move {
        println!("Server start.");
        let connection = loop {
            let incoming = server_endpoint.accept().await.unwrap();
            println!("Retry incoming.");
            if !incoming.remote_addr_validated() {
                let _ = incoming.retry();
                continue;
            }
            break incoming.await
        }?;
        println!("Accepting bi.");
        let (send, recv) = connection.accept_bi().await?;
        let mut stream = QuicStream::new(send, recv);
        println!("Receiving.");
        let bytes = stream.recv().await?;
        assert_eq!(&bytes, b"hello");
        println!("Received.");
        let _ = stream.send.get_mut().finish();
        let _ = stream.recv.get_mut().stop(0u32.into());
        connection.close(0u32.into(), b"server connection close");
        connection.closed().await;
        server_endpoint.close().await;
        anyhow::Ok(())
    });
    let client = spawn(async move {
        println!("Client start.");
        let connection = client_endpoint.connect(target, ALPN).await?;
        println!("Opening bi.");
        let (send, recv) = connection.open_bi().await?;
        let mut stream = QuicStream::new(send, recv);
        println!("Sending.");
        stream.send(b"hello").await?;
        println!("Sent.");
        let _ = stream.send.get_mut().finish();
        let _ = stream.recv.get_mut().stop(0u32.into());
        connection.closed().await;
        client_endpoint.close().await;
        anyhow::Ok(())
    });
    server.await??;
    client.await??;
    Ok(())
}
