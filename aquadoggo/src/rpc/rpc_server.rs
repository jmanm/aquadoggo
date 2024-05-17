use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use futures::{AsyncReadExt, TryFutureExt};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use log::{warn, error};

use crate::{aquadoggo_capnp, bus::ServiceSender, context::Context, manager::{ServiceReadySender, Shutdown}};

use super::document_repository::DocumentRepository;

pub async fn rpc_server(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<(), anyhow::Error> {
    let capnp_port = context.config.capnp_port;
    let capnp_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), capnp_port);

    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(&capnp_addr).await?;
            let document_repository = DocumentRepository::new(context, tx);
            let client: aquadoggo_capnp::document_repository::Client = capnp_rpc::new_client(document_repository);

            if tx_ready.send(()).is_err() {
                warn!("No subscriber informed about gRPC service being ready");
            };

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let (reader, writer) =
                    tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                let network = twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Server,
                    Default::default(),
                );

                let rpc_system = RpcSystem::new(Box::new(network), Some(client.clone().client));

                tokio::task::spawn_local({
                    rpc_system.map_err(|e| error!("error: {e:?}"))
                    // signal.map_err(|e| error!("error: {e:?}"))
                });

            }
        }).await
}