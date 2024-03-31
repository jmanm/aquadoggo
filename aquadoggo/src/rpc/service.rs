use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Result;
use log::{debug, warn};
use tonic::transport::Server;

use crate::{aquadoggo_rpc::connect_server::ConnectServer, bus::ServiceSender, context::Context, manager::{ServiceReadySender, Shutdown}};

use super::rpc_server::RpcServer;

pub async fn rpc_service(
    context: Context,
    signal: Shutdown,
    tx: ServiceSender,
    tx_ready: ServiceReadySender,
) -> Result<()> {
    let grpc_port = context.config.grpc_port;
    let grpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), grpc_port);

    let handler = RpcServer::new(context, tx);

    Server::builder()
        .add_service(ConnectServer::new(handler))
        .serve_with_shutdown(grpc_addr, async {
            debug!("gRPC service is ready");
            if tx_ready.send(()).is_err() {
                warn!("No subscriber informed about gRPC service being ready");
            };

            signal.await.ok();
        })
        .await?;
    Ok(())
}