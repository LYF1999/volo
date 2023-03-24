use std::io;

use futures::Future;
use motore::service::{Service, UnaryService};
use pilota::thrift::{Message, TransportError, TransportErrorKind};
use volo::{
    net::{dial::MakeTransport, Address},
    Unwrap,
};

use crate::{
    codec::MakeCodec,
    context::ClientContext,
    protocol::TMessageType,
    transport::{
        pingpong::thrift_transport::ThriftTransport,
        pool::{Config, PooledMakeTransport},
    },
    ThriftMessage,
};

#[derive(Clone)]
pub struct MakeClientTransport<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf>,
{
    make_transport: MkT,
    make_codec: MkC,
}

impl<MkT, MkC> MakeClientTransport<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf>,
{
    #[allow(unused)]
    pub fn new(make_transport: MkT, make_codec: MkC) -> Self {
        Self {
            make_transport,
            make_codec,
        }
    }
}

impl<MkT, MkC> UnaryService<Address> for MakeClientTransport<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    type Response = ThriftTransport<MkC::Encoder, MkC::Decoder>;
    type Error = io::Error;
    type Future<'s> = impl Future<Output = Result<Self::Response, Self::Error>> + 's;

    fn call(&self, target: Address) -> Self::Future<'_> {
        let make_transport = self.make_transport.clone();
        async move {
            let (rh, wh) = make_transport.make_transport(target).await?;
            Ok(ThriftTransport::new(rh, wh, self.make_codec.clone()))
        }
    }
}

pub struct Client<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    #[allow(clippy::type_complexity)]
    make_transport: PooledMakeTransport<MakeClientTransport<MkT, MkC>, Address>,
}

impl<MkT, MkC> Clone for Client<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    fn clone(&self) -> Self {
        Self {
            make_transport: self.make_transport.clone(),
        }
    }
}

impl<MkT, MkC> Client<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    pub fn new(make_transport: MkT, pool_cfg: Option<Config>, make_codec: MkC) -> Self {
        let make_transport = MakeClientTransport::new(make_transport, make_codec);
        let make_transport = PooledMakeTransport::new(make_transport, pool_cfg);
        Client { make_transport }
    }
}

impl<MkT, MkC> Client<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    pub async fn call<'cx, 's, Req: Message, Resp: Message>(
        &'s self,
        cx: &'cx mut ClientContext,
        req: ThriftMessage<Req>,
    ) -> Result<Option<ThriftMessage<Resp>>, crate::Error>
    where
        's: 'cx,
    {
        let rpc_info = &cx.rpc_info;
        let target = rpc_info.callee().volo_unwrap().address().ok_or_else(|| {
            TransportError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("address is required, rpc_info: {:?}", rpc_info),
            ))
        })?;
        let oneway = cx.message_type == TMessageType::OneWay;
        cx.stats.record_make_transport_start_at();
        let mut transport = self.make_transport.call(target).await?;
        cx.stats.record_make_transport_end_at();
        let resp = transport.send(cx, req, oneway).await;
        if let Ok(None) = resp {
            if !oneway {
                return Err(crate::Error::Transport(
                    pilota::thrift::TransportError::new(
                        TransportErrorKind::EndOfFile,
                        format!(
                            "an unexpected end of file from server, rpc_info: {:?}",
                            cx.rpc_info
                        ),
                    ),
                ));
            }
        }
        if cx.transport.should_reuse && resp.is_ok() {
            transport.reuse();
        }
        resp
    }
}
