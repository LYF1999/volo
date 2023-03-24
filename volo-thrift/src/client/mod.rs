//! Thrift client for Volo.
//!
//! Users should not use this module directly.
//! Instead, they should use the `Builder` type in the generated code.
//!
//! For users need to specify some options at call time, they may use ['callopt'][callopt].

use std::{
    marker::PhantomData,
    sync::{atomic::AtomicI32, Arc},
};

use faststr::FastStr;
use futures::Future;
use motore::{
    layer::{Identity, Layer, Layers, Stack},
    service::Service,
};
use pilota::thrift::{Message, TMessageType};
use tokio::time::Duration;
use volo::{
    context::{Endpoint, Role, RpcInfo},
    discovery::{Discover, DummyDiscover},
    loadbalance::{random::WeightedRandomBalance, LbConfig, MkLbLayer},
    net::{
        dial::{DefaultMakeTransport, MakeTransport},
        Address,
    },
};

use crate::{
    codec::{
        default::{framed::MakeFramedCodec, thrift::MakeThriftCodec, ttheader::MakeTTHeaderCodec},
        DefaultMakeCodec, MakeCodec,
    },
    context::{ClientContext, Config},
    error::{Error, Result},
    transport::{pingpong, pool},
    ThriftMessage,
};

mod callopt;
pub use callopt::CallOpt;

pub mod layer;

pub struct ClientBuilder<IL, OL, MkClient, MkT, MkC, LB> {
    config: Config,
    pool: Option<pool::Config>,
    callee_name: FastStr,
    caller_name: FastStr,
    address: Option<Address>, // maybe address use Arc avoid memory alloc
    inner_layer: IL,
    outer_layer: OL,
    make_transport: MkT,
    make_codec: MkC,
    mk_client: MkClient,
    mk_lb: LB,

    #[cfg(feature = "multiplex")]
    multiplex: bool,
}

impl<C>
    ClientBuilder<
        Identity,
        Identity,
        C,
        DefaultMakeTransport,
        DefaultMakeCodec<MakeTTHeaderCodec<MakeFramedCodec<MakeThriftCodec>>>,
        LbConfig<WeightedRandomBalance<<DummyDiscover as Discover>::Key>, DummyDiscover>,
    >
{
    pub fn new(service_name: impl AsRef<str>, service_client: C) -> Self {
        ClientBuilder {
            config: Default::default(),
            pool: None,
            caller_name: "".into(),
            callee_name: FastStr::new(service_name),
            address: None,
            inner_layer: Identity::new(),
            outer_layer: Identity::new(),
            mk_client: service_client,
            make_transport: DefaultMakeTransport::default(),
            make_codec: DefaultMakeCodec::default(),
            mk_lb: LbConfig::new(WeightedRandomBalance::new(), DummyDiscover {}),

            #[cfg(feature = "multiplex")]
            multiplex: false,
        }
    }
}

impl<IL, OL, C, MkT, MkC, LB, DISC> ClientBuilder<IL, OL, C, MkT, MkC, LbConfig<LB, DISC>> {
    pub fn load_balance<NLB>(
        self,
        load_balance: NLB,
    ) -> ClientBuilder<IL, OL, C, MkT, MkC, LbConfig<NLB, DISC>> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb.load_balance(load_balance),

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    pub fn discover<NDISC>(
        self,
        discover: NDISC,
    ) -> ClientBuilder<IL, OL, C, MkT, MkC, LbConfig<LB, NDISC>> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb.discover(discover),

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Sets the retry count of the client.
    pub fn retry_count(mut self, count: usize) -> Self {
        self.mk_lb = self.mk_lb.retry_count(count);
        self
    }
}

impl<IL, OL, C, MkT, MkC, LB> ClientBuilder<IL, OL, C, MkT, MkC, LB> {
    /// Sets the rpc timeout for the client.
    ///
    /// The default value is 1 second.
    ///
    /// Users can set this to `None` to disable the timeout.
    pub fn rpc_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.set_rpc_timeout(timeout);
        self
    }

    /// Sets the config for connection pool.
    pub fn pool_config(mut self, config: pool::Config) -> Self {
        self.pool = Some(config);
        self
    }

    /// Sets the connect timeout for the client.
    pub fn connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.set_connect_timeout(timeout);
        self
    }

    /// Sets the read write timeout for the client(a.k.a. IO timeout).
    pub fn read_write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.set_read_write_timeout(timeout);
        self
    }

    /// Sets the max frame size for the client.
    ///
    /// Defaults to 16MB.
    pub fn max_frame_size(mut self, max_frame_size: u32) -> Self {
        self.config.set_max_frame_size(max_frame_size);
        self
    }

    /// Sets the client's name sent to the server.
    pub fn caller_name(mut self, name: impl AsRef<str>) -> Self {
        self.caller_name = FastStr::new(name);
        self
    }

    pub fn mk_load_balance<NLB>(
        self,
        mk_load_balance: NLB,
    ) -> ClientBuilder<IL, OL, C, MkT, MkC, NLB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: mk_load_balance,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Set the codec to use for the client.
    ///
    /// This should not be used by most users, Volo has already provided a default encoder.
    /// This is only useful if you want to customize some protocol.
    ///
    /// If you only want to transform metadata across microservices, you can use [`metainfo`] to do
    /// this.
    #[doc(hidden)]
    pub fn make_codec<MakeCodec>(
        self,
        make_codec: MakeCodec,
    ) -> ClientBuilder<IL, OL, C, MkT, MakeCodec, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec,
            mk_lb: self.mk_lb,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Set the transport to use for the client.
    #[doc(hidden)]
    pub fn make_transport<MakeTransport>(
        self,
        make_transport: MakeTransport,
    ) -> ClientBuilder<IL, OL, C, MakeTransport, MkC, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Sets the target address.
    ///
    /// If the address is set, the call will be sent to the address directly.
    ///
    /// The client will skip the discovery and loadbalance Service if this is set.
    pub fn address<A: Into<Address>>(mut self, target: A) -> Self {
        self.address = Some(target.into());
        self
    }

    /// Adds a new inner layer to the client.
    ///
    /// The layer's `Service` should be `Send + Sync + Clone + 'static`.
    ///
    /// # Order
    ///
    /// Assume we already have two layers: foo and bar. We want to add a new layer baz.
    ///
    /// The current order is: foo -> bar (the request will come to foo first, and then bar).
    ///
    /// After we call `.layer_inner(baz)`, we will get: foo -> bar -> baz.
    ///
    /// The overall order for layers is: Timeout -> outer -> LoadBalance -> [inner] -> transport.
    pub fn layer_inner<Inner>(
        self,
        layer: Inner,
    ) -> ClientBuilder<Stack<Inner, IL>, OL, C, MkT, MkC, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: Stack::new(layer, self.inner_layer),
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Adds a new outer layer to the client.
    ///
    /// The layer's `Service` should be `Send + Sync + Clone + 'static`.
    ///
    /// # Order
    ///
    /// Assume we already have two layers: foo and bar. We want to add a new layer baz.
    ///
    /// The current order is: foo -> bar (the request will come to foo first, and then bar).
    ///
    /// After we call `.layer_outer(baz)`, we will get: foo -> bar -> baz.
    ///
    /// The overall order for layers is: Timeout -> [outer] -> LoadBalance -> inner -> transport.
    pub fn layer_outer<Outer>(
        self,
        layer: Outer,
    ) -> ClientBuilder<IL, Stack<Outer, OL>, C, MkT, MkC, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: Stack::new(layer, self.outer_layer),
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Adds a new outer layer to the client.
    ///
    /// The layer's `Service` should be `Send + Sync + Clone + 'static`.
    ///
    /// # Order
    ///
    /// Assume we already have two layers: foo and bar. We want to add a new layer baz.
    ///
    /// The current order is: foo -> bar (the request will come to foo first, and then bar).
    ///
    /// After we call `.layer_outer_front(baz)`, we will get: baz -> foo -> bar.
    ///
    /// The overall order for layers is: Timeout -> [outer] -> LoadBalance -> inner -> transport.
    pub fn layer_outer_front<Outer>(
        self,
        layer: Outer,
    ) -> ClientBuilder<IL, Stack<OL, Outer>, C, MkT, MkC, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: Stack::new(self.outer_layer, layer),
            mk_client: self.mk_client,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb,

            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    #[cfg(feature = "multiplex")]
    /// Enable multiplexing for the client.
    #[doc(hidden)]
    pub fn multiplex(self, multiplex: bool) -> ClientBuilder<IL, OL, C, Req, Resp, MkT, MkC, LB> {
        ClientBuilder {
            config: self.config,
            pool: self.pool,
            caller_name: self.caller_name,
            callee_name: self.callee_name,
            address: self.address,
            inner_layer: self.inner_layer,
            outer_layer: self.outer_layer,
            mk_client: self.mk_client,
            _marker: PhantomData,
            make_transport: self.make_transport,
            make_codec: self.make_codec,
            mk_lb: self.mk_lb,

            multiplex,
        }
    }
}

#[derive(Clone)]
pub struct MessageService<Resp, MkT, MkC>
where
    Resp: Message + Send + 'static,
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    #[cfg(not(feature = "multiplex"))]
    inner: pingpong::Client<MkT, MkC>,
    #[cfg(feature = "multiplex")]
    inner: motore::utils::Either<
        pingpong::Client<Resp, MkT, MkC>,
        crate::transport::multiplex::Client<Resp, MkT, MkC>,
    >,
    _marker: PhantomData<fn() -> Resp>,
}

pub struct MkMessageService<MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    #[cfg(not(feature = "multiplex"))]
    inner: pingpong::Client<MkT, MkC>,
    #[cfg(feature = "multiplex")]
    inner: motore::utils::Either<
        pingpong::Client<Resp, MkT, MkC>,
        crate::transport::multiplex::Client<Resp, MkT, MkC>,
    >,
}

impl<Resp, MkT, MkC> volo::service::MakeService<Resp> for MkMessageService<MkT, MkC>
where
    Resp: Message + Send + 'static,
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    type Service = MessageService<Resp, MkT, MkC>;

    type Error = Error;

    type Future = impl Future<Output = Result<Self::Service, Self::Error>> + Send;

    fn make_service(&self) -> Self::Future {
        let inner = self.inner.clone();
        async move {
            Ok(MessageService {
                _marker: PhantomData,
                inner,
            })
        }
    }
}

impl<Req, Resp, MkT, MkC> Service<ClientContext, Req> for MessageService<Resp, MkT, MkC>
where
    Req: Message + 'static + Send,
    Resp: Send + 'static + Message + Sync,
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    type Response = Option<Resp>;

    type Error = Error;

    type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>> + 'cx + Send where Self:'cx;

    fn call<'cx, 's>(&'s self, cx: &'cx mut ClientContext, req: Req) -> Self::Future<'cx>
    where
        's: 'cx,
    {
        async move {
            let msg = ThriftMessage::mk_client_msg(cx, Ok(req))?;
            let resp = self.inner.call(cx, msg).await;
            match resp {
                Ok(Some(ThriftMessage { data: Ok(data), .. })) => Ok(Some(data)),
                Ok(Some(ThriftMessage { data: Err(e), .. })) => Err(e),
                Err(e) => Err(e),
                Ok(None) => Ok(None),
            }
        }
    }
}

impl<IL, OL, C, MkT, MkC, LB> ClientBuilder<IL, OL, C, MkT, MkC, LB>
where
    C: volo::client::MkClient<
        Client<MkMessageService<MkT, MkC>, Layers<Stack<Stack<IL, <LB as MkLbLayer>::Layer>, OL>>>,
    >,
    LB: MkLbLayer,
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    /// Build volo client.
    pub fn build(mut self) -> C::Target {
        if let Some(timeout) = self.config.connect_timeout() {
            self.make_transport.set_connect_timeout(Some(timeout));
        }
        if let Some(timeout) = self.config.read_write_timeout() {
            self.make_transport.set_read_timeout(Some(timeout));
        }
        if let Some(timeout) = self.config.read_write_timeout() {
            self.make_transport.set_write_timeout(Some(timeout));
        }
        let mk_msg_svc = MkMessageService {
            #[cfg(not(feature = "multiplex"))]
            inner: pingpong::Client::new(self.make_transport, self.pool, self.make_codec),
            #[cfg(feature = "multiplex")]
            inner: if !self.multiplex {
                motore::utils::Either::A(pingpong::Client::new(
                    self.make_transport,
                    self.pool,
                    self.make_codec,
                ))
            } else {
                motore::utils::Either::B(crate::transport::multiplex::Client::new(
                    self.make_transport,
                    self.pool,
                    self.make_codec,
                ))
            },
        };

        let layers = Layers::new(self.inner_layer)
            .push(self.mk_lb.make())
            .push(self.outer_layer);

        self.mk_client.mk_client(Client {
            inner: Arc::new(ClientInner {
                callee_name: self.callee_name,
                config: self.config,
                address: self.address,
                caller_name: self.caller_name,
                seq_id: AtomicI32::new(0),
            }),
            make_service: mk_msg_svc,
            layer: layers,
        })
    }
}

/// A client for a Thrift service.
///
/// `Client` is designed to "clone and use", so it's cheap to clone it.
/// One important thing is that the `CallOpt` will not be cloned, because
/// it's designed to be per-request.
#[derive(Clone)]
pub struct Client<MS, L> {
    make_service: MS,
    layer: L,
    inner: Arc<ClientInner>,
}

impl<MS, L> Client<MS, L> {
    pub async fn build_service<Resp>(
        &self,
    ) -> Result<
        <L as Layer<<MS as volo::service::MakeService<Resp>>::Service>>::Service,
        crate::Error,
    >
    where
        MS: volo::service::MakeService<Resp, Error = crate::Error>,
        L: Layer<MS::Service>,
    {
        let inner_service = self.make_service.make_service().await?;

        Ok(self.layer.layer(inner_service))
    }
}

// unsafe impl<Req, Resp> Sync for Client<Req, Resp> {}

struct ClientInner {
    callee_name: FastStr,
    caller_name: FastStr,
    config: Config,
    address: Option<Address>,
    seq_id: AtomicI32,
}

impl<MS, L> Client<MS, L> {
    pub fn make_cx(&self, method: &'static str, oneway: bool) -> ClientContext {
        ClientContext::new(
            self.inner
                .seq_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            self.make_rpc_info(method),
            if oneway {
                TMessageType::OneWay
            } else {
                TMessageType::Call
            },
        )
    }

    fn make_rpc_info(&self, method: &'static str) -> RpcInfo<Config> {
        let caller = Endpoint::new(self.inner.caller_name.clone());
        let mut callee = Endpoint::new(self.inner.callee_name.clone());
        if let Some(target) = &self.inner.address {
            callee.set_address(target.clone());
        }
        let config = self.inner.config;

        RpcInfo::new(Role::Client, method.into(), caller, callee, config)
    }

    // pub fn with_opt<Opt>(self, opt: Opt) -> Client<WithOptService<S, Opt>> {
    //     Client {
    //         transport: WithOptService::new(self.transport, opt),
    //         inner: self.inner,
    //     }
    // }
}

// macro_rules! impl_client {
//     (($self: ident, &mut $cx:ident, $req: ident) => async move $e: tt ) => {
//         impl<S, Req: Send + 'static, Res: 'static>
//             volo::service::Service<crate::context::ClientContext, Req> for Client<MS, L>
//         where
//             S: volo::service::Service<
//                     crate::context::ClientContext,
//                     Req,
//                     Response = Option<Res>,
//                     Error = crate::Error,
//                 > + Sync
//                 + Send
//                 + 'static,
//         {
//             type Response = S::Response;
//             type Error = S::Error;
//             type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>> + 'cx;

//             fn call<'cx, 's>(
//                 &'s $self,
//                 $cx: &'cx mut crate::context::ClientContext,
//                 $req: Req,
//             ) -> Self::Future<'cx>
//             where
//                 's: 'cx,
//             {
//                 async move { $e }
//             }
//         }

//         impl<S, Req: Send + 'static, Res: 'static>
//             volo::client::OneShotService<crate::context::ClientContext, Req> for Client<S>
//         where
//             S: volo::client::OneShotService<
//                     crate::context::ClientContext,
//                     Req,
//                     Response = Option<Res>,
//                     Error = crate::Error,
//                 > + Sync
//                 + Send
//                 + 'static,
//         {
//             type Response = S::Response;
//             type Error = S::Error;
//             type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>> + 'cx;

//             fn call<'cx>(
//                 $self,
//                 $cx: &'cx mut crate::context::ClientContext,
//                 $req: Req,
//             ) -> Self::Future<'cx>
//             where
//                 Self: 'cx,
//             {
//                 async move { $e }
//             }
//         }
//     };
// }

// impl_client!((self, &mut cx, req) => async move {

//     let has_metainfo = metainfo::METAINFO.try_with(|_| {}).is_ok();

//     let mk_call = async { self.transport.call(cx, req).await };

//     if has_metainfo {
//         mk_call.await
//     } else {
//         metainfo::METAINFO
//             .scope(RefCell::new(metainfo::MetaInfo::default()), mk_call)
//             .await
//     }
// });
