use std::{
    marker::PhantomData,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use motore::{
    layer::{Identity, Layer, Stack},
    service::Service,
    BoxError,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
};
use tracing::info;
use volo::net::{
    conn::{OwnedReadHalf, OwnedWriteHalf},
    incoming::Incoming,
};

use crate::{
    codec::{
        default::{framed::MakeFramedCodec, thrift::MakeThriftCodec, ttheader::MakeTTHeaderCodec},
        DefaultMakeCodec, MakeCodec,
    },
    context::ServerContext,
    EntryMessage, Result,
};

pub struct Server<S, L, Req, MkC> {
    service: S,
    layer: L,
    make_codec: MkC,
    _marker: PhantomData<fn(Req)>,
    #[cfg(feature = "multiplex")]
    multiplex: bool,
}

impl<S, Req>
    Server<S, Identity, Req, DefaultMakeCodec<MakeTTHeaderCodec<MakeFramedCodec<MakeThriftCodec>>>>
{
    pub fn new(service: S) -> Self
    where
        S: Service<ServerContext, Req>,
    {
        Self {
            make_codec: DefaultMakeCodec::default(),
            service,
            layer: Identity::new(),
            _marker: PhantomData,
            #[cfg(feature = "multiplex")]
            multiplex: false,
        }
    }
}

impl<S, L, Req, MkC> Server<S, L, Req, MkC> {
    /// Adds a new inner layer to the server.
    ///
    /// The layer's `Service` should be `Send + Clone + 'static`.
    ///
    /// # Order
    ///
    /// Assume we already have two layers: foo and bar. We want to add a new layer baz.
    ///
    /// The current order is: foo -> bar (the request will come to foo first, and then bar).
    ///
    /// After we call `.layer(baz)`, we will get: foo -> bar -> baz.
    pub fn layer<Inner>(self, layer: Inner) -> Server<S, Stack<Inner, L>, Req, MkC> {
        Server {
            layer: Stack::new(layer, self.layer),
            service: self.service,
            make_codec: self.make_codec,
            _marker: PhantomData,
            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Adds a new front layer to the server.
    ///
    /// The layer's `Service` should be `Send + Clone + 'static`.
    ///
    /// # Order
    ///
    /// Assume we already have two layers: foo and bar. We want to add a new layer baz.
    ///
    /// The current order is: foo -> bar (the request will come to foo first, and then bar).
    ///
    /// After we call `.layer_front(baz)`, we will get: baz -> foo -> bar.
    pub fn layer_front<Front>(self, layer: Front) -> Server<S, Stack<L, Front>, Req, MkC> {
        Server {
            layer: Stack::new(self.layer, layer),
            service: self.service,
            make_codec: self.make_codec,
            _marker: PhantomData,
            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// Set the codec to use for the server.
    ///
    /// This should not be used by most users, Volo has already provided a default encoder.
    /// This is only useful if you want to customize some protocol.
    ///
    /// If you only want to transform metadata across microservices, you can use [`metainfo`] to do
    /// this.
    #[doc(hidden)]
    pub fn make_codec<MakeCodec>(self, make_codec: MakeCodec) -> Server<S, L, Req, MakeCodec> {
        Server {
            layer: self.layer,
            service: self.service,
            make_codec,
            _marker: PhantomData,
            #[cfg(feature = "multiplex")]
            multiplex: self.multiplex,
        }
    }

    /// The main entry point for the server.
    pub async fn run<MI: volo::net::incoming::MakeIncoming>(
        self,
        make_incoming: MI,
    ) -> Result<(), BoxError>
    where
        L: Layer<S>,
        MkC: MakeCodec<OwnedReadHalf, OwnedWriteHalf>,
        L::Service:
            Service<ServerContext, Req, Response = S::Response> + Clone + Send + 'static + Sync,
        <L::Service as Service<ServerContext, Req>>::Error: Into<crate::Error> + Send,
        for<'cx> <L::Service as Service<ServerContext, Req>>::Future<'cx>: Send,
        S: Service<ServerContext, Req> + Clone + Send + 'static,
        S::Error: Into<crate::Error> + Send,
        Req: EntryMessage + Send + 'static,
        S::Response: EntryMessage + Send + 'static + Sync,
    {
        // init server
        let service = self.layer.layer(self.service);

        let mut incoming = make_incoming.make_incoming().await?;
        info!("[VOLO] server start at: {:?}", incoming);

        let conn_cnt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let gconn_cnt = conn_cnt.clone();
        let (exit_notify, exit_flag, exit_mark) = (
            Arc::new(Notify::const_new()),
            Arc::new(parking_lot::RwLock::new(false)),
            Arc::new(std::sync::atomic::AtomicBool::default()),
        );
        let (exit_notify_inner, exit_flag_inner, exit_mark_inner) =
            (exit_notify.clone(), exit_flag.clone(), exit_mark.clone());

        // spawn accept loop
        let handler = tokio::spawn(async move {
            loop {
                match incoming.accept().await {
                    Ok(Some(conn)) => {
                        let (rh, wh) = conn.stream.into_split();
                        conn_cnt.fetch_add(1, Ordering::Relaxed);
                        let service = service.clone();

                        #[cfg(feature = "multiplex")]
                        if self.multiplex {
                            tokio::spawn(handle_conn_multiplex(
                                rh,
                                wh,
                                service,
                                self.make_codec.clone(),
                                exit_notify_inner.clone(),
                                exit_flag_inner.clone(),
                                exit_mark_inner.clone(),
                                conn_cnt.clone(),
                            ));
                        } else {
                            tokio::spawn(handle_conn(
                                rh,
                                wh,
                                service,
                                self.make_codec.clone(),
                                exit_notify_inner.clone(),
                                exit_flag_inner.clone(),
                                exit_mark_inner.clone(),
                                conn_cnt.clone(),
                            ));
                        }
                        #[cfg(not(feature = "multiplex"))]
                        tokio::spawn(handle_conn(
                            rh,
                            wh,
                            service,
                            self.make_codec.clone(),
                            exit_notify_inner.clone(),
                            exit_flag_inner.clone(),
                            exit_mark_inner.clone(),
                            conn_cnt.clone(),
                        ));
                    }
                    // no more incoming connections
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(e),
                }
            }
        });

        #[cfg(target_family = "unix")]
        {
            // graceful shutdown
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
            let mut sighup =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

            // graceful shutdown handler
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sighup.recv() => {}
                _ = sigterm.recv() => {}
                res = handler => {
                    match res {
                        Ok(res) => {
                            match res {
                                Ok(()) => {}
                                Err(e) => return Err(Box::new(e))
                            };
                        }
                        Err(e) => return Err(Box::new(e)),
                    }
                }
            }
        }

        // graceful shutdown handler for windows
        #[cfg(target_family = "windows")]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            res = handler => {
                match res {
                    Ok(res) => {
                        match res {
                            Ok(()) => {}
                            Err(e) => return Err(Box::new(e))
                        };
                    }
                    Err(e) => return Err(Box::new(e)),
                }
            }
        }

        // received signal, graceful shutdown now
        info!("[VOLO] received signal, gracefully exiting now");
        *exit_flag.write() = true;
        exit_mark.store(true, Ordering::Relaxed);

        // Now we won't accept new connections.
        // And we want to send crrst reply to the peers in the short future.
        if gconn_cnt.load(Ordering::Relaxed) != 0 {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        exit_notify.notify_waiters();

        // wait for all connections to be closed
        for _ in 0..28 {
            if gconn_cnt.load(Ordering::Relaxed) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    #[cfg(feature = "multiplex")]
    /// Use multiplexing to handle multiple requests in one connection.
    ///
    /// Not recommend for most users.
    #[doc(hidden)]
    pub fn multiplex(self, multiplex: bool) -> Server<S, L, Req, MkC> {
        Server {
            layer: self.layer,
            service: self.service,
            make_codec: self.make_codec,
            _marker: PhantomData,
            multiplex,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_conn<R, W, Req, Svc, Resp, MkC>(
    rh: R,
    wh: W,
    service: Svc,
    make_codec: MkC,
    exit_notify: Arc<Notify>,
    exit_flag: Arc<parking_lot::RwLock<bool>>,
    exit_mark: Arc<std::sync::atomic::AtomicBool>,
    conn_cnt: Arc<std::sync::atomic::AtomicUsize>,
) where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    Svc: Service<ServerContext, Req, Response = Resp> + Clone + Send + 'static,
    Svc::Error: Send,
    Svc::Error: Into<crate::Error>,
    Req: EntryMessage + Send + 'static,
    Resp: EntryMessage + Send + 'static,
    MkC: MakeCodec<R, W>,
{
    // get read lock and create Notified
    let notified = {
        let r = exit_flag.read();
        if *r {
            return;
        }
        exit_notify.notified()
    };

    let (encoder, decoder) = make_codec.make_codec(rh, wh);

    tracing::trace!("[VOLO] handle conn by ping-pong");
    crate::transport::pingpong::serve(encoder, decoder, notified, exit_mark, &service).await;
    conn_cnt.fetch_sub(1, Ordering::Relaxed);
}

#[cfg(feature = "multiplex")]
#[allow(clippy::too_many_arguments)]
async fn handle_conn_multiplex<R, W, Req, Svc, Resp, MkC>(
    rh: R,
    wh: W,
    service: Svc,
    make_codec: MkC,
    exit_notify: Arc<Notify>,
    exit_flag: Arc<parking_lot::RwLock<bool>>,
    exit_mark: Arc<std::sync::atomic::AtomicBool>,
    conn_cnt: Arc<std::sync::atomic::AtomicUsize>,
) where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    Svc: Service<ServerContext, Req, Response = Resp> + Clone + Send + 'static + Sync,
    Svc::Error: Into<crate::Error> + Send,
    Req: EntryMessage + Send + 'static,
    Resp: EntryMessage + Send + 'static,
    MkC: MakeCodec<R, W>,
{
    // get read lock and create Notified
    let notified = {
        let r = exit_flag.read();
        if *r {
            return;
        }
        exit_notify.notified()
    };

    let (encoder, decoder) = make_codec.make_codec(rh, wh);

    info!("[VOLO] handle conn by multiplex");
    crate::transport::multiplex::serve(encoder, decoder, notified, exit_mark, service).await;
    conn_cnt.fetch_sub(1, Ordering::Relaxed);
}
