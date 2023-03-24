use futures::Future;
pub use motore::service::*;

pub trait MakeService<Resp> {
    type Service;
    type Error;

    type Future: Future<Output = Result<Self::Service, Self::Error>>;

    fn make_service(&self) -> Self::Future;
}
