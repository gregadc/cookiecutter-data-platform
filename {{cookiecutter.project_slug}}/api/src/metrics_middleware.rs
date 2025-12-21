use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures::future::{ok, Ready};
use prometheus::{Counter, CounterVec, Histogram, HistogramOpts, Opts, Registry};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Instant;

#[derive(Clone)]
pub struct MetricsMiddleware {
    registry: Registry,
    incoming_requests: Counter,
    response_codes: CounterVec,
    response_time: Histogram,
}

impl MetricsMiddleware {
    pub fn new(registry: Registry) -> Self {
        let incoming_requests = Counter::with_opts(Opts::new(
            "crypto_api_incoming_requests",
            "Total number of incoming HTTP requests",
        ))
        .unwrap();

        let response_codes = CounterVec::new(
            Opts::new(
                "crypto_api_response_code",
                "HTTP response codes of the API",
            ),
            &["status"],
        )
        .unwrap();

        let response_time = Histogram::with_opts(
            HistogramOpts::new(
                "crypto_api_response_time_seconds",
                "Response time per request",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
        )
        .unwrap();

        registry.register(Box::new(incoming_requests.clone())).unwrap();
        registry.register(Box::new(response_codes.clone())).unwrap();
        registry.register(Box::new(response_time.clone())).unwrap();

        Self {
            registry,
            incoming_requests,
            response_codes,
            response_time,
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl<S, B> Transform<S, ServiceRequest> for MetricsMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>
        + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = MetricsMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(MetricsMiddlewareService {
            service: Rc::new(service),
            incoming_requests: self.incoming_requests.clone(),
            response_codes: self.response_codes.clone(),
            response_time: self.response_time.clone(),
        })
    }
}

pub struct MetricsMiddlewareService<S> {
    service: Rc<S>,
    incoming_requests: Counter,
    response_codes: CounterVec,
    response_time: Histogram,
}

impl<S, B> Service<ServiceRequest> for MetricsMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error>
        + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = actix_web::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(
        &self,
        ctx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        self.incoming_requests.inc();

        let start = Instant::now();
        let service = self.service.clone();
        let response_codes = self.response_codes.clone();
        let response_time = self.response_time.clone();

        Box::pin(async move {
            let res = service.call(req).await?;

            response_codes
                .with_label_values(&[&res.status().as_u16().to_string()])
                .inc();

            response_time.observe(start.elapsed().as_secs_f64());

            Ok(res)
        })
    }
}
