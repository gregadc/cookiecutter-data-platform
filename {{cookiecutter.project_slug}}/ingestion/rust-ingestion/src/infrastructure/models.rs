use crate::api::provider::MarketData;

#[derive(Debug)]
pub struct MarketDataWithSymbol {
    pub symbol: String,
    pub data: MarketData,
}
