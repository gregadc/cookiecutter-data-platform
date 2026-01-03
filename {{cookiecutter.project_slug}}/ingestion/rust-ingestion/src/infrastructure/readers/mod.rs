mod csv_reader;
mod s3_reader;

pub use csv_reader::read_csv_local;
pub use infrastructure::s3_reader::read_csv_s3;
