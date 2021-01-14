use std::error::Error;

use lambda_runtime::error::HandlerError;

impl From<AdwordsError> for HandlerError {
    fn from(adwords_error: AdwordsError) -> Self {
        HandlerError::new(adwords_error)
    }
}


#[derive(Debug)]
pub struct AdwordsError {
    message: String,
}

impl AdwordsError {
    pub fn new(message: String) -> AdwordsError {
        AdwordsError { message }
    }
}

impl From<&str> for AdwordsError {
    fn from(s: &str) -> Self {
        AdwordsError::new(s.to_string())
    }
}

impl From<String> for AdwordsError {
    fn from(s: String) -> Self {
        AdwordsError::new(s)
    }
}

impl std::fmt::Display for AdwordsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "error occurred: {}", self.message)
    }
}

impl Error for AdwordsError {}

impl lambda_runtime::error::LambdaErrorExt for AdwordsError {
    fn error_type(&self) -> &str {
        "AdwordsError"
    }
}
