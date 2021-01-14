use lambda_runtime::error::HandlerError;
use lambda_runtime::Handler;
use lambda_runtime::lambda;
use serde_json::Value;
use serde::ser::{Serialize, Serializer, SerializeStruct};
use serde::Deserialize;
use rusoto_dynamodb::DynamoDbClient;
use rusoto_core::Region;
use rust_aws_lambda_example::utils::*;

struct Lambda {
    s3_ireland: rusoto_s3::S3Client,
    s3_london: rusoto_s3::S3Client,
    sqs: rusoto_sqs::SqsClient,
    athena: rusoto_athena::AthenaClient,
    dynamo: DynamoDbClient,
}


#[derive(Deserialize)]
struct Request {
    test_name: Option<String>
}


impl Lambda {
    fn new() -> Lambda {
        let s3_ireland = rusoto_s3::S3Client::new(rusoto_core::region::Region::EuWest1);
        let s3_london = rusoto_s3::S3Client::new(rusoto_core::region::Region::EuWest2);
        let sqs = rusoto_sqs::SqsClient::new(rusoto_core::region::Region::EuWest2);
        let athena = rusoto_athena::AthenaClient::new("eu-west-2".parse().unwrap());
        let dynamo = DynamoDbClient::new(Region::default());
        Lambda { s3_ireland, s3_london, sqs, athena, dynamo }
    }
}


impl Handler<serde_json::Value, (), HandlerError> for Lambda {
    fn run(&mut self, event: serde_json::Value, _ctx: lambda_runtime::Context) -> Result<(), HandlerError> {
        let request = serde_json::from_value::<Request>(event).unwrap();
        self.my_lambda_function(request);
        do_something();
        Ok(())
    }
}


fn main() {
    lambda!(Lambda::new());
}


impl Lambda {
    fn my_lambda_function(&mut self, request: Request) {
        println!("test my lambda function with {:?}", request.test_name.unwrap_or_default())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::Context;

    #[test]
    fn lambda_test() {
        let json_string =
            r#"{
                "test_name": "test_1"
                     }"#
            ;
        let res = serde_json::from_str(json_string);
        if res.is_ok() {
            let event: Value = res.unwrap();
            let conx: Context = Default::default();
            let mut lambda = Lambda::new();
            lambda.run(event.clone(), conx);
        }
    }
}