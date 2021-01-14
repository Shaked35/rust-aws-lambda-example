#![recursion_limit = "256"]

pub mod error;

use arrow::json;
use lambda_runtime::error::HandlerError;
use std::io::Read;
use rusoto_s3::{S3, S3Client, PutObjectRequest, PutObjectError, DeleteObjectRequest, DeleteObjectOutput, DeleteObjectError};
use crate::consts::{BUCKET, CONSTRAINTS_TABLE_KEY_FIELD, DYNAMO_MAX_BATCH_WRITE_ITEM};
use std::collections::{LinkedList, HashMap};
use std::time::Instant;
use csv::Reader;
use std::error::Error;
use std::rc::Rc;
use parquet::schema::parser::parse_message_type;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use parquet::file::writer::{SerializedFileWriter, FileWriter};
use parquet::column::writer::ColumnWriter;
use rusoto_core::{RusotoError, RusotoFuture};
use rusoto_core::ByteStream;
use std::path::PathBuf;
use rustc_serialize::json::{Encoder, Json};
use rustc_serialize::json::Json::Object;
use rustc_serialize::Encodable;
use rusoto_dynamodb::{DynamoDb, AttributeValue, DynamoDbClient, ScanInput, GetItemInput, PutRequest, WriteRequest, BatchWriteItemInput, BatchWriteItemOutput, BatchWriteItemError};
use crate::report_types::{GoogleReport, AdwordsConfiguration, ReportConfig};
use crate::utils::error::AdwordsError;
use inflector::Inflector;
use tempfile::NamedTempFile;
use std::io;
use flate2::read::GzDecoder;
use std::borrow::Borrow;


pub fn s3_get_file_names(s3: &rusoto_s3::S3Client, folder_name: String) -> LinkedList<String> {
    let request = rusoto_s3::ListObjectsRequest {
        bucket: BUCKET.to_owned(),
        prefix: Option::from(folder_name),
        ..Default::default()
    };
    let response = s3.list_objects(request);
    let list_output = response.sync()
        .map_err(|_| "could not get response from get object request");
    let list_of_objects = list_output.unwrap_or_default();
    let list_of_files = list_of_objects.contents;
    let mut list_of_file_names: LinkedList<String> = LinkedList::new();
    for file in list_of_files.unwrap() {
        list_of_file_names.push_front(file.key.unwrap());
    }

    list_of_file_names
}

pub fn get_s3_file_reader(s3: &S3Client, path: String, bucket: String, file_delimiter: char) -> Reader<impl Read + Send> {
    let start = Instant::now();
    let reader = s3.get_object(rusoto_s3::GetObjectRequest {
        bucket: bucket.to_owned(),
        key: path.to_owned(),
        ..Default::default()
    }).sync()
        .expect(&format!("could not find resource: {}", path))
        .body.expect("has no body")
        .into_blocking_read();

    let mut d = GzDecoder::new(reader).unwrap();
    println!("got reader, took: {:?}", start.elapsed());
    csv::ReaderBuilder::new()
        .delimiter(file_delimiter as u8)
        .from_reader(d)
}


pub fn write_parquet(writer: &mut SerializedFileWriter<File>, columns: Vec<Vec<Option<String>>>) -> Result<(), Box<dyn Error>> {
    let mut row_group_writer = writer.next_row_group()?;
    let mut col_number = 0;
    while let Some(mut col_writer) = row_group_writer.next_column()? {
        let column = columns.get(col_number).unwrap();
        col_number = col_number + 1;
        match col_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                let mut vals = vec![];
                let mut levels: Vec<i16> = vec![];
                column.iter().for_each(|v| vals.push(parquet::data_type::ByteArray::from(v.clone().unwrap().as_str())));
                column.iter().for_each(|v| levels.push((1)));
                typed_writer.write_batch(
                    &vals, Some(levels.as_slice()), Some(levels.as_slice()))?;
                row_group_writer.close_column(col_writer)?;
            }
            ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                let mut vals: Vec<i32> = vec![];
                let mut levels: Vec<i16> = vec![];
                column.iter().for_each(|mut v| {
                    let string = v.clone().unwrap_or(String::from("0"));
                    if string.eq(" --") || string.eq("") {
                        vals.push(0)
                    } else {
                        vals.push(string.parse::<i32>().unwrap())
                    }
                });
                column.iter().for_each(|v| levels.push((1)));

                typed_writer.write_batch(
                    &vals, Some(levels.as_slice()), Some(levels.as_slice()),
                )?;
                row_group_writer.close_column(col_writer)?;
            }
            ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                let mut vals: Vec<i64> = vec![];
                let mut levels: Vec<i16> = vec![];
                column.iter().for_each(|mut v| {
                    let string = v.clone().unwrap_or(String::from("0"));
                    if string.eq(" --") || string.eq("") {
                        vals.push(0)
                    } else {
                        vals.push(string.parse::<i64>().unwrap())
                    }
                });
                column.iter().for_each(|v| levels.push((1)));

                typed_writer.write_batch(
                    &vals, Some(levels.as_slice()), Some(levels.as_slice()),
                )?;
                row_group_writer.close_column(col_writer)?;
            }
            ColumnWriter::FloatColumnWriter(ref mut typed_writer) => {
                let mut vals: Vec<f32> = vec![];
                let mut levels: Vec<i16> = vec![];
                column.iter().for_each(|mut v| {
                    let string = v.clone().unwrap_or(String::from("0"));
                    if string.eq(" --") || string.eq("") {
                        vals.push(0.0)
                    } else if string.contains("%") {
                        if string.eq("< 10%") {
                            vals.push(0.05)
                        } else if string.eq("> 90%") {
                            vals.push(0.95)
                        } else {
                            let percent = string.replace("%", "");
                            vals.push((percent.parse::<f32>().unwrap()) / 100.0)
                        }
                    } else {
                        vals.push(string.parse::<f32>().unwrap())
                    }
                });
                column.iter().for_each(|v| levels.push((1)));

                typed_writer.write_batch(
                    &vals, Some(levels.as_slice()), Some(levels.as_slice()),
                )?;
                row_group_writer.close_column(col_writer)?;
            }
            _ => {}
        }
    }
    writer.close_row_group(row_group_writer)?;

    Ok(())
}

pub fn create_parquet_writer(schema: &String, tmp_file_path: String) -> Result<SerializedFileWriter<File>, Box<dyn Error>> {
    let schema = Rc::new(parse_message_type(&schema)?);
    let props = Rc::new(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_data_pagesize_limit(1024 * 8)
        .set_max_row_group_size(1024 * 1024 * 1024)
        .set_write_batch_size(1024 * 1024)
        .build());
    let file = File::create(tmp_file_path)?;
    let writer = SerializedFileWriter::new(file, schema, props)?;
    Ok(writer)
}


pub fn s3_upload_from_vec(s3: &S3Client, in_mem_file: Vec<u8>, s3_bucket: String, s3_path: String) -> Result<(), RusotoError<PutObjectError>> {
    let body = ByteStream::from(in_mem_file);
    let put_object_request = PutObjectRequest { bucket: s3_bucket, key: s3_path, body: Some(body), ..Default::default() };
    s3.put_object(put_object_request).sync()?;
    Ok(())
}


pub fn s3_delete_file(s3: &S3Client, s3_bucket: String, s3_path: String) -> RusotoFuture<DeleteObjectOutput, DeleteObjectError> {
    s3.delete_object(DeleteObjectRequest {
        bucket: s3_bucket,
        bypass_governance_retention: None,
        key: s3_path,
        mfa: None,
        request_payer: None,
        version_id: None,
    })
}

pub fn push_raw_values(row: &mut HashMap<String, String>, fields: Vec<String>) -> Vec<String> {
    let mut v = vec![];
    for field in fields.iter() {
        let value = row.get_mut(field).unwrap_or(&mut String::from("")).to_string();
        v.push(value);
    }
    v
}

pub fn get_number_of_fields<S: rustc_serialize::Encodable>(s: &mut S) -> usize {
    let json = get_struct_as_json(s);
    let mut counter = 0;
    if let Object(object) = json {
        counter = object.keys().count();
    }
    counter
}

pub fn get_struct_as_json<S: rustc_serialize::Encodable>(s: &mut S) -> Json {
    let mut json = "".to_owned();
    {
        let mut encoder = Encoder::new(&mut json);
        s.encode(&mut encoder).unwrap();
    }
    let json = Json::from_str(&json).unwrap();
    json
}


pub fn get_all_accounts(dynamo: &DynamoDbClient, accounts_table: String) -> Result<HashMap<String, Vec<i64>>, Box<dyn Error>> {
    let mut af_accounts = vec![];
    let mut not_af_accounts = vec![];
    let scanner_object = ScanInput { table_name: accounts_table, ..Default::default() };
    let scanner = dynamo.scan(scanner_object).sync()?;
    let accounts = scanner.items.unwrap();
    for account in accounts.iter().to_owned() {
        let account_id = account.get("account_id").unwrap().s.as_ref().unwrap().to_string();
        let is_af = account.get("is_af").unwrap().to_owned().s.as_ref().unwrap().to_string();
        if is_af == "true" {
            af_accounts.push(account_id.parse::<i64>().unwrap());
        } else {
            not_af_accounts.push(account_id.parse::<i64>().unwrap());
        }
    }
    let mut accounts_map: HashMap<String, Vec<i64>> = HashMap::new();
    accounts_map.insert("af".to_string(), af_accounts);
    accounts_map.insert("not_af".to_string(), not_af_accounts);
    println!("{:?}", accounts_map);
    Ok(accounts_map)
}


pub fn get_website(dynamo: &DynamoDbClient, accounts_table: String, account_id: String) -> Result<String, Box<dyn Error>> {
    let mut filter = HashMap::with_capacity(1);
    filter.insert(CONSTRAINTS_TABLE_KEY_FIELD.to_owned(), AttributeValue { s: Some(account_id), ..Default::default() });
    let scanner_object = GetItemInput { key: filter, table_name: accounts_table, ..Default::default() };
    let scanner = dynamo.get_item(scanner_object).sync()?;
    let account = scanner.item.unwrap();
    Ok(account.get("website").unwrap().to_owned().s.unwrap_or("".to_string()))
}


pub fn record_to_write_request<S: rustc_serialize::Encodable>(record: &mut S) -> Result<WriteRequest, Box<dyn Error>> {
    let item = convert_to_dynamo_item(record)?;
    let put_request = PutRequest { item };
    Ok(WriteRequest { delete_request: None, put_request: Some(put_request) })
}

pub fn write_requests_to_batch_write_request(write_requests: Vec<WriteRequest>, table_name: String) -> BatchWriteItemInput {
    let mut items = HashMap::new();
    items.insert(table_name, write_requests);
    BatchWriteItemInput { request_items: items, ..Default::default() }
}

fn convert_to_dynamo_item<S: rustc_serialize::Encodable>(record: &mut S) -> Result<HashMap<String, AttributeValue>, Box<dyn Error>> {
    let mut row: HashMap<String, AttributeValue> = HashMap::new();
    let mut json = "".to_owned();
    {
        let mut encoder = Encoder::new(&mut json);
        record.encode(&mut encoder).unwrap();
    }
    let json = Json::from_str(&json).unwrap();
    if let Object(object) = json {
        for (key, field) in object.iter() {
            let value = field.to_string().replace("\"", "");
            row.insert(key.to_string().replace("\"", ""), AttributeValue { s: Some(value), ..Default::default() });
        }
    }
    Ok(row)
}

pub fn upload_dynamo_requests(insert_futures: Vec<RusotoFuture<BatchWriteItemOutput, BatchWriteItemError>>) {
    for result in insert_futures {
        let output = result.sync().unwrap();
        if let Some(unprocessed) = output.to_owned().unprocessed_items {
            if !unprocessed.is_empty() {}
        }
    }
}

pub fn ask_dynamo_batch_requests(dynamo: &DynamoDbClient, table_name: &str, mut write_requests: &mut Vec<WriteRequest>, insert_futures: &mut Vec<RusotoFuture<BatchWriteItemOutput, BatchWriteItemError>>) {
    let batch_write_request = write_requests_to_batch_write_request(write_requests.to_owned(), table_name.to_string());
    let batch_write_result = dynamo.batch_write_item(batch_write_request);
    insert_futures.push(batch_write_result);
}

pub fn record_to_dynamo_request<S: rustc_serialize::Encodable>(record: &mut S, dynamo: &DynamoDbClient, table_name: &str, write_requests: &mut Vec<WriteRequest>, mut insert_futures: &mut Vec<RusotoFuture<BatchWriteItemOutput, BatchWriteItemError>>) {
    let write_request = record_to_write_request(record).unwrap();
    write_requests.push(write_request);
    if write_requests.len() == DYNAMO_MAX_BATCH_WRITE_ITEM as usize {
        ask_dynamo_batch_requests(dynamo, table_name, write_requests, &mut insert_futures);
        write_requests.clear();
    }
}

pub fn normalize_name(head: &str) -> String {
    head.to_string()
        .to_lowercase()
        .to_snake_case()
        .replace(".", "")
        .replace(")", "")
        .replace("(", "")
        .replace("%", "")
        .replace("$", "")
        .replace("+", "")
        .replace("?", "")
        .replace("/", "")
        .replace("\\", "")
        .replace("}", "")
        .replace("{", "")
}

pub fn parse_adwords_configuration(mut response: &mut (impl Read + Send)) -> AdwordsConfiguration {
    let mut configuration = NamedTempFile::new().unwrap();
    io::copy(&mut response, &mut configuration).unwrap();
    let adwords_bi_configuration: AdwordsConfiguration = ::serde_json::from_reader(File::open(configuration.path()).unwrap()).unwrap();
    configuration.close();
    adwords_bi_configuration
}

pub fn parse_report_config(mut response: &mut (impl Read + Send)) -> ReportConfig {
    let mut configuration = NamedTempFile::new().unwrap();
    io::copy(&mut response, &mut configuration).unwrap();
    let adwords_bi_configuration: ReportConfig = ::serde_json::from_reader(File::open(configuration.path()).unwrap()).unwrap();
    configuration.close();
    adwords_bi_configuration
}


pub fn get_s3_json_object(s3: &S3Client, s3_path: String, bucket: String) -> impl Read + Send {
    let mut response = s3.get_object(rusoto_s3::GetObjectRequest {
        bucket,
        key: s3_path,
        ..Default::default()
    }).sync().unwrap().body.expect("").into_blocking_read();
    response
}


