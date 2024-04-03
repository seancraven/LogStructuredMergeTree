use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

use actix_web::{error::ErrorInternalServerError, web};
use actix_web::{App, HttpResponse, HttpServer};
use anyhow::anyhow;
use chrono::Local;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

type State = web::Data<Mutex<Database>>;
#[tokio::main]
async fn main() {
    // let mut db = Database::new();
    // db.insert(0, String::from("dave"));
    // HttpServer::new(|| {
    //     App::new()
    //         .app_data(Mutex::new(Database::new()))
    //         .service(read)
    //         .service(insert)
    // });
}

// #[actix_web::get("/{id}")]
// async fn read(data: State, id: web::Path<usize>) -> actix_web::Result<HttpResponse> {
//     let value = data
//         .lock()
//         .unwrap()
//         .read(*id)
//         .ok_or(ErrorInternalServerError(format!("id: {:?} not found", *id)))?;
//     Ok(HttpResponse::Ok().body(value))
// }
// #[actix_web::post("/{id}")]
// async fn insert(
//     data: State,
//     id: web::Path<usize>,
//     value: web::Form<String>,
// ) -> actix_web::Result<HttpResponse> {
//     data.lock().unwrap().insert(*id, value.into_inner())?;
//     Ok(HttpResponse::Ok().finish())
// }

/// Log Structured merge tree that uses an append only
/// File system, to encode
struct Database {
    index: HashMap<usize, usize>,
    max_buff_size: usize,
    logs: Vec<PathBuf>,
    current_buffer: Vec<u8>,
}
impl Database {
    pub fn new() -> Database {
        Database {
            index: HashMap::new(),
            max_buff_size: 200,
            logs: vec![],
            current_buffer: vec![],
        }
    }
    pub fn insert<T: Serialize>(&mut self, key: usize, value: T) -> Result<(), std::io::Error> {
        let mut value_as_bytes = format!("{},", key).into_bytes();
        println! {"{:?}", value_as_bytes.clone()}
        value_as_bytes.extend(serde_json::to_vec(&value)?);
        println!("{:?}", value_as_bytes.clone());
        value_as_bytes.push(b'\n');
        println!("{:?}", value_as_bytes.clone());
        let offset = self.current_buffer.len();
        self.index.insert(key, offset);
        self.current_buffer.extend(value_as_bytes);
        if self.current_buffer.len() > self.max_buff_size {
            let path = PathBuf::from(format!("log_{:?}", Local::now().format("%d/%m%y-%H%M%S")));
            std::fs::write(path, self.current_buffer.clone())?;
            self.current_buffer = vec![];
            self.index = HashMap::new();
        }
        Ok(())
    }
    pub fn read(&self, key: usize) -> anyhow::Result<Option<String>> {
        if let Some(offset) = self.index.get(&key) {
            let mut end_index: Option<usize> = None;
            let mut start_index: Option<usize> = None;
            for (i, char) in self.current_buffer[*offset..].iter().enumerate() {
                if *char == b',' {
                    start_index = Some(i);
                    break;
                }
            }
            for (i, char) in self.current_buffer[*offset..].iter().enumerate() {
                if *char == b'\n' {
                    end_index = Some(i);
                    break;
                }
            }
            let (start_idx, end_idx) = match (start_index, end_index) {
                (Some(start), Some(end)) => (start, end),
                _ => return Err(anyhow!("Couldn't decode file")),
            };
            let object_byte_slice =
                &self.current_buffer[*offset + start_idx + 1..*offset + end_idx];
            if object_byte_slice[0] != b'\"'
                || object_byte_slice[object_byte_slice.len() - 1] != b'\"'
            {
                return Err(anyhow!("Failure to deserialize"));
            }

            return Ok(Some(String::from_utf8(
                object_byte_slice[1..object_byte_slice.len() - 1].to_vec(),
            )?));
        }
        todo!()
    }
}
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_insert() {
        let mut db = Database::new();
        assert!(db.insert(10, "Sexy").is_ok());
    }
    #[test]
    fn test_read_in_memory() {
        let mut db = Database::new();
        db.insert(10, "Sexy").unwrap();
        assert_eq!(db.read(10).unwrap().unwrap(), String::from("Sexy"));
    }
}
