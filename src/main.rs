use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use actix_web::{
    error::ErrorInternalServerError,
    web::{self, Data},
};
use actix_web::{App, HttpResponse, HttpServer};
use anyhow::anyhow;
use chrono::{Local, Offset};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

type State = web::Data<Mutex<Database>>;
#[tokio::main]
async fn main() {
    let mut db = Database::new();
    HttpServer::new(|| {
        App::new()
            .app_data(Mutex::new(Database::new()))
            .service(read)
            .service(insert)
    });
}

#[actix_web::get("/{id}")]
async fn read(data: State, id: web::Path<usize>) -> actix_web::Result<HttpResponse> {
    let value = data
        .lock()
        .unwrap()
        .read(*id)
        .map_err(ErrorInternalServerError)?
        .ok_or(ErrorInternalServerError(format!("id: {:?} not found", *id)))?;
    Ok(HttpResponse::Ok().body(value))
}
#[actix_web::post("/{id}")]
async fn insert(
    data: State,
    id: web::Path<usize>,
    value: web::Form<String>,
) -> actix_web::Result<HttpResponse> {
    data.lock()
        .unwrap()
        .insert(*id, value.into_inner())
        .map_err(ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().finish())
}

/// Log Structured merge tree that uses an append only
/// File system, to encode
struct Database {
    current_index: HashMap<usize, usize>,
    current_buffer: Vec<u8>,
    max_buff_size: usize,
    logs: Vec<(PathBuf, HashMap<usize, usize>)>,
    previous_compaction_size: usize,
}
impl Database {
    pub fn new() -> Database {
        let max_buf_size = 200;
        Database {
            current_index: HashMap::new(),
            max_buff_size: max_buf_size,
            logs: vec![],
            // Add extra capacity as buffer is small and objects can
            // write over, thus avoid allocation.
            current_buffer: Vec::with_capacity(2 * max_buf_size),
            previous_compaction_size: 0,
        }
    }
    pub fn insert<T: Serialize>(&mut self, key: usize, value: T) -> anyhow::Result<()> {
        let value_as_bytes = Database::to_buffer_bytes(key, value)?;
        let offset = self.current_buffer.len();
        self.current_index.insert(key, offset);
        self.current_buffer.extend(value_as_bytes);
        if self.current_buffer.len() > self.max_buff_size {
            let path = Database::new_buf_name();
            std::fs::write(path, self.current_buffer.clone())?;
            self.current_buffer = Vec::with_capacity(2 * self.max_buff_size);
            self.current_index = HashMap::new();
        }
        if self.logs.len() > 2 * self.previous_compaction_size {
            self.merge_and_compact()?;
        }
        Ok(())
    }
    fn to_buffer_bytes<T: Serialize>(key: usize, value: T) -> Result<Vec<u8>, std::io::Error> {
        let mut value_as_bytes = format!("{},", key).into_bytes();
        value_as_bytes.extend(serde_json::to_vec(&value)?);
        value_as_bytes.push(b'\n');
        Ok(value_as_bytes)
    }
    fn new_buf_name() -> PathBuf {
        PathBuf::from(format!("log_{:?}", Local::now().format("%d/%m%y-%H%M%S")))
    }
    /// Read into the databse. First checks current in memory index,
    /// Then reads through previous history maps.
    pub fn read(&self, key: usize) -> anyhow::Result<Option<String>> {
        if let Some(offset) = self.current_index.get(&key) {
            return Ok(Some(Database::read_buffer(&self.current_buffer, *offset)?));
        } else {
            for (buffer_path, map) in self.logs.iter() {
                let buf = fs::read(buffer_path)?;
                if let Some(offset) = map.get(&key) {
                    return Ok(Some(Database::read_buffer(&buf, *offset)?));
                }
            }
        };
        Ok(None)
    }
    fn read_buffer(buffer: &[u8], offset: usize) -> anyhow::Result<String> {
        let mut end_index: Option<usize> = None;
        let mut start_index: Option<usize> = None;
        for (i, char) in buffer[offset..].iter().enumerate() {
            if *char == b',' {
                start_index = Some(i);
                break;
            }
        }
        for (i, char) in buffer[offset..].iter().enumerate() {
            if *char == b'\n' {
                end_index = Some(i);
                break;
            }
        }
        let (start_idx, end_idx) = match (start_index, end_index) {
            (Some(start), Some(end)) => (start, end),
            _ => return Err(anyhow!("Couldn't decode file")),
        };
        let object_byte_slice = &buffer[offset + start_idx + 1..offset + end_idx];
        if object_byte_slice[0] != b'\"' || object_byte_slice[object_byte_slice.len() - 1] != b'\"'
        {
            return Err(anyhow!(
                "Failure to deserialize; Termination characters not found."
            ));
        }

        Ok(String::from_utf8(
            object_byte_slice[1..object_byte_slice.len() - 1].to_vec(),
        )?)
    }
    /// Implement index merging and compaction. This implementation is
    /// very slow.
    fn merge_and_compact(&mut self) -> anyhow::Result<()> {
        let mut compact_index = HashMap::new();
        for (path, map) in self.logs.iter() {
            for (map_key, offset) in map.iter() {
                if compact_index.get(map_key).is_none() && self.current_index.get(map_key).is_none()
                {
                    let buffer = fs::read(path)?;
                    let item = Database::read_buffer(&buffer, *offset)?.into_bytes();
                    compact_index.insert(*map_key, item);
                }
            }
        }
        for (key, item) in compact_index.into_iter() {
            self.insert(key, item)?
        }
        self.previous_compaction_size = self.logs.len();
        Ok(())
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
