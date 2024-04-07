use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::PathBuf,
    sync::Mutex,
};

use actix_web::{
    error::ErrorInternalServerError,
    web::{self},
};
use actix_web::{App, HttpResponse, HttpServer};
use anyhow::{anyhow, Context};
use chrono::Local;
use serde::Serialize;

type State = web::Data<Mutex<Database>>;
#[tokio::main]
async fn main() {
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
#[derive(Debug)]
/// Log Structured merge tree that uses an append only
/// File system, to encode
struct Database {
    current_index: HashMap<usize, usize>,
    current_buffer: Vec<u8>,
    max_buff_size: usize,
    logs: VecDeque<(PathBuf, HashMap<usize, usize>)>,
    previous_compaction_size: usize,
}
impl Database {
    pub fn new() -> Database {
        let max_buf_size = 200;
        Database {
            current_index: HashMap::new(),
            max_buff_size: max_buf_size,
            logs: VecDeque::new(),
            // Add extra capacity as buffer is small and objects can
            // write over, thus avoid allocation.
            current_buffer: Vec::with_capacity(max_buf_size),
            previous_compaction_size: 0,
        }
    }
    pub fn insert<T: Serialize>(&mut self, key: usize, value: T) -> anyhow::Result<()> {
        self.insert_without_compaction(key, value)?;
        if self.logs.len() > (2 * self.previous_compaction_size).max(5) {
            self.merge_and_compact()?;
        }
        Ok(())
    }
    fn insert_without_compaction<T: Serialize>(
        &mut self,
        key: usize,
        value: T,
    ) -> anyhow::Result<()> {
        let ser_value = Database::to_buffer_bytes(key, value)?;
        if let Some((log_path, old_buffer)) = self.inner_insert(key, ser_value) {
            fs::write(log_path, old_buffer)?;
        };
        Ok(())
    }

    /// Insertion function without io or fallable operations.
    /// Resets inner state and returns A path to write a log file,
    /// and the logs contents if this should happen.
    fn inner_insert(&mut self, key: usize, bytes: Vec<u8>) -> Option<(PathBuf, Vec<u8>)> {
        let offset = self.current_buffer.len();
        self.current_index.insert(key, offset);
        self.current_buffer.extend(bytes);
        if self.current_buffer.len() > self.max_buff_size {
            let log_index = std::mem::take(&mut self.current_index);
            let old_buffer = std::mem::replace(
                &mut self.current_buffer,
                Vec::with_capacity(self.max_buff_size),
            );
            let log_path = Database::new_buf_name();
            self.logs.push_back((log_path.clone(), log_index));
            return Some((log_path, old_buffer));
        };
        None
    }
    fn to_buffer_bytes<T: Serialize>(key: usize, value: T) -> Result<Vec<u8>, std::io::Error> {
        let mut value_as_bytes = format!("{},", key).into_bytes();
        value_as_bytes.extend(serde_json::to_string(&value)?.into_bytes());
        value_as_bytes.push(b'\n');
        Ok(value_as_bytes)
    }
    fn new_buf_name() -> PathBuf {
        PathBuf::from(format!(
            "log_{}_{}",
            Local::now().format("%d-%m-%Y-%H:%M:%S"),
            uuid::Uuid::new_v4()
        ))
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
        let obj_str = serde_json::from_slice::<String>(object_byte_slice)
            .context(format!("{:?}", String::from_utf8_lossy(object_byte_slice)))?;
        Ok(obj_str)
    }
    /// Implement index merging and compaction. This implementation is
    /// very slow.
    fn merge_and_compact(&mut self) -> anyhow::Result<()> {
        let mut compacted_keys: HashSet<usize> = HashSet::new();
        let old_logs = self.logs.clone();
        let mut dropped_logs = Vec::with_capacity(old_logs.len());
        for (path, map) in old_logs.iter().rev() {
            let buffer = fs::read(path)?;
            for (map_key, offset) in map.iter() {
                if !compacted_keys.contains(map_key) && self.current_index.get(map_key).is_none() {
                    let item = Database::read_buffer(&buffer, *offset)?;
                    compacted_keys.insert(*map_key);
                    self.insert_without_compaction(*map_key, item)?;
                }
            }
            dropped_logs.push(path.clone());
            std::fs::remove_file(path)?;
        }
        self.previous_compaction_size = old_logs.len();
        let new_logs = self
            .logs
            .iter()
            .filter_map(|(p, l)| {
                if !dropped_logs.contains(p) {
                    return Some((p.clone(), l.clone()));
                }
                None
            })
            .collect();

        self.logs = new_logs;
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};

    use super::*;

    impl Drop for Database {
        fn drop(&mut self) {
            println!("dropping db");
            for (path, _) in self.logs.iter() {
                if path.exists() {
                    std::fs::remove_file(path).unwrap();
                }
            }
            for (path, _) in self.logs.iter() {
                assert!(!path.exists());
            }
        }
    }

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
    fn setup_in_memory_insert_check() -> (Database, usize, Vec<u8>, usize, Vec<u8>) {
        let db = Database::new();
        let key = 0;
        let fail_key = 69;
        let value = Database::to_buffer_bytes(key, "VerySexy").unwrap();
        let fail_value = Database::to_buffer_bytes(fail_key, "NotSexy").unwrap();
        (db, key, value, fail_key, fail_value)
    }
    #[test]
    fn test_read_out_memory_log_append() {
        let (mut db, _, _, fail_key, fail_value) = setup_in_memory_insert_check();
        while db.inner_insert(fail_key, fail_value.clone()).is_none() {}
        assert_eq!(db.logs.len(), 1);
    }
    #[test]
    fn test_read_out_memory_index_is_reset() {
        let (mut db, key, value, fail_key, fail_value) = setup_in_memory_insert_check();
        db.inner_insert(key, value);
        while db.inner_insert(fail_key, fail_value.clone()).is_none() {}
        assert!(db.current_index.get(&key).is_none());
    }
    #[test]
    fn test_read_out_memory_current_index() {
        let (mut db, _, _, fail_key, fail_value) = setup_in_memory_insert_check();
        while db.inner_insert(fail_key, fail_value.clone()).is_none() {}
        db.inner_insert(fail_key, fail_value);
        let mut expected_current_index_state = HashMap::new();
        expected_current_index_state.insert(fail_key, 0);
        assert_eq!(expected_current_index_state, db.current_index);
    }
    #[test]
    fn test_insert_full() {
        let mut db = Database::new();
        let key = 0;
        let value = String::from("Sexy");
        while db.logs.is_empty() {
            db.insert(key, value.clone()).unwrap();
        }
    }
    #[test]
    fn test_read_out_memory() {
        let (mut db, key, _, fail_key, _) = setup_in_memory_insert_check();
        let value = String::from("Sexy");
        let fail_value = String::from("NotSexy");
        db.insert(key, value.clone()).unwrap();
        while db.logs.is_empty() {
            db.insert(fail_key, fail_value.clone()).unwrap();
        }
        let item = db.read(key).unwrap().unwrap();
        assert_eq!(item, value);
    }
    #[test]
    fn test_random_read_write() {
        let mut buf: Vec<u8> = vec![];
        let mut solutions = vec![];
        for i in 0..100 {
            let key = buf.len();
            let value = format!("Item {}", i);
            solutions.push((key, value.clone()));
            let bytes = Database::to_buffer_bytes(key, value).unwrap();
            buf.extend(bytes);
        }
        for (offset, sol) in solutions {
            assert_eq!(sol, Database::read_buffer(&buf, offset).unwrap());
        }
    }
    #[test]
    fn test_single_compaction_reads() {
        let (mut db, key, _, fail_key, _) = setup_in_memory_insert_check();
        let value = String::from("Sexy");
        let fail_value = String::from("NotSexy");
        db.insert(key, value.clone()).unwrap();
        while db.previous_compaction_size != 0 {
            db.insert(fail_key, fail_value.clone()).unwrap();
        }
        assert_eq!(db.read(key).unwrap().unwrap(), value);
    }
    #[test]
    fn test_throughput() {
        let mut db = Database::new();
        let value = String::from("Sexy");
        for i in 0..1000 {
            db.insert(i, value.clone()).unwrap();

            // println!("{:?}", db);
        }
    }
    #[test]
    fn test_key_retention() {
        let mut db = Database::new();
        let value = String::from("Sexy");
        for i in 0..1000 {
            db.insert(i, value.clone()).unwrap();
            db.read(i).unwrap().unwrap();
        }
        for i in 0..1000 {
            assert!(db.read(i).unwrap().is_some(), "Failed on {}", i);
        }
    }
    #[test]
    fn test_multi_compaction_reads() {
        let mut rng = thread_rng();
        let (mut db, key, _, _, _) = setup_in_memory_insert_check();
        let value = String::from("Sexy");
        db.insert(key, value.clone()).unwrap();
        let mut fail = vec![];
        for i in 0..40 {
            fail.push(i);
        }
        let fail_value = String::from_utf8_lossy(&fail);
        // Force compaction event.
        for _ in 0..3 {
            while db.logs.len() < 2 {
                db.insert(rng.gen_range(1..20), fail_value.clone()).unwrap();
            }
            db.merge_and_compact().unwrap();
        }

        assert_eq!(db.read(key).unwrap().unwrap(), value);
    }
    #[test]
    /// Check compaction still maintains lifo status.
    fn test_multi_compaction_correctness() {
        let mut rng = thread_rng();
        let (mut db, key, _, _, _) = setup_in_memory_insert_check();
        let mut fail = vec![];
        for i in 0..40 {
            fail.push(i);
        }
        let fail_value = String::from_utf8_lossy(&fail);
        // Force compaction event.
        while db.logs.is_empty() {
            db.insert(rng.gen_range(1..20), fail_value.clone()).unwrap();
        }
        db.insert(key, String::from("Ultra Sexy")).unwrap();
        db.merge_and_compact().unwrap();

        // Force write to disk
        while db.current_index.get(&key).is_some() {
            db.insert(rng.gen_range(1..20), fail_value.clone()).unwrap();
        }
        // Check correct read from disk on compacted file.
        assert_eq!(db.read(key).unwrap().unwrap(), String::from("Ultra Sexy"));
    }
}
