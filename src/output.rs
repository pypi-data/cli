use std::borrow::Cow;
use std::io::Write;
use std::path::PathBuf;

use serde::Serialize;

// use serde_with::{DisplayFromStr, serde_as};

static BUFFER_SIZE: usize = 1024 * 1024 * 50;

#[derive(Debug, Clone)]
pub enum OutputMode {
    Json,
    Directory(PathBuf),
}

#[derive(Serialize)]
pub struct Payload<'a> {
    pub oid: String,
    pub path: PathBuf,
    pub contents: Cow<'a, str>,
}

pub struct OutputDriver {
    mode: OutputMode,
    buffer: Vec<u8>,
    pub matches: usize
}

impl OutputDriver {
    pub fn new(mode: OutputMode) -> Self {
        let capacity = match mode {
            OutputMode::Json => BUFFER_SIZE,
            OutputMode::Directory(_) => 0,
        };

        OutputDriver {
            mode,
            buffer: Vec::with_capacity(capacity),
            matches: 0
        }
    }

    #[inline(always)]
    pub fn push(&mut self, item: Payload) -> anyhow::Result<()> {
        self.matches += 1;
        match &self.mode {
            OutputMode::Json => {
                let json = serde_json::to_vec(&item)?;
                self.buffer.extend(json);
                self.buffer.push(b'\n');
                if self.buffer.len() >= BUFFER_SIZE {
                    self.flush()?;
                }
            }
            OutputMode::Directory(dir) => {
                let first_component = &item.oid[0..=2];
                let second_component = &item.oid[3..=4];
                let output_dir = dir.join(first_component).join(second_component);
                std::fs::create_dir_all(&output_dir)?;
                let mut file = std::fs::File::create(output_dir.join(item.oid))?;
                file.write_all(item.contents.as_bytes())?;
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if let OutputMode::Json = self.mode {
            let mut locked = std::io::stdout().lock();
            locked.write_all(&self.buffer)?;
            drop(locked);
            self.buffer.clear();
        }
        Ok(())
    }
}
