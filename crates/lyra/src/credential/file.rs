//! 凭据文件的读取缓存与原子写回. 两条线路的文件格式完全相同, 区别只在字段含义与
//! 错误文本, 因此这里只实现一份.
//!
//! 文件是运行真值: 运维可以直接编辑, 下一个请求就会读到新内容. 缓存按修改时间与
//! 大小判断是否需要重新解析, 对外可观测语义与每次读取一致.
//!
//! Credential file reading cache and atomic write-back. Both routes share one file format
//! and differ only in field meaning and error text, so this is implemented once.
//!
//! The file is the runtime source of truth: operators may edit it directly and the next
//! request reads the new content. The cache decides by modification time and size whether
//! to parse again, keeping the externally observable semantics identical to reading every
//! time.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde_json::Value;
use tokio::io::AsyncWriteExt;

use super::{Entry, Error, enabled, parse_json_value};

/// 文件缓存按修改时间与大小缓存解析结果. 凭据文件很小, 但每个请求都要取号, 缓存让
/// 未变更时跳过重复解析. 它只在状态锁内访问.
///
/// The file cache keeps the parse result by modification time and size. The credential
/// file is small but every request selects an account, so caching skips repeated parsing
/// when the file is unchanged. It is accessed only under the state lock.
struct FileCache {
    valid: bool,
    /// SystemTime 没有 Default 实现, 缓存未生效时 valid 为假, 因此初值不参与比较.
    ///
    /// SystemTime has no Default implementation; the initial value never takes part in a
    /// comparison because valid is false while the cache is empty.
    mod_time: SystemTime,
    size: u64,
    entries: Vec<Entry>,
}

impl Default for FileCache {
    fn default() -> Self {
        Self {
            valid: false,
            mod_time: SystemTime::UNIX_EPOCH,
            size: 0,
            entries: Vec::new(),
        }
    }
}

/// 一条线路的凭据文件. 读取带缓存, 写回先写 .new 再原子改名.
///
/// One route's credential file. Reads are cached; a write goes to a .new file first and
/// then renames atomically.
pub struct CredentialFile {
    path: PathBuf,
    /// 读取失败消息的前缀, 两条线路使用不同的文本以便定位到具体文件.
    ///
    /// The prefix of read failure messages; the two routes use different text so the
    /// failing file is identifiable.
    read_error: &'static str,
    /// 过滤后没有启用条目时返回的错误, 两条线路的哨兵不同.
    ///
    /// The error returned when no entry is enabled after filtering; the two routes use
    /// different sentinels.
    empty_error: Error,
    cache: FileCache,
}

impl CredentialFile {
    pub fn new(path: impl Into<PathBuf>, read_error: &'static str, empty_error: Error) -> Self {
        Self {
            path: path.into(),
            read_error,
            empty_error,
            cache: FileCache::default(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 读取凭据文件. include_disabled 为假时只返回启用条目, 且没有启用条目时报错,
    /// 与改写前的 Python 实现一致.
    ///
    /// 返回的条目是缓存内容的副本, 调用方改写元素不会影响缓存.
    ///
    /// Reads the credential file. When include_disabled is false it returns only enabled
    /// entries and errors when none are enabled, matching the Python implementation
    /// before the rewrite.
    ///
    /// The returned entries are copies of the cached content, so a caller modifying an
    /// element cannot affect the cache.
    pub async fn load(&mut self, include_disabled: bool) -> Result<Vec<Entry>, Error> {
        let metadata = tokio::fs::metadata(&self.path)
            .await
            .map_err(|error| Error::message(format!("{}: {error}", self.read_error)))?;
        // Unix 上 modified() 不会失败; 取不到时退回纪元只会让缓存每次都重新解析, 不会
        // 读到过期内容.
        //
        // modified() does not fail on Unix; falling back to the epoch only makes the cache
        // re-parse every time and never serves stale content.
        let mod_time = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if !self.cache.valid || mod_time != self.cache.mod_time || metadata.len() != self.cache.size
        {
            let data = tokio::fs::read(&self.path)
                .await
                .map_err(|error| Error::message(format!("{}: {error}", self.read_error)))?;
            let entries = decode_entries(&data)?;
            self.cache = FileCache {
                valid: true,
                mod_time,
                size: metadata.len(),
                entries,
            };
        }
        if include_disabled {
            return Ok(self.cache.entries.clone());
        }
        let filtered: Vec<Entry> = self
            .cache
            .entries
            .iter()
            .filter(|entry| enabled(entry))
            .cloned()
            .collect();
        if filtered.is_empty() {
            return Err(self.empty_error.clone());
        }
        Ok(filtered)
    }

    /// 写回凭据文件并让缓存失效.
    ///
    /// Writes the credential file back and invalidates the cache.
    pub async fn store(&mut self, entries: &[Entry]) -> Result<(), Error> {
        write_entries_file(&self.path, entries).await?;
        self.cache.valid = false;
        Ok(())
    }
}

/// 原子写回凭据文件. 先用 .new 临时文件写入再改名, 避免中断后留下半截文件. 权限保持
/// 600, 与改写前的 Python 实现一致. 编码时不做 HTML 转义, 因为开启后名字里的 & 会被
/// 写成 U+0026 的转义写法, 而不是字面字符.
///
/// Atomically writes the credential file. It writes a .new temporary file first and
/// renames it, avoiding a half file after an interruption. Permissions stay 600, matching
/// the Python implementation before the rewrite. No HTML escaping is applied during
/// encoding, because with it an ampersand in a name would be written as an escaped form
/// rather than the literal character.
pub async fn write_entries_file(path: &Path, entries: &[Entry]) -> Result<(), Error> {
    let payload = Value::Object(
        [(
            "credentials".to_owned(),
            Value::Array(entries.iter().cloned().map(Value::Object).collect()),
        )]
        .into_iter()
        .collect(),
    );
    // to_string_pretty 使用两空格缩进, 与 Go 的 SetIndent("", "  ") 相同; serde_json
    // 不做 HTML 转义, 与 SetEscapeHTML(false) 相同.
    //
    // to_string_pretty indents with two spaces like Go's SetIndent("", "  "), and
    // serde_json performs no HTML escaping, like SetEscapeHTML(false).
    let mut data = serde_json::to_string_pretty(&payload)
        .map_err(|error| Error::message(format!("编码凭据文件失败: {error}")))?;
    data.push('\n');
    let temporary = temporary_path(path);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)
        .await
        .map_err(|error| Error::message(format!("写入凭据文件失败: {error}")))?;
    file.write_all(data.as_bytes())
        .await
        .map_err(|error| Error::message(format!("写入凭据文件失败: {error}")))?;
    // tokio 的文件写入在后台线程池中执行, write_all 返回时数据可能仍未写入文件, 丢弃
    // 句柄也不会等待它完成. 改名前必须 flush, 否则读者可能看到空文件; CI 在 Linux 上
    // 观察到过这种情况. flush 之后再释放句柄并改名, 与 Go 的 Close 后 Rename 等价.
    //
    // tokio performs file writes on a background thread pool: when write_all returns the
    // data may not have reached the file yet, and dropping the handle does not wait for it.
    // Flushing before the rename is required or readers can see an empty file, which CI
    // observed on Linux. Flush, then release the handle and rename, as Go closes then
    // renames.
    file.flush()
        .await
        .map_err(|error| Error::message(format!("写入凭据文件失败: {error}")))?;
    drop(file);
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|error| Error::message(format!("替换凭据文件失败: {error}")))?;
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".new");
    PathBuf::from(name)
}

/// 解析凭据文件, 兼容旧格式.
///
/// 与改写前的 Python 实现一致: 顶层是对象且有 credentials 数组时用该数组; 其余情况
/// 把整个顶层对象当作一条凭据. 数字保持原始字面量, 以保留整数字面量与小数之间的区别,
/// 这是判定 priority 是否合法的前提.
///
/// Parses the credential file and stays compatible with the old format. Like the Python
/// implementation before the rewrite: when the top level is an object with a credentials
/// array, that array is used; otherwise the whole top-level object is treated as one
/// credential. Numbers keep their original literal form, preserving the distinction
/// between integer literals and decimals, which is the precondition for judging a valid
/// priority.
pub fn decode_entries(data: &[u8]) -> Result<Vec<Entry>, Error> {
    let value = parse_json_value(data)
        .map_err(|error| Error::message(format!("解析凭据文件失败: {error}")))?;
    let Value::Object(object) = value else {
        return Err(Error::message("凭据文件顶层不是对象"));
    };
    match object.get("credentials").cloned() {
        None | Some(Value::Null) => Ok(vec![object]),
        Some(Value::Array(items)) => {
            let mut entries = Vec::with_capacity(items.len());
            for (position, item) in items.into_iter().enumerate() {
                match item {
                    Value::Object(entry) => entries.push(entry),
                    _ => return Err(Error::message(format!("第 {position} 条凭据不是对象"))),
                }
            }
            Ok(entries)
        }
        Some(_) => Err(Error::message("credentials 字段不是数组")),
    }
}
