//! 候选凭据文件的路径校验与原子发布.
//!
//! 校验分三步: 路径必须能解析出存在的父目录, 不能指向受保护的账号池路径, 且目标当前不存在.
//! 发布先写同目录的临时文件, 再用硬链接挂到目标名上: 硬链接在目标已存在时必定失败, 因此竞态
//! 期间出现的文件不会被覆盖, 中断也只留下临时文件而不会留下半截候选文件.
//!
//! Candidate credential file path validation and atomic publication.
//!
//! Validation has three steps: the path must resolve to an existing parent directory, it must not
//! point at a protected pool path, and the target must not exist. Publication writes a temporary
//! file in the same directory and hard-links it to the target name: a hard link always fails when
//! the target exists, so a file appearing during the race is never overwritten and an interruption
//! leaves a temporary file rather than a half-written candidate.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

/// 候选文件权限: 只允许属主读写, 与 Go 版 CreateTemp 的默认权限相同.
///
/// The candidate file permission: owner read and write only, matching the default of Go's
/// CreateTemp.
const CANDIDATE_MODE: u32 = 0o600;

/// 校验输出路径并返回可以直接发布的绝对路径.
///
/// 父目录必须已经存在: 命令不会隐式创建目录, 因为候选文件的目录由使用者选择, 静默创建会把
/// 凭据写到非预期的位置.
///
/// Validates the output path and returns an absolute path ready for publication. The parent
/// directory must already exist: the command never creates one implicitly, because the caller
/// chooses the directory and silent creation would put credentials somewhere unintended.
pub(crate) async fn candidate_path(path: &Path, protected: &[PathBuf]) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty() {
        return Err("必须用 --out 指定独立候选文件，不能使用生产池作为输出".to_owned());
    }
    let Some(absolute) = resolve_with_existing_parent(path).await else {
        return Err("候选文件父目录不可用".to_owned());
    };
    for pool in protected {
        if let Some(pool) = resolve_best_effort(pool).await
            && pool == absolute
        {
            return Err("候选文件不能写入生产凭据路径".to_owned());
        }
    }
    // 用 symlink_metadata 而不是 metadata: 指向既存文件的符号链接同样必须被拒绝, 而 metadata 会
    // 跟随链接把它当成那个既存文件.
    //
    // symlink_metadata rather than metadata: a symbolic link to an existing file must be rejected as
    // well, while metadata would follow the link and treat it as that existing file.
    match tokio::fs::symlink_metadata(&absolute).await {
        Ok(_) => Err("候选文件已存在，拒绝覆盖".to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(absolute),
        Err(error) => Err(format!("读取候选文件状态失败: {error}")),
    }
}

/// 把候选路径解析成绝对路径, 并解析父目录上的符号链接; 父目录不存在时返回 None.
///
/// 候选文件的父目录必须已经存在, 因此在授权开始之前就能发现无法保存的输出. 解析父目录的另一
/// 个作用是让指向同一目录的别名路径与规范路径比较结果一致, 否则一条符号链接就能绕过受保护
/// 路径的限制.
///
/// Resolves the candidate path to an absolute one and follows symbolic links on its parent
/// directory, returning None when the parent does not exist. The parent of a candidate must already
/// exist, so an unsavable output is found before authorization starts. Resolving the parent also
/// makes an alias of the same directory compare equal to its canonical form; otherwise one
/// symbolic link would bypass the protected path check.
async fn resolve_with_existing_parent(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    let name = absolute.file_name()?.to_owned();
    let parent = tokio::fs::canonicalize(absolute.parent()?).await.ok()?;
    Some(parent.join(name))
}

/// 以尽力而为的方式解析受保护路径: 父目录不存在时只做绝对化.
///
/// 受保护的账号池路径可能尚未创建, 因此不能要求它存在; 解析不到符号链接时至少要保证与同一
/// 绝对路径相等.
///
/// Resolves a protected path on a best-effort basis: when the parent does not exist it is only made
/// absolute. A protected pool path may not be created yet and must not be required to exist; when
/// symbolic links cannot be resolved, equality with the same absolute path must still hold.
async fn resolve_best_effort(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    let name = absolute.file_name()?.to_owned();
    let parent = match tokio::fs::canonicalize(absolute.parent()?).await {
        Ok(parent) => parent,
        Err(_) => absolute.parent()?.to_path_buf(),
    };
    Some(parent.join(name))
}

/// 原子发布候选文件: 内容是一个 credentials 数组, 只有一条记录.
///
/// 写入在临时文件上完成并落盘之后再发布, 因此目标路径要么不存在, 要么是完整内容.
///
/// Publishes a candidate file atomically: the content is a credentials array holding exactly one
/// record. The write completes and is flushed to disk on a temporary file before publication, so
/// the target path either does not exist or holds complete content.
pub(crate) async fn write_candidate(
    path: &Path,
    entry: Map<String, Value>,
    protected: &[PathBuf],
    temporary_prefix: &str,
) -> Result<PathBuf, String> {
    let target = candidate_path(path, protected).await?;
    let payload = Value::Object(
        [(
            "credentials".to_owned(),
            Value::Array(vec![Value::Object(entry)]),
        )]
        .into_iter()
        .collect(),
    );
    let mut data = serde_json::to_string_pretty(&payload)
        .map_err(|error| format!("编码候选文件失败: {error}"))?;
    data.push('\n');

    let directory = target
        .parent()
        .ok_or_else(|| "候选文件路径没有父目录".to_owned())?;
    let temporary = directory.join(format!(
        "{temporary_prefix}{}",
        crate::antigravity::provider::new_identifier()
    ));
    let result = write_and_publish(&temporary, &target, data.as_bytes()).await;
    // 临时文件无论成功与否都清理: 失败时留下它只会污染候选目录.
    //
    // The temporary file is removed either way: on failure it would only litter the candidate
    // directory.
    let _ = tokio::fs::remove_file(&temporary).await;
    result.map(|()| target)
}

async fn write_and_publish(temporary: &Path, target: &Path, data: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(CANDIDATE_MODE)
        .open(temporary)
        .map_err(|error| format!("创建候选文件失败: {error}"))?;
    file.write_all(data)
        .map_err(|error| format!("写入候选文件失败: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("保存候选文件失败: {error}"))?;
    drop(file);
    tokio::fs::hard_link(temporary, target)
        .await
        .map_err(|error| format!("发布候选文件失败，未覆盖目标: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> tempfile::TempDir {
        match tempfile::TempDir::new() {
            Ok(dir) => dir,
            Err(error) => panic!("创建临时目录失败: {error}"),
        }
    }

    fn entry(name: &str) -> Map<String, Value> {
        match serde_json::json!({"name": name, "access_token": "synthetic"}) {
            Value::Object(entry) => entry,
            _ => Map::new(),
        }
    }

    async fn write(path: &Path, protected: &[PathBuf]) -> Result<PathBuf, String> {
        write_candidate(path, entry("synthetic"), protected, ".candidate-").await
    }

    #[tokio::test]
    async fn protected_paths_and_existing_files_are_never_overwritten() {
        let dir = temp_dir();

        // 1. 已存在的普通文件与符号链接不得覆盖.
        let path = dir.path().join("existing.json");
        if let Err(error) = std::fs::write(&path, "preserve") {
            panic!("写入夹具失败: {error}");
        }
        let link = dir.path().join("linked.json");
        if let Err(error) = std::os::unix::fs::symlink(&path, &link) {
            panic!("创建符号链接失败: {error}");
        }
        for target in [&path, &link] {
            let error = match write(target, &[]).await {
                Ok(path) => panic!("已存在的目标被覆盖: {}", path.display()),
                Err(error) => error,
            };
            assert!(error.contains("已存在"), "{error}");
        }
        match std::fs::read(&path) {
            Ok(content) if content == b"preserve" => {}
            other => panic!("既有内容被改动: {other:?}"),
        }

        // 2. 生产凭据池路径包括别名目录符号链接必须被拒绝.
        let pool = dir.path().join("credentials.json");
        let alias = dir.path().join("alias");
        if let Err(error) = std::os::unix::fs::symlink(dir.path(), &alias) {
            panic!("创建符号链接失败: {error}");
        }
        for target in [pool.clone(), alias.join("credentials.json")] {
            let error = match write(&target, std::slice::from_ref(&pool)).await {
                Ok(path) => panic!("生产池路径被接受: {}", path.display()),
                Err(error) => error,
            };
            assert!(error.contains("生产凭据路径"), "{error}");
        }
    }
}
