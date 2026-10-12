//! 配置校验, 命令行解析与发布契约的端到端测试.
//!
//! End-to-end tests of configuration validation, command line parsing and release contract.

mod common;

use std::path::Path;

use common::run_cli;

#[tokio::test]
async fn release_contract_covers_version_command_and_package_script()
-> Result<(), Box<dyn std::error::Error>> {
    // version 子命令输出版本号与提交号, 供下载方核对产物与标签一致.
    let output = run_cli(&["version"], &[])?;
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(env!("CARGO_PKG_VERSION")), "{stdout}");
    assert!(stdout.contains("commit"), "{stdout}");

    let temp_dir = tempfile::tempdir()?;
    let dummy_binary = temp_dir.path().join("aria_dummy");
    std::fs::write(&dummy_binary, b"dummy executable content")?;

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("无法定位仓库根目录")?;
    let package_script = repo_root.join("scripts/package.sh");

    let target_triple = "test-target-triple";
    let tag = "v0.0.0-test";
    let archive_name = format!("aria-{tag}-{target_triple}.tar.gz");
    let checksum_name = format!("{archive_name}.sha256");

    let dist_dir = repo_root.join("dist");
    let archive_path = dist_dir.join(&archive_name);
    let checksum_path = dist_dir.join(&checksum_name);

    let status = std::process::Command::new("bash")
        .arg(&package_script)
        .arg(target_triple)
        .arg(tag)
        .arg(&dummy_binary)
        .status()?;
    assert!(status.success(), "scripts/package.sh 应当执行成功");

    struct CleanupGuard<'a> {
        archive: &'a Path,
        checksum: &'a Path,
    }
    impl Drop for CleanupGuard<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.archive);
            let _ = std::fs::remove_file(self.checksum);
            if let Some(parent) = self.archive.parent() {
                let _ = std::fs::remove_dir(parent);
            }
        }
    }
    let _guard = CleanupGuard {
        archive: &archive_path,
        checksum: &checksum_path,
    };

    assert!(archive_path.is_file(), "产物压缩包应当存在");
    assert!(checksum_path.is_file(), "校验文件应当存在");

    let checksum_content = std::fs::read_to_string(&checksum_path)?;
    let parts: Vec<&str> = checksum_content
        .trim_end_matches(&['\r', '\n'][..])
        .split("  ")
        .collect();
    assert_eq!(
        parts.len(),
        2,
        "校验文件格式应为 sha256sum 输出格式 (双空格分隔哈希与文件名)"
    );
    assert_eq!(parts[0].len(), 64, "哈希值应当为 64 位十六进制字符");
    assert!(
        parts[0].chars().all(|c| c.is_ascii_hexdigit()),
        "哈希值应当全由十六进制字符组成"
    );
    assert_eq!(
        parts[1], archive_name,
        "校验文件中的文件名应当与归档文件名一致"
    );

    let archive_path_str = archive_path.to_str().ok_or("归档路径转换失败")?;
    let tar_output = std::process::Command::new("tar")
        .args(["-tzf", archive_path_str])
        .output()?;
    assert!(tar_output.status.success(), "tar 列出归档内容应当成功");
    let tar_entries: Vec<&str> = std::str::from_utf8(&tar_output.stdout)?
        .lines()
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(
        tar_entries,
        vec!["aria"],
        "归档内应当只有根目录下的 aria 一项成员"
    );

    Ok(())
}
