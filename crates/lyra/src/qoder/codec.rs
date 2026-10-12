// Copyright (c) 2026 ggbdpq (MIT License)
// Copyright (c) 2026 wangjunyao (MIT License)

//! Qoder 网关自定义数据编解码与加密模块。
//!
//! 协议事实来源为逆向工程所得的公开协议基准实现。
//! 网关在传输敏感载荷时，采用自定义字符表与三段字符重排算法混淆 Base64 编码，
//! 同时使用 AES-128-CBC 与 RSA PKCS1 进行双层加密保护。

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use md5::{Digest, Md5};

use super::signatures::APPCODE;

/// 自定义 Base64 混淆字母表，包含 64 个特定顺序的 ASCII 可打印字符。
pub const CUSTOM_ALPHABET: &str =
    "_doRTgHZBKcGVjlvpC,@aFSx#DPuNJme&i*MzLOEn)sUrthbf%Y^w.(kIQyXqWA!";

/// 标准 Base64 字母表。
pub const STD_ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// 自定义填充字符，替换标准 Base64 中的等号。
pub const CUSTOM_PAD: char = '$';

/// 静态构建从标准 Base64 字符到自定义混淆字符的单字节映射表。
const STD_TO_CUSTOM: [u8; 256] = {
    let mut table = [0u8; 256];
    let std_bytes = STD_ALPHABET.as_bytes();
    let custom_bytes = CUSTOM_ALPHABET.as_bytes();
    let mut i = 0;
    while i < 64 {
        table[std_bytes[i] as usize] = custom_bytes[i];
        i += 1;
    }
    table[b'=' as usize] = CUSTOM_PAD as u8;
    table
};

/// 静态构建从自定义混淆字符到标准 Base64 字符的反向单字节映射表。
const CUSTOM_TO_STD: [u8; 256] = {
    let mut table = [0u8; 256];
    let std_bytes = STD_ALPHABET.as_bytes();
    let custom_bytes = CUSTOM_ALPHABET.as_bytes();
    let mut i = 0;
    while i < 64 {
        table[custom_bytes[i] as usize] = std_bytes[i];
        i += 1;
    }
    table[CUSTOM_PAD as usize] = b'=';
    table
};

/// 对 Base64 文本按规则划分三段并执行位置重排。
///
/// 算法规则：设字符串长度为 n，分界点 a = floor(n / 3)，
/// 重排结果依次连接尾段文本、中段文本与头段文本。
/// 该重排在置换代数上属于自逆操作，二次重排即可完全复原。
pub fn rearrange(text: &str) -> String {
    let len = text.len();
    if len == 0 {
        return String::new();
    }
    let a = len / 3;
    let head = &text[..a];
    let mid = &text[a..len - a];
    let tail = &text[len - a..];

    let mut result = String::with_capacity(len);
    result.push_str(tail);
    result.push_str(mid);
    result.push_str(head);
    result
}

/// 将原始字节切片编码为网关协议规定的自定义 Base64 字符串。
pub fn encode(plaintext: &[u8]) -> String {
    let std_b64 = STANDARD.encode(plaintext);
    let rearranged = rearrange(&std_b64);
    let mut output = String::with_capacity(rearranged.len());
    for b in rearranged.bytes() {
        let mapped = STD_TO_CUSTOM[b as usize];
        if mapped != 0 {
            output.push(mapped as char);
        } else {
            output.push(b as char);
        }
    }
    output
}

/// 将网关协议规定的自定义 Base64 字符串还原解码为原始二进制数据。
pub fn decode(encoded: &str) -> Result<Vec<u8>, String> {
    let mut std_rearranged = String::with_capacity(encoded.len());
    for ch in encoded.chars() {
        let byte_val = ch as usize;
        if byte_val >= 256 {
            return Err(format!("char out of custom alphabet: {ch}"));
        }
        let mapped = CUSTOM_TO_STD[byte_val];
        if mapped == 0 {
            return Err(format!("char out of custom alphabet: {ch}"));
        }
        std_rearranged.push(mapped as char);
    }
    let std_b64 = rearrange(&std_rearranged);
    STANDARD
        .decode(std_b64)
        .map_err(|err| format!("base64 decode error: {err}"))
}

/// 计算指定输入文本的 MD5 摘要值并输出为 32 位小写十六进制字符串。
pub fn md5_hex(text: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

/// 计算认证接口所需的日期签名摘要字符串。
pub fn sign_auth_date(date: &str, secret: &str) -> String {
    md5_hex(&format!("{APPCODE}&{secret}&{date}"))
}

/// 获取当前时间的 RFC1123 格式 GMT 时间戳字符串。
pub fn rfc1123_now() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// 使用 AES-128-CBC 算法与 PKCS7 填充加密明文数据，基于 aws-lc-rs 实现。
pub fn aes_128_cbc_encrypt(plain: &[u8], key: &[u8; 16]) -> Result<Vec<u8>, String> {
    use aws_lc_rs::cipher::{self, EncryptionContext, PaddedBlockEncryptingKey, UnboundCipherKey};
    let unbound = UnboundCipherKey::new(&cipher::AES_128, key)
        .map_err(|err| format!("构造 AES 未绑定密钥失败: {err:?}"))?;
    let key_obj = PaddedBlockEncryptingKey::cbc_pkcs7(unbound)
        .map_err(|err| format!("构造 AES 密钥失败: {err:?}"))?;

    let mut buffer = plain.to_vec();
    let enc_ctx = EncryptionContext::Iv128((*key).into());
    key_obj
        .less_safe_encrypt(&mut buffer, enc_ctx)
        .map_err(|err| format!("AES 加密失败: {err:?}"))?;
    Ok(buffer)
}

/// Qoder 官方网关服务端固定 1024 位 RSA 公钥模数 (大端序 128 字节常量).
const QODER_RSA_MODULUS_BYTES: [u8; 128] = [
    0xc0, 0xf2, 0x23, 0x07, 0xe5, 0xcd, 0x36, 0x2e, 0x29, 0x6b, 0xb0, 0x44, 0x70, 0xf6, 0xde, 0x8f,
    0xbf, 0x93, 0x5c, 0xe2, 0x4e, 0x8f, 0xcf, 0x51, 0x1a, 0x0e, 0x27, 0x01, 0x32, 0x97, 0x69, 0xc4,
    0xa7, 0x6e, 0x49, 0x9b, 0xb9, 0x38, 0x03, 0x6a, 0x52, 0xaf, 0x1e, 0xaf, 0x81, 0x8c, 0xf7, 0x9a,
    0x26, 0x00, 0x62, 0x0e, 0x3c, 0xe8, 0x7e, 0x37, 0x1d, 0x2c, 0xa6, 0xd8, 0x58, 0x03, 0x60, 0x6a,
    0x1b, 0x3f, 0xa5, 0xe8, 0x74, 0x64, 0x3c, 0x9e, 0xd2, 0xdb, 0x7e, 0x85, 0x67, 0x3e, 0xf7, 0x22,
    0x7f, 0xca, 0x56, 0xe2, 0xe7, 0xc0, 0x8f, 0x09, 0x27, 0x60, 0x9b, 0xb8, 0x96, 0xa9, 0xf2, 0x4b,
    0xe1, 0x78, 0x20, 0x99, 0xa6, 0x60, 0x16, 0xa5, 0xbf, 0xdc, 0x3f, 0x1f, 0xf7, 0x56, 0xbf, 0xc9,
    0xe8, 0x8d, 0x7b, 0x5d, 0xc5, 0xbe, 0x30, 0xbf, 0x45, 0xa0, 0x22, 0x3a, 0x00, 0xeb, 0xce, 0xcf,
];

/// 1024 位大整数内部表示 (16 个小端序 u64 词).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct U1024([u64; 16]);

/// 2048 位双长整数中间结果 (32 个小端序 u64 词).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct U2048([u64; 32]);

impl U1024 {
    fn from_be_bytes(bytes: &[u8; 128]) -> Self {
        let mut limbs = [0u64; 16];
        for (i, limb) in limbs.iter_mut().enumerate() {
            let start = 128 - (i + 1) * 8;
            let mut chunk = [0u8; 8];
            chunk.copy_from_slice(&bytes[start..start + 8]);
            *limb = u64::from_be_bytes(chunk);
        }
        Self(limbs)
    }

    fn to_be_bytes(self) -> [u8; 128] {
        let mut bytes = [0u8; 128];
        for i in 0..16 {
            let chunk = self.0[i].to_be_bytes();
            let start = 128 - (i + 1) * 8;
            bytes[start..start + 8].copy_from_slice(&chunk);
        }
        bytes
    }

    fn mul_wide(&self, other: &Self) -> U2048 {
        let mut out = [0u64; 32];
        for i in 0..16 {
            let mut carry = 0u128;
            for j in 0..16 {
                let prod =
                    (self.0[i] as u128) * (other.0[j] as u128) + (out[i + j] as u128) + carry;
                out[i + j] = prod as u64;
                carry = prod >> 64;
            }
            out[i + 16] = carry as u64;
        }
        U2048(out)
    }
}

impl U2048 {
    fn rem_mod(&self, modulus: &U1024) -> U1024 {
        let mut rem = [0u64; 16];
        for bit_idx in (0..2048).rev() {
            let limb = bit_idx / 64;
            let offset = bit_idx % 64;
            let bit = (self.0[limb] >> offset) & 1;

            let mut carry = bit;
            for item in &mut rem {
                let next_carry = *item >> 63;
                *item = (*item << 1) | carry;
                carry = next_carry;
            }

            let mut ge = carry != 0;
            if !ge {
                let mut strictly_less = false;
                for i in (0..16).rev() {
                    if rem[i] > modulus.0[i] {
                        ge = true;
                        break;
                    } else if rem[i] < modulus.0[i] {
                        strictly_less = true;
                        break;
                    }
                }
                if !ge && !strictly_less {
                    ge = true;
                }
            }

            if ge {
                let mut borrow = 0u128;
                for (i, item) in rem.iter_mut().enumerate() {
                    let diff = (*item as u128)
                        .wrapping_sub(modulus.0[i] as u128)
                        .wrapping_sub(borrow);
                    *item = diff as u64;
                    borrow = if diff > (u64::MAX as u128) { 1 } else { 0 };
                }
            }
        }
        U1024(rem)
    }
}

fn mul_mod(a: &U1024, b: &U1024, n: &U1024) -> U1024 {
    a.mul_wide(b).rem_mod(n)
}

fn modexp_65537(m: &U1024, n: &U1024) -> U1024 {
    let mut cur = *m;
    for _ in 0..16 {
        cur = mul_mod(&cur, &cur, n);
    }
    mul_mod(&cur, m, n)
}

/// 使用 Qoder 服务端固定 1024 位 RSA 公钥对明文数据执行 PKCS1 v1.5 加密。
///
/// 算法遵循 RFC 2313 PKCS#1 v1.5 标准构造填充块，并计算 c = m^65537 mod n。
/// 之所以不使用工程现有的 aws-lc-rs 库，是因为该库在 rsa/key.rs 第 777 行显式硬编码拒绝
/// 位数小于 2048 位的密钥（0..=2047 均返回 KeyRejected::too_small 错误）。
/// Qoder 服务端固定采用 1024 位公钥，且此处仅执行公钥加密运算，公钥与指数均为已知公开常数，
/// 运算耗时不涉及私钥侧信道泄漏，因此不需要常数时间实现，采用纯 Rust 自包含模幂计算最为稳健。
pub fn rsa_pkcs1_encrypt(plain: &[u8]) -> Result<Vec<u8>, String> {
    if plain.len() > 117 {
        return Err("明文数据长度超出 1024 位 RSA PKCS1 容纳上限".to_owned());
    }

    let mut block = [0u8; 128];
    block[0] = 0x00;
    block[1] = 0x02;
    let ps_len = 128 - 3 - plain.len();
    for item in &mut block[2..2 + ps_len] {
        let mut r = 0u8;
        while r == 0 {
            r = rand::random();
        }
        *item = r;
    }
    block[2 + ps_len] = 0x00;
    block[3 + ps_len..].copy_from_slice(plain);

    let n = U1024::from_be_bytes(&QODER_RSA_MODULUS_BYTES);
    let m = U1024::from_be_bytes(&block);
    let c = modexp_65537(&m, &n);
    Ok(c.to_be_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qoder::catalog::ModelCatalog;
    use crate::qoder::signatures::DEFAULT_SECRET;

    /// 编解码, 签名与加解密的固定向量, 以及模型目录的解析与翻译规则. 目录断言自
    /// catalog.rs 并入: 显示名翻译为内部键, 内部键原样通过, 大小写不同的名称与停用
    /// 条目不命中.
    #[test]
    fn test_qoder_codec_crypto_and_catalog() -> Result<(), String> {
        let cases = [
            ("", ""),
            ("a", "$p$#"),
            ("ab", "$SB#"),
            ("abc", "MSK#"),
            ("abcd", "$$KMD_#S"),
            ("hello, world", "bNO%zHqrBZJPHFru"),
        ];
        for (plain, expected) in cases {
            assert_eq!(encode(plain.as_bytes()), expected);
        }

        let texts = [
            "",
            "a",
            "ab",
            "abc",
            "hello, world",
            "你好，世界！含 emoji 🎉",
            "qoder gateway custom codec roundtrip test payload with special characters: !@#$%^&*()",
        ];
        for text in texts {
            let encoded = encode(text.as_bytes());
            let decoded = decode(&encoded)?;
            assert_eq!(decoded, text.as_bytes());
        }

        let payload = r#"{"payload":"{\"personalToken\":\"x\"}","encodeVersion":"1"}"#;
        assert_eq!(
            encode(payload.as_bytes()),
            "Fh#(WzDFDLNEj)u(I*l*B%BEw$Wh#S%au(tLuLf*lLf*mgf*e,BrBOmYKf#xLru(gzBMn*m.f*NHFYN("
        );

        assert_eq!(
            sign_auth_date("Tue, 29 Sep 2026 12:00:00 GMT", DEFAULT_SECRET),
            "cbbb4b907f733402e8e236806685d3e5"
        );

        let temp_key = b"0123456789abcdef";
        let rsa_out = rsa_pkcs1_encrypt(temp_key)?;
        assert_eq!(rsa_out.len(), 128);

        // 验证确定性固定填充块的模幂密文与 Python 计算结果完全一致
        let mut fixed_block = [0u8; 128];
        fixed_block[0] = 0x00;
        fixed_block[1] = 0x02;
        fixed_block[2..111].fill(0xff);
        fixed_block[111] = 0x00;
        fixed_block[112..].copy_from_slice(temp_key);
        let n = U1024::from_be_bytes(&QODER_RSA_MODULUS_BYTES);
        let m = U1024::from_be_bytes(&fixed_block);
        let c = modexp_65537(&m, &n);
        let expected_c_hex = "76c573dfb64a9da7876226f7d27a9a03d4a854cd22ee7fb4eb02c72d79b9be2140ee8792c64a1a48ef6adf93bee4d0c2615cf46771a26117a8671b8a00f3f7853a265cfca7aa426adcd0b53e2af72f2a2a4cba1427cd34019e758d10eb3090593debe8efa3f8560646695227e394da330200d23ac0d5ec2800b84e431c2797e1";
        assert_eq!(hex::encode(c.to_be_bytes()), expected_c_hex);

        let aes_out = aes_128_cbc_encrypt(b"hello world", temp_key)?;
        assert_eq!(aes_out.len(), 16);

        // 模型目录的解析与翻译: 显示名翻译为内部键, 内部键原样通过, 大小写不同的名称与
        // 停用条目不命中.
        let catalog_body = serde_json::json!({
            "chat": [
                {"key": "auto", "display_name": "Auto", "enable": true},
                {"key": "kmodel_latest", "display_name": "Kimi-K3", "enable": true},
                {"key": "offmodel", "display_name": "Off", "enable": false}
            ],
            "developer": [{"key": "auto", "display_name": "Auto", "enable": true}]
        })
        .to_string();
        let catalog = ModelCatalog::parse(catalog_body.as_bytes())?;
        assert_eq!(catalog.resolve("Kimi-K3").as_deref(), Some("kmodel_latest"));
        assert_eq!(catalog.resolve("auto").as_deref(), Some("auto"));
        assert_eq!(catalog.resolve("Kimi-k3"), None);
        assert_eq!(catalog.resolve("Off"), None);
        assert!(catalog.describe_entries().contains("Kimi-K3=kmodel_latest"));
        Ok(())
    }
}
