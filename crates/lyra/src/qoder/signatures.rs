// Copyright (c) 2026 ggbdpq (MIT License)
// Copyright (c) 2026 wangjunyao (MIT License)

//! Qoder 网关协议常量定义文件。
//!
//! 此处汇集网关通信所需的公开协议凭证、默认端点与固定协议版本标识。
//! 根据逆向工程事实，网关使用固定的服务端 RSA 公钥包裹每请求的对称加密密钥，
//! 客户端版本号与应用标识用于网关注册与鉴权校验。

/// 网关服务端 RSA 公钥 PEM 格式字符串。
/// 服务端在解密每请求对称密钥时依赖此公钥对应的私钥。
pub const SERVER_PUBKEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDA8iMH5c02LilrsERw9t6Pv5Nc\n\
4k6Pz1EaDicBMpdpxKduSZu5OANqUq8er4GM95omAGIOPOh+Nx0spthYA2BqGz+l\n\
6HRkPJ7S236FZz73In/KVuLnwI8JJ2CbuJap8kvheCCZpmAWpb/cPx/3Vr/J6I17\n\
XcW+ML9FoCI6AOvOzwIDAQAB\n\
-----END PUBLIC KEY-----";

/// 客户端协议版本号，与官方扩展客户端保持同步。
pub const COSY_VERSION: &str = "0.1.43";

/// 网关应用标识码。
pub const APPCODE: &str = "cosy";

/// 默认网关接入根地址。
pub const DEFAULT_GATEWAY_URL: &str = "https://gateway.qoder.com.cn";

/// 认证签名计算使用的默认静态机密串，来源为公开协议基准实现。
pub const DEFAULT_SECRET: &str = "d2FyLCB3YXIgbmV2ZXIgY2hhbmdlcw==";
