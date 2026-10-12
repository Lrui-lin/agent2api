//! 图片请求的**两步接力**（映射上 `imageMode = relay`，见 `model_rules::ImageMode`）。
//!
//! ── 做什么 ──────────────────────────────────────────────────
//! 请求带图、而承载它的那条映射配了图片回退目标且选了 relay 时：
//!   ① 回退模型（如 `sn-glm-5-3-flash`）只负责把**每张图**转写成文字；
//!   ② 网关把请求里的图片块换成这段文字（标明「由谁转写、原模型看不到原图」）；
//!   ③ 换好的请求照常交给原模型（如 `sn-glm-5-3`）回答。
//! 客户端看到的请求名、协议与流式形态都不变。
//!
//! ── 在哪一步做 ────────────────────────────────────────────────
//! `provider_loop::forward_with_providers` 拿到最终候选链之后、进账号循环之前：
//! 一次请求只做一次（换号重试复用同一份换好的请求体），且「哪一家、哪个回退
//! 模型」与 `send_body` 的按家改写读的是同一份 `wire_target_for_provider`。
//! 候选链里第一家配了 relay 的说了算；换好之后请求里没有图了，后面的家
//! （哪怕配的是 switch）也只会看到文字 —— 这是有意的：同一条请求不给两种答案。
//!
//! ── 转写怎么发 ────────────────────────────────────────────────
//! 走网关自己的 `UpstreamService::forward`（凭证、换号、限额冷却、出站脱敏与
//! 协议翻译都与真实请求一致），收窄成：只准这一家、非流式、不去重、跳过模型
//! 启停门禁（回退模型在管理页关着也照样能用，与 switch 模式同一口径），并带
//! `skip_image_relay` —— 否则回退模型自己的映射若也配了 relay，就会无限套娃。
//! 转写请求**不单独进请求日志**：次数与 token 写进主请求尝试明细的提示里。
//!
//! ── 失败怎么办 ────────────────────────────────────────────────
//! 任何一张图转写失败（超时 / 上游报错 / 空回答）都**整体放弃接力**、请求体
//! 原样不动 —— `send_body` 看到请求仍带图，按 switch 把整条请求改发回退模型。
//! 宁可这一轮由回退模型回答，也不让原模型对着一张「没描述出来的图」编答案。
//! 已经转写成功的那几张照样进缓存，客户端重试时不用再转一次。
//!
//! ── 缓存 ─────────────────────────────────────────────────────
//! 多轮对话里同一张图会在每一轮的历史里重复出现，所以按
//! `(provider, 回退模型, 提示词版本, 图片内容指纹)` 缓存转写结果（进程内存、
//! LRU + TTL）。每轮只有**新出现的图**要真的转写。单次请求最多新转写
//! [`MAX_NEW_PER_REQUEST`] 张：超过时（典型是重启后接着一条带很多截图的会话）
//! 先转前几张进缓存、本轮按 switch 回答，后续几轮逐步补齐后自然恢复接力。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::server::core::key_scope::KeyScope;
use crate::server::core::model_rules::ImageMode;
use crate::server::core::providers::catalog::WireTarget;
use crate::server::logging;

use super::aggregate::aggregate_frame_stream;
use super::usage::RequestTelemetry;
use super::{ForwardOutcome, ForwardRequest, UpstreamService};

/// 转写提示词的版本号（进缓存键）：改了提示词就换号，旧转写自然失效。
const PROMPT_VERSION: &str = "v1";
/// 单张图转写的总预算（含等首帧与读完回答）
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(90);
/// 转写回答的输出上限：截图里的长文档 / 大表格要能写完
const DESCRIBE_MAX_TOKENS: u64 = 4096;
/// 同时转写几张（新图通常一轮只有一两张，这个上限只在会话补齐时起作用）
const DESCRIBE_CONCURRENCY: usize = 3;
/// 单次请求最多新转写几张（见模块头「缓存」）
const MAX_NEW_PER_REQUEST: usize = 8;
/// 缓存容量与有效期
const CACHE_CAPACITY: usize = 512;
const CACHE_TTL_MS: i64 = 6 * 60 * 60 * 1000;

const DESCRIBE_PROMPT: &str = "你是另一个只能读文字的模型的「眼睛」。请把这张图片的内容完整、准确地转写成文字，\
它会替代原图交给那个模型继续回答用户的问题，那个模型看不到原图。要求：\n\
1. 先用一句话说明图片类型（截图 / 照片 / 图表 / 文档 / 界面等）和整体内容。\n\
2. 逐字照录图中所有可见文字，按原有阅读顺序和层级；代码、命令、报错、路径、数字、单位一字不改。\n\
3. 表格用 Markdown 表格还原；图表写清坐标轴、图例、关键数值和趋势；界面写清各区域、按钮、选中/高亮状态。\n\
4. 与理解相关的位置关系、颜色、标注、箭头、圈选也要写出来。\n\
5. 看不清的部分写「[看不清]」，不要猜测，也不要编造图里没有的内容。\n\
6. 只做客观转写，不回答问题、不给建议、不加评论。";
/// 这一条请求的接力计划：谁家、谁转写、谁回答。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RelayPlan {
    pub provider: String,
    /// 回退模型（该家真名，负责转写）
    pub describer: String,
    /// 原模型（该家真名，负责回答；只用于日志与提示文案）
    pub answerer: String,
}

/// 从「候选家 + 各家解析结果」里挑出接力计划：第一家配了 relay、且回退模型
/// 与原模型不同的说了算（纯函数，便于单测；取解析结果由调用方做）。
pub(super) fn plan_from<'a>(targets: impl IntoIterator<Item = (&'a str, WireTarget)>) -> Option<RelayPlan> {
    targets.into_iter().find_map(|(provider, wire)| {
        let describer = wire.image_model.as_deref()?.trim();
        if wire.image_mode != ImageMode::Relay
            || describer.is_empty()
            || describer.eq_ignore_ascii_case(&wire.model)
        {
            return None;
        }
        Some(RelayPlan {
            provider: provider.to_string(),
            describer: describer.to_string(),
            answerer: wire.model.clone(),
        })
    })
}

/// 一个图片块里的图片地址（`image_url` 可以是 `{url}` 对象，也可以直接是字符串）
fn image_url_text(part: &Value) -> &str {
    match part.get("image_url") {
        Some(Value::String(url)) => url,
        Some(Value::Object(object)) => object.get("url").and_then(Value::as_str).unwrap_or(""),
        _ => "",
    }
}

fn is_image_part(part: &Value) -> bool {
    part.get("type").and_then(Value::as_str) == Some("image_url")
}

/// 图片内容指纹：对地址整串（data URL 即内容本身）取 sha256
fn fingerprint(url: &str) -> String {
    let digest = Sha256::digest(url.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// 请求里出现过的图片（按首次出现顺序去重）：`(指纹, 那个图片块的原样副本)`。
/// 地址为空的块不收 —— 它没有可转写的内容，留在原处由上游自己处理。
pub(super) fn collect_images(body: &Value) -> Vec<(String, Value)> {
    let mut seen: Vec<(String, Value)> = Vec::new();
    let messages = body.get("messages").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    for message in messages {
        let Some(parts) = message.get("content").and_then(Value::as_array) else { continue };
        for part in parts.iter().filter(|part| is_image_part(part)) {
            let url = image_url_text(part);
            if url.is_empty() {
                continue;
            }
            let key = fingerprint(url);
            if !seen.iter().any(|(existing, _)| *existing == key) {
                seen.push((key, part.clone()));
            }
        }
    }
    seen
}

/// 把请求里的每个图片块换成对应的转写文字（`texts` 按指纹查；查不到的块不动）。
/// 同一张图在历史里出现几次就换几次，编号按首次出现顺序（与 `collect_images` 一致）。
pub(super) fn replace_images(body: &mut Value, texts: &HashMap<String, String>, describer: &str) -> usize {
    let order: Vec<String> = collect_images(body).into_iter().map(|(key, _)| key).collect();
    let mut replaced = 0;
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else { return 0 };
    for message in messages.iter_mut() {
        let Some(parts) = message.get_mut("content").and_then(Value::as_array_mut) else { continue };
        for part in parts.iter_mut() {
            if !is_image_part(part) {
                continue;
            }
            let key = fingerprint(image_url_text(part));
            let Some(text) = texts.get(&key) else { continue };
            let number = order.iter().position(|existing| *existing == key).map_or(0, |index| index + 1);
            *part = json!({
                "type": "text",
                "text": format!(
                    "[图片 {number}：以下内容由 {describer} 转写，你看不到原图，请以这段转写为准]\n{text}\n[图片 {number} 转写结束]"
                ),
            });
            replaced += 1;
        }
    }
    replaced
}

// ─── 转写缓存 ─────────────────────────────────────────────

struct CacheEntry {
    text: String,
    stored_at: i64,
    touched_at: i64,
}

/// 进程内 LRU + TTL（时刻由调用方传入，便于单测）
pub(super) struct DescribeCache {
    entries: HashMap<String, CacheEntry>,
    capacity: usize,
    ttl_ms: i64,
}

impl DescribeCache {
    pub(super) fn new(capacity: usize, ttl_ms: i64) -> Self {
        Self { entries: HashMap::new(), capacity, ttl_ms }
    }

    pub(super) fn get(&mut self, key: &str, now: i64) -> Option<String> {
        let expired = self.entries.get(key).is_some_and(|entry| now - entry.stored_at > self.ttl_ms);
        if expired {
            self.entries.remove(key);
            return None;
        }
        let entry = self.entries.get_mut(key)?;
        entry.touched_at = now;
        Some(entry.text.clone())
    }

    pub(super) fn put(&mut self, key: String, text: String, now: i64) {
        self.entries.insert(key, CacheEntry { text, stored_at: now, touched_at: now });
        while self.entries.len() > self.capacity {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched_at)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => self.entries.remove(&key),
                None => break,
            };
        }
    }
}

fn cache() -> &'static Mutex<DescribeCache> {
    static CACHE: OnceLock<Mutex<DescribeCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(DescribeCache::new(CACHE_CAPACITY, CACHE_TTL_MS)))
}

fn lock_cache() -> std::sync::MutexGuard<'static, DescribeCache> {
    match cache().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn cache_key(plan: &RelayPlan, image_key: &str) -> String {
    format!(
        "{}\u{1}{}\u{1}{PROMPT_VERSION}\u{1}{image_key}",
        plan.provider.to_ascii_lowercase(),
        plan.describer.to_ascii_lowercase()
    )
}
// ─── 转写一张 ─────────────────────────────────────────────

/// 回答正文（`choices[0].message.content`：字符串，或文本块数组）
fn completion_text(completion: &Value) -> String {
    let content = completion
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"));
    match content {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("")
            .trim()
            .to_string(),
        _ => String::new(),
    }
}

/// 一次转写的结果：文字 + 这次转写花掉的 token
struct Described {
    text: String,
    tokens: i64,
}

/// 发一次转写：网关自己的转发链路，收窄见模块头「转写怎么发」。
///
/// 返回装箱的 `Send` future：这里会再进 `forward`，而 `forward` 走到
/// `forward_with_providers` 又会来到本模块 —— 不在这里擦掉类型，
/// 编译器就得推一个无限递归的 future 类型。
///
/// 参数全部按值传（`UpstreamService` 内部是 `Arc`，克隆很便宜）：几张图的转写
/// 并发跑在一个 future 流里，借用进去会撞上「Send 不够通用」的高阶生命周期限制。
fn describe_one(
    service: UpstreamService,
    plan: RelayPlan,
    image: Value,
    parent: Arc<RequestTelemetry>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Described, String>> + Send>> {
    Box::pin(async move {
        let body = json!({
            "model": plan.describer,
            "stream": false,
            "max_tokens": DESCRIBE_MAX_TOKENS,
            "messages": [{ "role": "user", "content": [
                { "type": "text", "text": DESCRIBE_PROMPT },
                image,
            ]}],
        });
        let telemetry = Arc::new(RequestTelemetry::with_id());
        // 主请求被「终止请求」掐掉时，转写一起停
        if let Some(token) = parent.cancel_token() {
            telemetry.set_cancel_token(token);
        }
        let forward = service.forward(ForwardRequest {
            body,
            stream: false,
            dedupe_key: String::new(),
            client_headers: Default::default(),
            telemetry: telemetry.clone(),
            allowed_providers: Some(KeyScope::provider_only(&plan.provider)),
            pinned_account: None,
            ignore_model_gate: true,
            skip_image_relay: true,
        });
        let text = match tokio::time::timeout(DESCRIBE_TIMEOUT, forward).await {
            Err(_) => return Err(format!("转写超时（{} 秒）", DESCRIBE_TIMEOUT.as_secs())),
            Ok(Err(error)) => return Err(error.message),
            Ok(Ok(ForwardOutcome::Completion { body })) => completion_text(&body),
            Ok(Ok(ForwardOutcome::Stream { stream, .. })) => {
                use futures::StreamExt;
                match tokio::time::timeout(
                    DESCRIBE_TIMEOUT,
                    aggregate_frame_stream(stream.boxed(), telemetry.clone(), None),
                )
                .await
                {
                    Err(_) => return Err(format!("转写超时（{} 秒）", DESCRIBE_TIMEOUT.as_secs())),
                    Ok(Err(error)) => return Err(error.message),
                    Ok(Ok(aggregated)) => completion_text(&aggregated.body),
                }
            }
        };
        if text.is_empty() {
            return Err("回退模型没有给出转写文字".to_string());
        }
        Ok(Described { text, tokens: telemetry.snapshot().total_tokens })
    })
}

// ─── 入口 ───────────────────────────────────────────────

/// 一次接力尝试的结果。
pub(super) struct RelayOutcome {
    /// 换好图片的请求体；None = 本轮不接力（请求原样，`send_body` 会按 switch 改发）
    pub body: Option<Value>,
    /// 写进主请求尝试明细的提示（成功与放弃都写，用户能在请求日志里看到这一轮怎么走的）
    pub notice: String,
}

/// 选路前的接力入口。返回 None = 与接力无关（没图 / 候选家都没配 relay）。
pub(super) async fn prepare(
    service: &UpstreamService,
    body: &Value,
    provider_ids: &[&str],
    telemetry: &Arc<RequestTelemetry>,
) -> Option<RelayOutcome> {
    if !super::payload::request_has_images(body) {
        return None;
    }
    let requested = body.get("model").and_then(Value::as_str).unwrap_or("").trim().to_string();
    let plan = plan_from(provider_ids.iter().map(|provider| {
        (*provider, crate::server::core::providers::catalog::wire_target_for_provider(&requested, provider, None))
    }))?;
    let images = collect_images(body);
    if images.is_empty() {
        return None;
    }

    let now = logging::now_ms();
    let mut texts: HashMap<String, String> = HashMap::new();
    let mut pending: Vec<(String, Value)> = Vec::new();
    {
        let mut cache = lock_cache();
        for image in &images {
            match cache.get(&cache_key(&plan, &image.0), now) {
                Some(text) => {
                    texts.insert(image.0.clone(), text);
                }
                None => pending.push(image.clone()),
            }
        }
    }
    let cached = texts.len();
    let deferred = pending.len().saturating_sub(MAX_NEW_PER_REQUEST);
    pending.truncate(MAX_NEW_PER_REQUEST);

    let started = logging::now_ms();
    let jobs: Vec<_> = pending
        .into_iter()
        .map(|(key, part)| {
            let job = describe_one(service.clone(), plan.clone(), part, telemetry.clone());
            async move { (key, job.await) }
        })
        .collect();
    let results: Vec<(String, Result<Described, String>)> = {
        use futures::StreamExt;
        futures::stream::iter(jobs).buffer_unordered(DESCRIBE_CONCURRENCY).collect().await
    };

    // 转写耗时（并发跑，墙钟时间）：带进提示，慢在哪一步一眼可见
    let elapsed = format!("{:.1}s", (logging::now_ms() - started).max(0) as f64 / 1000.0);
    let mut tokens = 0i64;
    let mut failures: Vec<String> = Vec::new();
    let described = results.len();
    {
        let mut cache = lock_cache();
        let now = logging::now_ms();
        for (key, result) in results {
            match result {
                Ok(done) => {
                    tokens += done.tokens;
                    cache.put(cache_key(&plan, &key), done.text.clone(), now);
                    texts.insert(key, done.text);
                }
                Err(reason) => failures.push(reason),
            }
        }
    }

    let fallback = |why: String| {
        logging::console_line(
            "[ImageRelay]",
            &format!("两步接力未完成（{why}），本轮按整段改发 {}", plan.describer),
        );
        Some(RelayOutcome {
            body: None,
            notice: format!("图片两步接力未完成（{why}），本轮整段改发 {}", plan.describer),
        })
    };
    if let Some(reason) = failures.first() {
        return fallback(format!("{} 张转写失败（{elapsed}）：{reason}", failures.len()));
    }
    if deferred > 0 {
        return fallback(format!(
            "新图 {} 张超过单次上限 {MAX_NEW_PER_REQUEST}，已先转写 {described} 张进缓存",
            described + deferred
        ));
    }

    let mut relayed = body.clone();
    let replaced = replace_images(&mut relayed, &texts, &plan.describer);
    logging::console_line(
        "[ImageRelay]",
        &format!(
            "{} 张图由 {} 转写后交给 {}（新转写 {described}，缓存 {cached}，转写 {elapsed} / {tokens} tokens，替换 {replaced} 处）",
            images.len(),
            plan.describer,
            plan.answerer
        ),
    );
    Some(RelayOutcome {
        body: Some(relayed),
        notice: format!(
            "图片两步接力：{} 张图由 {} 转写后交给 {}（新转写 {described} · 缓存 {cached} · 转写 {elapsed} / {tokens} tokens）",
            images.len(),
            plan.describer,
            plan.answerer
        ),
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    fn wire(model: &str, image: Option<&str>, mode: ImageMode) -> WireTarget {
        WireTarget {
            model: model.to_string(),
            reasoning: None,
            image_model: image.map(str::to_string),
            image_mode: mode,
        }
    }

    fn image(url: &str) -> Value {
        json!({ "type": "image_url", "image_url": { "url": url } })
    }

    /// 计划只认 relay + 有回退 + 回退与原模型不同；第一家符合的说了算
    #[test]
    fn plan_picks_the_first_relay_provider() {
        assert_eq!(plan_from([("raccoon", wire("sn-glm-5-3", Some("sn-glm-5-3-flash"), ImageMode::Switch))]), None);
        assert_eq!(plan_from([("raccoon", wire("sn-glm-5-3", None, ImageMode::Relay))]), None);
        assert_eq!(plan_from([("raccoon", wire("sn-glm-5-3", Some("SN-GLM-5-3"), ImageMode::Relay))]), None);
        let plan = plan_from([
            ("catpaw", wire("glm-5.3", None, ImageMode::Switch)),
            ("raccoon", wire("sn-glm-5-3", Some("sn-glm-5-3-flash"), ImageMode::Relay)),
            ("zcode", wire("glm-5.3", Some("glm-5.3-flash"), ImageMode::Relay)),
        ])
        .expect("raccoon 配了 relay");
        assert_eq!(plan.provider, "raccoon");
        assert_eq!(plan.describer, "sn-glm-5-3-flash");
        assert_eq!(plan.answerer, "sn-glm-5-3");
    }

    /// 去重按内容、顺序按首次出现；字符串形态的 image_url 也认；空地址不收
    #[test]
    fn collect_dedupes_by_content_in_first_seen_order() {
        let body = json!({ "messages": [
            { "role": "user", "content": [ image("data:image/png;base64,AAA"), { "type": "text", "text": "看图" } ] },
            { "role": "assistant", "content": "好的" },
            { "role": "user", "content": [
                { "type": "image_url", "image_url": "data:image/png;base64,BBB" },
                image("data:image/png;base64,AAA"),
                image(""),
            ]},
        ]});
        let images = collect_images(&body);
        assert_eq!(images.len(), 2);
        assert_eq!(images[0].0, fingerprint("data:image/png;base64,AAA"));
        assert_eq!(images[1].0, fingerprint("data:image/png;base64,BBB"));
    }

    /// 替换：每处出现都换、编号按首次出现、没有转写的块保持原样
    #[test]
    fn replace_swaps_every_occurrence_and_keeps_unknown_images() {
        let mut body = json!({ "messages": [
            { "role": "user", "content": [ image("data:a"), { "type": "text", "text": "第一轮" } ] },
            { "role": "user", "content": [ image("data:b"), image("data:a"), image("data:c") ] },
        ]});
        let texts: HashMap<String, String> = [
            (fingerprint("data:a"), "红色方块".to_string()),
            (fingerprint("data:b"), "终端报错 E42".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(replace_images(&mut body, &texts, "flash"), 3);
        let second = body["messages"][1]["content"].as_array().expect("数组");
        let first_text = second[1]["text"].as_str().expect("文本");
        assert!(first_text.starts_with("[图片 1：以下内容由 flash 转写"), "{first_text}");
        assert!(first_text.contains("红色方块"));
        assert!(second[0]["text"].as_str().expect("文本").contains("[图片 2"));
        // 没转写的那张原样留着 —— 请求因此仍然「带图」，send_body 会按 switch 处理
        assert_eq!(second[2]["type"], "image_url");
        assert_eq!(body["messages"][0]["content"][1]["text"], "第一轮");
    }

    #[test]
    fn cache_expires_and_evicts_least_recently_used() {
        let mut cache = DescribeCache::new(2, 1_000);
        cache.put("a".into(), "A".into(), 0);
        cache.put("b".into(), "B".into(), 10);
        assert_eq!(cache.get("a", 20).as_deref(), Some("A")); // a 被用过，b 成了最久没用的
        cache.put("c".into(), "C".into(), 30);
        assert_eq!(cache.get("b", 40), None, "容量满时淘汰最久没用的");
        assert_eq!(cache.get("a", 40).as_deref(), Some("A"));
        assert_eq!(cache.get("a", 2_000), None, "超过有效期失效");
    }

    #[test]
    fn completion_text_reads_string_or_text_parts() {
        let string = json!({ "choices": [{ "message": { "content": "  一段转写 " } }] });
        assert_eq!(completion_text(&string), "一段转写");
        let parts = json!({ "choices": [{ "message": { "content": [
            { "type": "text", "text": "上半" }, { "type": "text", "text": "下半" } ] } }] });
        assert_eq!(completion_text(&parts), "上半下半");
        assert_eq!(completion_text(&json!({ "choices": [] })), "");
    }

    /// 缓存键带上家、回退模型与提示词版本：换回退模型 / 改提示词都不会读到旧转写
    #[test]
    fn cache_key_separates_describers() {
        let flash = RelayPlan { provider: "raccoon".into(), describer: "sn-glm-5-3-flash".into(), answerer: "x".into() };
        let other = RelayPlan { describer: "sn-kimi-k3".into(), ..flash.clone() };
        assert_ne!(cache_key(&flash, "k"), cache_key(&other, "k"));
        assert!(cache_key(&flash, "k").contains(PROMPT_VERSION));
    }
}
