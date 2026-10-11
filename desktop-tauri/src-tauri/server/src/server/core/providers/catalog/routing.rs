//! 对外模型名到上游模型的解析。路由与发送侧共用本解析，避免选中一家后再换一套规则。

use serde_json::Value;

use crate::server::core::{custom_providers, model_rules};
use crate::server::core::models::model_id;
use crate::server::core::providers::{kind_from_id, kind_id, ProviderKind};

use super::{adapter_for, all_kinds, manifest_for};

/// `image_model` 是那条映射上的**图片请求回退目标**（`None` = 不回退）：
/// 请求体里带图片时调用方把发送名换成它（`upstream::payload::send_body`）。
/// 与 `reasoning` 同一条映射、同一次解析 —— 它要是拆出去单独查一遍，
/// 「名字按这条映射、回退按另一条」的串味就会重演。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WireTarget {
    pub model: String,
    pub reasoning: Option<String>,
    pub image_model: Option<String>,
}

pub(super) fn entry_id_in_manifest(manifest: &[Value], requested: &str) -> Option<String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }
    // 保留展示名兼容，但必须先匹配原始 ID；返回清单里的实际大小写。
    manifest.iter().find(|entry| model_id(entry).eq_ignore_ascii_case(requested))
        .or_else(|| manifest.iter().find(|entry| {
            entry.get("name")
                .map(crate::server::core::models::shape_value_text)
                .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        }))
        .map(model_id)
}

pub(super) fn builtin_target(
    rules: &model_rules::ModelRules,
    kind: ProviderKind,
    manifest: &[Value],
    requested: &str,
) -> Option<WireTarget> {
    let provider = kind_id(kind);
    if let Some(id) = entry_id_in_manifest(manifest, requested) {
        if rules.default_enabled(provider, &id) {
            let binding = rules.binding(provider, &id, &id);
            return Some(WireTarget {
                reasoning: binding.and_then(|m| m.reasoning.clone()),
                // 图片回退与等级同一来源（本名直发时从同名默认绑定借），
                // 并按该家清单解析成真名 —— 用户在管理页选的是广告名。
                image_model: binding
                    .and_then(|m| m.image_target.clone())
                    .and_then(|fallback| entry_id_in_manifest(manifest, &fallback))
                    .or_else(|| binding.and_then(|m| m.image_target.clone())),
                model: id,
            });
        }
    }
    let candidates: Vec<_> = rules.mappings.iter()
        .filter(|mapping| mapping.alias.eq_ignore_ascii_case(requested))
        .filter(|mapping| !mapping.alias.eq_ignore_ascii_case(&mapping.target))
        .filter(|mapping| mapping.provider.as_deref().map_or(true, |id| id == provider))
        .filter(|mapping| rules.binding(provider, &mapping.alias, &mapping.target)
            .is_some_and(|effective| effective.enabled))
        .filter_map(|mapping| {
            entry_id_in_manifest(manifest, &mapping.target).map(|target| (mapping, target))
        })
        .collect();
    let prefix = super::super::cline::models::Pool::from_provider_id(provider)
        .map(super::super::cline::models::Pool::target_prefix);
    let selected = candidates.iter()
        .filter(|(mapping, _)| mapping.provider.is_some())
        .find(|(_, target)| prefix.is_some_and(|prefix| target.starts_with(prefix)))
        .or_else(|| candidates.iter().find(|(mapping, _)| mapping.provider.is_some()))
        .or_else(|| candidates.first());
    selected.map(|(mapping, target)| {
        let binding = rules.binding(provider, &mapping.alias, &mapping.target);
        let image_target = binding.and_then(|effective| effective.image_target.clone());
        WireTarget {
            model: target.clone(),
            reasoning: binding.and_then(|effective| effective.reasoning.clone()),
            // 图片回退目标按该家清单解析成真名；解析不出（自定义模型、
            // 清单已变）原样发，由上游的报错路径给出可读失败。
            image_model: image_target
                .as_deref()
                .and_then(|fallback| entry_id_in_manifest(manifest, fallback))
                .or(image_target),
        }
    })
}

/// 原始能力查询不应用开关，供管理 API 展开旧版全局规则。
pub fn providers_for_model(model: &str) -> Vec<String> {
    let model = model.trim();
    if model.is_empty() {
        return Vec::new();
    }
    let manifests: Vec<_> = all_kinds().into_iter().map(|kind| (kind, manifest_for(kind))).collect();
    let exact = manifests.iter().any(|(_, items)| {
        items.iter().any(|item| model_id(item).eq_ignore_ascii_case(model))
    });
    let mut result: Vec<_> = manifests.into_iter().filter(|(_, items)| {
        if exact {
            items.iter().any(|item| model_id(item).eq_ignore_ascii_case(model))
        } else {
            entry_id_in_manifest(items, model).is_some()
        }
    }).map(|(kind, _)| kind_id(kind).to_string()).collect();
    result.extend(custom_providers::list().into_iter().filter_map(|provider| {
        let models = provider.get("models")?.as_array()?;
        models.iter().any(|entry| model_id(entry).eq_ignore_ascii_case(model))
            .then(|| provider.get("id").and_then(Value::as_str).unwrap_or("").to_string())
    }));
    result
}

/// 只返回有开启绑定的提供商，不把关闭的 ID 回落给默认提供商。
pub fn forwarding_providers(model: &str) -> Vec<String> {
    let rules = model_rules::current();
    let mut result: Vec<_> = all_kinds().into_iter().filter(|kind| {
        builtin_target(&rules, *kind, &manifest_for(*kind), model).is_some()
    }).map(|kind| kind_id(kind).to_string()).collect();
    result.extend(custom_providers::carriers_of_model(model));
    result
}

pub fn model_blocked_everywhere(model: &str) -> bool {
    if !forwarding_providers(model).is_empty() {
        return false;
    }
    !providers_for_model(model).is_empty()
        || model_rules::current().has_alias(model)
        || custom_providers::list().iter().any(|provider| {
            provider.get("mappings").and_then(Value::as_array).is_some_and(|mappings| {
                mappings.iter().any(|mapping| mapping.get("alias").and_then(Value::as_str)
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(model)))
            })
        })
}

pub fn wire_target_for_provider(model: &str, provider_id: &str, _account: Option<&Value>) -> WireTarget {
    let requested = model.trim();
    // 自定义提供商在自己的转发模块解析一次，避免把别名提前改写后丢失其等级绑定。
    let resolved = kind_from_id(provider_id).and_then(|kind| {
        builtin_target(&model_rules::current(), kind, &manifest_for(kind), requested)
    });
    resolved.unwrap_or_else(|| WireTarget {
        model: requested.to_string(),
        reasoning: None,
        image_model: None,
    })
}

pub fn default_model_catalog() -> Vec<Value> {
    let rules = model_rules::current();
    let mut result = Vec::new();
    for kind in all_kinds().into_iter().filter(|kind| adapter_for(*kind).supports_default_model()) {
        for entry in manifest_for(kind) {
            let id = model_id(&entry);
            if !id.is_empty() && rules.default_enabled(kind_id(kind), &id)
                && !result.iter().any(|item| model_id(item).eq_ignore_ascii_case(&id))
            {
                result.push(entry);
            }
        }
    }
    result
}

pub fn default_model_usable(model: &str) -> bool {
    entry_id_in_manifest(&default_model_catalog(), model).is_some()
}

#[cfg(test)]
mod tests {
    use super::builtin_target;
    use crate::server::core::model_rules::ModelRules;
    use crate::server::core::providers::ProviderKind;
    use serde_json::{json, Value};

    /// `from_raw` 读的是 kv 整体形态（规则包在 `modelRules` 键下），这层包装在测试里做掉。
    fn rules_of(mappings: Value) -> ModelRules {
        let object = json!({ "modelRules": { "mappings": mappings } })
            .as_object()
            .cloned()
            .expect("测试给对象");
        ModelRules::from_raw(&object)
    }

    fn raccoon_manifest() -> Vec<Value> {
        vec![json!({ "id": "sn-glm-5-3" }), json!({ "id": "sn-glm-5-3-flash" })]
    }

    /// 本名直发（alias == target 的默认绑定上挂回退）—— 这正是线上实际在用的那条配置。
    #[test]
    fn default_binding_carries_the_image_fallback() {
        let rules = rules_of(json!([
            { "alias": "sn-glm-5-3", "target": "sn-glm-5-3", "provider": "raccoon",
              "imageTarget": "sn-glm-5-3-flash" },
        ]));
        let wire = builtin_target(&rules, ProviderKind::Raccoon, &raccoon_manifest(), "sn-glm-5-3")
            .expect("本名在清单里");
        assert_eq!(wire.model, "sn-glm-5-3");
        assert_eq!(wire.image_model.as_deref(), Some("sn-glm-5-3-flash"));
    }

    /// 别名路径：回退目标按该家清单解析成真名（大小写以清单为准）。
    #[test]
    fn alias_fallback_resolves_to_manifest_spelling() {
        let rules = rules_of(json!([
            { "alias": "glm-main", "target": "sn-glm-5-3", "provider": "raccoon",
              "imageTarget": "SN-GLM-5-3-FLASH" },
        ]));
        let wire = builtin_target(&rules, ProviderKind::Raccoon, &raccoon_manifest(), "glm-main")
            .expect("别名命中");
        assert_eq!(wire.model, "sn-glm-5-3");
        assert_eq!(wire.image_model.as_deref(), Some("sn-glm-5-3-flash"));
    }

    /// 清单里解析不出的回退目标原样发（由上游报错给出可读失败），没配回退则为 None。
    #[test]
    fn unresolved_fallback_passes_through_and_missing_means_none() {
        let rules = rules_of(json!([
            { "alias": "glm-main", "target": "sn-glm-5-3", "provider": "raccoon",
              "imageTarget": "not-in-manifest" },
            { "alias": "glm-plain", "target": "sn-glm-5-3", "provider": "raccoon" },
        ]));
        let manifest = raccoon_manifest();
        let wire = builtin_target(&rules, ProviderKind::Raccoon, &manifest, "glm-main").expect("别名命中");
        assert_eq!(wire.image_model.as_deref(), Some("not-in-manifest"));
        let plain = builtin_target(&rules, ProviderKind::Raccoon, &manifest, "glm-plain").expect("别名命中");
        assert_eq!(plain.image_model, None);
    }

    /// 别家的绑定不串到这一家：catpaw 上的回退不能让 raccoon 的同名请求换模型。
    #[test]
    fn another_providers_fallback_does_not_leak() {
        let rules = rules_of(json!([
            { "alias": "sn-glm-5-3", "target": "sn-glm-5-3", "provider": "catpaw",
              "imageTarget": "sn-glm-5-3-flash" },
        ]));
        let wire = builtin_target(&rules, ProviderKind::Raccoon, &raccoon_manifest(), "sn-glm-5-3")
            .expect("本名在清单里");
        assert_eq!(wire.model, "sn-glm-5-3");
        assert_eq!(wire.image_model, None);
    }
}
