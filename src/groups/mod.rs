//! Consumer groups: the coordinator state machine and how group ids appear in Git refs.

use crate::broker::validate_topic_name;

/// A group id as one ref path component: raw when it is a valid topic-style name,
/// otherwise `%` + lowercase hex of its UTF-8 bytes. `%` never occurs in a valid
/// name, so the two forms cannot collide, and Git accepts it in ref names.
pub fn group_ref_component(group: &str) -> String {
    if validate_topic_name(group).is_ok() {
        group.to_string()
    } else {
        let hex: String = group.bytes().map(|b| format!("{b:02x}")).collect();
        format!("%{hex}")
    }
}

/// Inverse of [`group_ref_component`]; `None` for components kommit never writes.
pub fn group_from_ref_component(component: &str) -> Option<String> {
    let Some(hex) = component.strip_prefix('%') else {
        return validate_topic_name(component)
            .is_ok()
            .then(|| component.to_string());
    };
    if hex.len() % 2 != 0 {
        return None;
    }
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_safe_group_ids_are_used_raw() {
        assert_eq!(group_ref_component("billing"), "billing");
        assert_eq!(
            group_ref_component("console-consumer-42"),
            "console-consumer-42"
        );
    }

    #[test]
    fn other_group_ids_are_percent_hex() {
        assert_eq!(group_ref_component("my group"), "%6d792067726f7570");
        assert_eq!(group_ref_component(""), "%");
        assert_eq!(group_ref_component("a..b"), "%612e2e62");
    }

    #[test]
    fn components_decode_back() {
        for g in ["billing", "my group", "", "a..b", "über"] {
            assert_eq!(
                group_from_ref_component(&group_ref_component(g)).as_deref(),
                Some(g)
            );
        }
        assert_eq!(group_from_ref_component("%zz"), None);
    }
}
