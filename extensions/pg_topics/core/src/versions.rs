use kafka_protocol::messages::ApiKey;

pub const VERSIONS: [(ApiKey, i16, i16); 24] = [
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::SaslHandshake, 1, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::OffsetCommit, 2, 8),
    (ApiKey::OffsetFetch, 1, 8),
    (ApiKey::InitProducerId, 0, 4),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::DeleteTopics, 1, 5),
    (ApiKey::DescribeConfigs, 1, 4),
    (ApiKey::AlterConfigs, 0, 2),
    (ApiKey::IncrementalAlterConfigs, 0, 1),
    (ApiKey::DeleteGroups, 0, 2),
    (ApiKey::DescribeCluster, 0, 1),
    (ApiKey::ListGroups, 0, 4),
    (ApiKey::DescribeGroups, 0, 5),
];

pub fn supported(key: ApiKey, version: i16) -> bool {
    VERSIONS
        .iter()
        .any(|&(k, lo, hi)| k == key && (lo..=hi).contains(&version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advertised_version_is_in_the_crate_range_and_below_topic_ids() {
        let topic_ids = [
            (ApiKey::Metadata, 13),
            (ApiKey::Produce, 13),
            (ApiKey::Fetch, 13),
        ];
        for (key, lo, hi) in VERSIONS {
            let crate_range = key.valid_versions();
            assert!(
                crate_range.min <= lo && lo <= hi && hi <= crate_range.max,
                "{key:?} {lo}-{hi} is outside {crate_range}"
            );
            if let Some((_, cutoff)) = topic_ids.iter().find(|(k, _)| *k == key) {
                assert!(hi < *cutoff, "{key:?} {hi} uses topic IDs");
            }
        }
    }

    #[test]
    fn supported_checks_both_ends() {
        assert!(supported(ApiKey::Produce, 3));
        assert!(supported(ApiKey::Produce, 9));
        assert!(!supported(ApiKey::Produce, 2));
        assert!(!supported(ApiKey::Produce, 10));
        assert!(!supported(ApiKey::SaslHandshake, 0));
        assert!(supported(ApiKey::CreateTopics, 2));
        assert!(!supported(ApiKey::CreateTopics, 1));
        assert!(!supported(ApiKey::DeleteRecords, 0));
    }
}
