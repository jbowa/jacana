use {
    crate::config::TransactionsFilterCfg,
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError, bs58,
    solana_pubkey::Pubkey, std::collections::HashSet,
};

#[derive(Debug, Clone)]
enum TransactionsFilterMode {
    Disabled,
    All { exclude_votes: bool },
    AllVotes,
    Mentions(HashSet<Pubkey>),
}

#[derive(Debug, Clone)]
pub struct TransactionsFilter {
    mode: TransactionsFilterMode,
}

impl TransactionsFilter {
    pub fn from_cfg(cfg: &TransactionsFilterCfg) -> Result<Self, GeyserPluginError> {
        if cfg.mentions.is_empty() {
            return Ok(Self {
                mode: TransactionsFilterMode::Disabled,
            });
        }

        // Check for wildcard "*" - matches all transactions.
        if cfg.mentions.iter().any(|s| s == "*") {
            return Ok(Self {
                mode: TransactionsFilterMode::All {
                    exclude_votes: cfg.exclude_votes,
                },
            });
        }

        // Check for "all_votes" - matches all vote transactions.
        if cfg
            .mentions
            .iter()
            .any(|s| s.eq_ignore_ascii_case("all_votes"))
        {
            return Ok(Self {
                mode: TransactionsFilterMode::AllVotes,
            });
        }

        let mentions = Self::parse_pubkey_list(&cfg.mentions, "mentions")?;
        let mode = if mentions.is_empty() {
            TransactionsFilterMode::Disabled
        } else {
            TransactionsFilterMode::Mentions(mentions)
        };

        Ok(Self { mode })
    }

    fn parse_pubkey_list(
        list: &[String],
        field_name: &'static str,
    ) -> Result<HashSet<Pubkey>, GeyserPluginError> {
        let mut pubkeys = HashSet::with_capacity(list.len());

        for s in list {
            // Skip special keywords.
            if s == "*" || s.eq_ignore_ascii_case("all_votes") {
                continue;
            }

            let bytes =
                bs58::decode(s)
                    .into_vec()
                    .map_err(|e| GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid base58 pubkey in filters.transactions.{}: {} ({})",
                            field_name, s, e
                        ),
                    })?;

            if bytes.len() != 32 {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: format!(
                        "pubkey in filters.transactions.{} must be 32 bytes: {}",
                        field_name, s
                    ),
                });
            }

            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            pubkeys.insert(Pubkey::new_from_array(arr));
        }

        Ok(pubkeys)
    }

    #[inline]
    pub fn matches<'a>(&self, is_vote: bool, keys: impl Iterator<Item = &'a Pubkey>) -> bool {
        match &self.mode {
            TransactionsFilterMode::Disabled => false,
            TransactionsFilterMode::All { exclude_votes } => {
                if *exclude_votes && is_vote {
                    return false;
                }
                true
            }
            TransactionsFilterMode::AllVotes => is_vote,
            TransactionsFilterMode::Mentions(set) => keys.into_iter().any(|k| set.contains(k)),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, TransactionsFilterMode::Disabled)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_pubkey::Pubkey};

    fn new_pubkey(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    fn make_cfg(mentions: Vec<&str>) -> TransactionsFilterCfg {
        TransactionsFilterCfg {
            mentions: mentions.into_iter().map(String::from).collect(),
            exclude_votes: false,
        }
    }

    fn make_cfg_ex(mentions: Vec<&str>, exclude_votes: bool) -> TransactionsFilterCfg {
        TransactionsFilterCfg {
            mentions: mentions.into_iter().map(String::from).collect(),
            exclude_votes,
        }
    }

    #[test]
    fn test_disabled_mode() {
        let cfg = make_cfg(vec![]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        assert!(!filter.is_enabled());
        assert!(!filter.matches(false, [&new_pubkey(1)].into_iter()));
        assert!(!filter.matches(true, [&new_pubkey(1)].into_iter()));
    }

    #[test]
    fn test_all_mode_with_wildcard() {
        let cfg = make_cfg(vec!["*"]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.is_enabled());
        assert!(filter.matches(false, [&new_pubkey(1)].into_iter()));
        assert!(filter.matches(true, [&new_pubkey(2)].into_iter()));
        matches!(filter.mode, TransactionsFilterMode::All { .. });
        if let TransactionsFilterMode::All { exclude_votes } = filter.mode {
            assert!(!exclude_votes);
        }
    }

    #[test]
    fn test_all_mode_exclude_votes_true() {
        let cfg = make_cfg_ex(vec!["*"], true);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.is_enabled());
        assert!(filter.matches(false, [].into_iter()));
        assert!(!filter.matches(true, [].into_iter()));
        if let TransactionsFilterMode::All { exclude_votes } = filter.mode {
            assert!(exclude_votes);
        } else {
            panic!("expected All mode");
        }
    }

    #[test]
    fn test_all_mode_exclude_votes_false() {
        let cfg = make_cfg_ex(vec!["*"], false);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.matches(false, [].into_iter()));
        assert!(filter.matches(true, [].into_iter()));
        if let TransactionsFilterMode::All { exclude_votes } = filter.mode {
            assert!(!exclude_votes);
        } else {
            panic!("expected All mode");
        }
    }

    #[test]
    fn test_all_votes_mode() {
        let cfg = make_cfg(vec!["all_votes"]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.is_enabled());
        assert!(filter.matches(true, [&new_pubkey(1)].into_iter()));
        assert!(!filter.matches(false, [&new_pubkey(1)].into_iter()));
        assert!(matches!(filter.mode, TransactionsFilterMode::AllVotes));
    }

    #[test]
    fn test_all_votes_case_insensitive() {
        let cases = vec!["all_votes", "ALL_VOTES", "All_Votes", "aLl_VoTeS"];
        for case in cases {
            let cfg = make_cfg(vec![case]);
            let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
            assert!(filter.matches(true, [&new_pubkey(1)].into_iter()));
            assert!(!filter.matches(false, [&new_pubkey(1)].into_iter()));
        }
    }

    #[test]
    fn test_mentions_mode() {
        let pk1 = new_pubkey(42);
        let pk2 = new_pubkey(99);
        let cfg = make_cfg(vec![&bs58::encode(pk1).into_string()]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.is_enabled());
        assert!(filter.matches(false, [&pk1].into_iter()));
        assert!(filter.matches(false, [&pk2, &pk1].into_iter()));
        assert!(!filter.matches(false, [&pk2].into_iter()));
        assert!(!filter.matches(false, [].into_iter()));
    }

    #[test]
    fn test_mentions_multiple_keys() {
        let pk1 = new_pubkey(10);
        let pk2 = new_pubkey(20);
        let pk3 = new_pubkey(30);

        let cfg = make_cfg(vec![
            &bs58::encode(pk1).into_string(),
            &bs58::encode(pk2).into_string(),
        ]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.matches(false, [&pk1].into_iter()));
        assert!(filter.matches(false, [&pk2].into_iter()));
        assert!(!filter.matches(false, [&pk3].into_iter()));
        assert!(filter.matches(false, [&pk3, &pk2, &pk1].into_iter()));
    }

    #[test]
    fn test_invalid_base58_pubkey() {
        let cfg = TransactionsFilterCfg {
            mentions: vec!["!".repeat(44)],
            exclude_votes: false,
        };
        let result = TransactionsFilter::from_cfg(&cfg);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_invalid_pubkey_length() {
        let cfg = TransactionsFilterCfg {
            mentions: vec!["shortkey".to_string()],
            exclude_votes: false,
        };
        let result = TransactionsFilter::from_cfg(&cfg);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_mixed_special_and_pubkeys() {
        let pk = new_pubkey(55);
        let cfg = make_cfg(vec!["*", &bs58::encode(pk).into_string()]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        // Wildcard takes precedence.
        matches!(filter.mode, TransactionsFilterMode::All { .. });
    }

    #[test]
    fn test_all_votes_with_pubkeys_ignored() {
        let pk = new_pubkey(77);
        let cfg = make_cfg(vec!["all_votes", &bs58::encode(pk).into_string()]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();
        // all_votes takes precedence (first special keyword found).
        assert!(matches!(filter.mode, TransactionsFilterMode::AllVotes));
    }

    #[test]
    fn test_empty_iterator() {
        let pk = new_pubkey(1);
        let cfg = make_cfg(vec![&bs58::encode(pk).into_string()]);
        let filter = TransactionsFilter::from_cfg(&cfg).unwrap();

        // Empty key iterator should not match.
        assert!(!filter.matches(false, [].into_iter()));
    }
}
