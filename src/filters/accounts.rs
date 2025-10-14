use {
    crate::config::AccountsFilterCfg,
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError, bs58,
    solana_pubkey::Pubkey, std::collections::HashSet,
};

#[derive(Debug, Clone)]
enum AccountsFilterMode {
    Disabled,
    All,
    Addresses(HashSet<Pubkey>),
    Programs(HashSet<Pubkey>),
    AddressesOrPrograms {
        addresses: HashSet<Pubkey>,
        programs: HashSet<Pubkey>,
    },
}

#[derive(Debug, Clone)]
pub struct AccountsFilter {
    mode: AccountsFilterMode,
}

impl AccountsFilter {
    pub fn from_cfg(cfg: &AccountsFilterCfg) -> Result<Self, GeyserPluginError> {
        // Check for wildcard in either field.
        let has_wildcard =
            cfg.addresses.iter().any(|s| s == "*") || cfg.programs.iter().any(|s| s == "*");

        if has_wildcard {
            return Ok(Self {
                mode: AccountsFilterMode::All,
            });
        }

        let addresses = Self::parse_pubkey_list(&cfg.addresses, "addresses")?;
        let programs = Self::parse_pubkey_list(&cfg.programs, "programs")?;
        let mode = match (addresses.is_empty(), programs.is_empty()) {
            (true, true) => AccountsFilterMode::Disabled,
            (false, true) => AccountsFilterMode::Addresses(addresses),
            (true, false) => AccountsFilterMode::Programs(programs),
            (false, false) => AccountsFilterMode::AddressesOrPrograms {
                addresses,
                programs,
            },
        };

        Ok(Self { mode })
    }

    fn parse_pubkey_list(
        list: &[String],
        field_name: &'static str,
    ) -> Result<HashSet<Pubkey>, GeyserPluginError> {
        let mut pubkeys = HashSet::with_capacity(list.len());

        for s in list {
            if s == "*" {
                continue;
            }

            let bytes =
                bs58::decode(s)
                    .into_vec()
                    .map_err(|e| GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid base58 pubkey in filters.accounts.{}: {} ({})",
                            field_name, s, e
                        ),
                    })?;

            if bytes.len() != 32 {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: format!(
                        "pubkey in filters.accounts.{} must be 32 bytes: {}",
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
    pub fn matches(&self, account_pubkey: &Pubkey, owner_pubkey: &Pubkey) -> bool {
        match &self.mode {
            AccountsFilterMode::Disabled => false,
            AccountsFilterMode::All => true,
            AccountsFilterMode::Addresses(set) => set.contains(account_pubkey),
            AccountsFilterMode::Programs(set) => set.contains(owner_pubkey),
            AccountsFilterMode::AddressesOrPrograms {
                addresses,
                programs,
            } => addresses.contains(account_pubkey) || programs.contains(owner_pubkey),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, AccountsFilterMode::Disabled)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_pubkey::Pubkey};

    fn new_pubkey(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    fn make_cfg(addresses: Vec<&str>, programs: Vec<&str>) -> AccountsFilterCfg {
        AccountsFilterCfg {
            addresses: addresses.into_iter().map(String::from).collect(),
            programs: programs.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn test_disabled_mode() {
        let cfg = make_cfg(vec![], vec![]);
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();
        assert!(!filter.is_enabled());
        assert!(!filter.matches(&new_pubkey(1), &new_pubkey(2)));
    }

    #[test]
    fn test_all_mode_with_wildcard() {
        let cfg = make_cfg(vec!["*"], vec![]);
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.is_enabled());
        assert!(filter.matches(&new_pubkey(10), &new_pubkey(20)));

        let cfg2 = make_cfg(vec![], vec!["*"]);
        let filter2 = AccountsFilter::from_cfg(&cfg2).unwrap();
        assert!(filter2.matches(&new_pubkey(5), &new_pubkey(6)));

        // Wildcard in either should enable "All".
        let cfg3 = make_cfg(vec!["*"], vec!["*"]);
        let filter3 = AccountsFilter::from_cfg(&cfg3).unwrap();
        assert!(matches!(filter3.mode, AccountsFilterMode::All));
    }

    #[test]
    fn test_addresses_only_mode() {
        let addr1 = new_pubkey(42);
        let cfg = make_cfg(vec![&bs58::encode(addr1).into_string()], vec![]);
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.is_enabled());
        assert!(filter.matches(&addr1, &new_pubkey(99)));
        assert!(!filter.matches(&new_pubkey(88), &new_pubkey(99)));
    }

    #[test]
    fn test_programs_only_mode() {
        let program1 = new_pubkey(55);
        let cfg = make_cfg(vec![], vec![&bs58::encode(program1).into_string()]);
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.is_enabled());
        assert!(filter.matches(&new_pubkey(99), &program1));
        assert!(!filter.matches(&new_pubkey(1), &new_pubkey(2)));
    }

    #[test]
    fn test_addresses_or_programs_mode() {
        let addr = new_pubkey(10);
        let program = new_pubkey(20);
        let cfg = make_cfg(
            vec![&bs58::encode(addr).into_string()],
            vec![&bs58::encode(program).into_string()],
        );
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.is_enabled());
        assert!(filter.matches(&addr, &new_pubkey(99))); // address match.
        assert!(filter.matches(&new_pubkey(88), &program)); // program match.
        assert!(!filter.matches(&new_pubkey(77), &new_pubkey(66))); // no match.
    }

    #[test]
    fn test_invalid_pubkey_length() {
        let cfg = AccountsFilterCfg {
            addresses: vec!["shortkey".to_string()],
            programs: vec![],
        };
        let result = AccountsFilter::from_cfg(&cfg);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_invalid_base58_pubkey() {
        let cfg = AccountsFilterCfg {
            addresses: vec!["!".repeat(44)], // invalid base58 chars.
            programs: vec![],
        };
        let result = AccountsFilter::from_cfg(&cfg);
        assert!(matches!(
            result,
            Err(GeyserPluginError::ConfigFileReadError { .. })
        ));
    }

    #[test]
    fn test_star_ignored_in_parse_pubkey_list() {
        // "*" should be skipped without error.
        let cfg = make_cfg(vec!["*"], vec![]);
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();
        assert!(filter.is_enabled());
        assert!(matches!(filter.mode, AccountsFilterMode::All));
    }

    #[test]
    fn test_multiple_valid_pubkeys() {
        let pk1 = new_pubkey(1);
        let pk2 = new_pubkey(2);
        let cfg = make_cfg(
            vec![
                &bs58::encode(pk1).into_string(),
                &bs58::encode(pk2).into_string(),
            ],
            vec![],
        );
        let filter = AccountsFilter::from_cfg(&cfg).unwrap();

        assert!(filter.matches(&pk1, &new_pubkey(99)));
        assert!(filter.matches(&pk2, &new_pubkey(99)));
        assert!(!filter.matches(&new_pubkey(3), &new_pubkey(4)));
    }
}
