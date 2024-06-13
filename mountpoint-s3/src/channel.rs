use std::{fmt::Display, path::PathBuf, str::FromStr};

use thiserror::Error;

use crate::prefix::{Prefix, PrefixError};

#[derive(Debug, Clone, Default)]
pub struct ChannelConfig {
    pub name: String,
    pub prefix: Prefix,
}

impl ChannelConfig {
    pub fn parse(s: &str) -> Result<Self, ChannelError> {
        let Some((name, prefix)) = s.split_once(':') else {
            return Err(ChannelError::MissingSeparator);
        };
        Ok(Self {
            name: name.to_owned(),
            prefix: prefix.parse()?,
        })
    }
}

impl Display for ChannelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.prefix)
    }
}

impl FromStr for ChannelConfig {
    type Err = ChannelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ChannelConfig::parse(s)
    }
}

#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("Missing separator")]
    MissingSeparator,
    #[error("Invalid prefix")]
    InvalidPrefix(#[from] PrefixError),
}

#[derive(Debug)]
pub struct Channel {
    pub mount_point: PathBuf,
    pub bucket: String,
    pub prefix: Prefix,
}
