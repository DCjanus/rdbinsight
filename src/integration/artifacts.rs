use semver::Version;

use crate::parser::Item;

#[derive(Debug)]
pub struct ParsedRdbArtifacts {
    redis_version: Version,
    items: Vec<Item>,
}

impl ParsedRdbArtifacts {
    pub fn new(redis_version: Version, items: Vec<Item>) -> Self {
        Self {
            redis_version,
            items,
        }
    }

    pub fn redis_version(&self) -> &Version {
        &self.redis_version
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }
}
