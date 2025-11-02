#![allow(dead_code)]

use anyhow::{Context, Result};
use testcontainers::{
    GenericBuildableImage, GenericImage, core::BuildImageOptions, runners::AsyncBuilder,
};

use super::RedisVersion;

const DOCKERFILE_TEMPLATE: &str = include_str!("../image/Dockerfile");

/// Build (or reuse) the Redis image for the provided version and return the final `GenericImage`.
pub async fn build_image(version: RedisVersion, skip_if_exists: bool) -> Result<GenericImage> {
    GenericBuildableImage::new(
        version.image_name().to_string(),
        version.image_tag().to_string(),
    )
    .with_dockerfile_string(DOCKERFILE_TEMPLATE.to_owned())
    .build_image_with(
        BuildImageOptions::new()
            .with_skip_if_exists(skip_if_exists)
            .with_build_arg("REDIS_VERSION", version.image_tag()),
    )
    .await
    .context("build redis docker image via testcontainers")
}
