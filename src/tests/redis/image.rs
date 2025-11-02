#![allow(dead_code)]

use anyhow::{Context, Result};
use semver::Version;
use testcontainers::{
    GenericBuildableImage, GenericImage, core::BuildImageOptions, runners::AsyncBuilder,
};

use super::version::REDIS_IMAGE_NAME;

const DOCKERFILE_TEMPLATE: &str = include_str!("../image/Dockerfile");

/// Build (or reuse) the Redis image for the provided version and return the final `GenericImage`.
pub async fn build_image(version: &Version, skip_if_exists: bool) -> Result<GenericImage> {
    let tag = version.to_string();
    GenericBuildableImage::new(REDIS_IMAGE_NAME.to_string(), tag.clone())
        .with_dockerfile_string(DOCKERFILE_TEMPLATE.to_owned())
        .build_image_with(
            BuildImageOptions::new()
                .with_skip_if_exists(skip_if_exists)
                .with_build_arg("REDIS_VERSION", tag),
        )
        .await
        .context("build redis docker image via testcontainers")
}
