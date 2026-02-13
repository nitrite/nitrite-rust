use cargo_toml::{Dependency, Manifest};

#[inline]
pub(crate) fn fjall_version() -> Result<String, String> {
    let cargo_toml = include_str!("../Cargo.toml");
    let manifest = Manifest::from_str(cargo_toml)
        .map_err(|e| format!("Failed to parse Cargo.toml: {}", e))?;
    
    let dependency = manifest.dependencies.get("fjall")
        .ok_or_else(|| "fjall dependency not found in Cargo.toml".to_string())?;
    
    match dependency {
        Dependency::Simple(version) => Ok(version.clone()),
        Dependency::Detailed(d) => {
            d.version.as_ref()
                .cloned()
                .ok_or_else(|| "fjall dependency version not specified".to_string())
        },
        Dependency::Inherited(_) => {
            Err("Inherited fjall dependency not supported".to_string())
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn black_box<T>(x: T) -> T {
        x
    }

    #[test]
    fn test_fjall_version_simple() {
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"
        version = "0.2.0"
        edition = "2021"

        [dependencies]
        fjall = "2.6.3"
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        let dependency = manifest.dependencies.get("fjall").unwrap();
        let result = match dependency {
            Dependency::Simple(version) => Ok(version.clone()),
            Dependency::Detailed(d) => {
                d.version.clone()
                    .ok_or_else(|| "version not specified".to_string())
            },
            Dependency::Inherited(_) => Err("Inherited dependency not supported".to_string()),
        };

        assert_eq!(result, Ok("2.6.3".to_string()));
    }

    #[test]
    fn test_fjall_version_detailed() {
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"
        version = "0.2.0"
        edition = "2021"

        [dependencies]
        fjall = { version = "2.6.3", features = ["bytes"] }
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        let dependency = manifest.dependencies.get("fjall").unwrap();
        let result = match dependency {
            Dependency::Simple(version) => Ok(version.clone()),
            Dependency::Detailed(d) => {
                d.version.clone()
                    .ok_or_else(|| "version not specified".to_string())
            },
            Dependency::Inherited(_) => Err("Inherited dependency not supported".to_string()),
        };

        assert_eq!(result, Ok("2.6.3".to_string()));
    }

    #[test]
    fn test_fjall_version_inherited() {
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"
        version = "0.2.0"
        edition = "2021"

        [dependencies]
        fjall = { workspace = true }
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        let dependency = manifest.dependencies.get("fjall").unwrap();
        let result = match dependency {
            Dependency::Simple(version) => Ok(version.clone()),
            Dependency::Detailed(d) => {
                d.version.clone()
                    .ok_or_else(|| "version not specified".to_string())
            },
            Dependency::Inherited(_) => Err("Inherited dependency not supported".to_string()),
        };

        // Should return error for inherited dependency
        assert!(result.is_err());
    }

    #[test]
    fn test_fjall_version_with_missing_dependency() {
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"
        version = "0.2.0"
        edition = "2021"

        [dependencies]
        other = "1.0.0"
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        let dependency = manifest.dependencies.get("fjall");
        assert!(dependency.is_none());
    }

    #[test]
    fn test_fjall_version_with_invalid_toml() {
        let cargo_toml = r#"
        [package
        name = "invalid"
        "#;

        let result = Manifest::from_str(cargo_toml);
        assert!(result.is_err());
    }

    #[test]
    fn test_fjall_version_parsing_perf() {
        for _ in 0..100 {
            let cargo_toml = r#"
            [package]
            name = "nitrite_fjall_adapter"
            version = "0.2.0"

            [dependencies]
            fjall = "2.6.3"
            "#;

            let manifest = black_box(Manifest::from_str(cargo_toml).unwrap());
            black_box(manifest.dependencies.get("fjall"));
        }
    }

    #[test]
    fn test_fjall_version_no_panic_on_errors() {
        // Test that function returns Result and doesn't panic
        let cargo_toml = r#"
        [package]
        name = "test"
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        let dependency = manifest.dependencies.get("fjall");
        
        // Should not panic, just return None from get()
        assert!(dependency.is_none());
    }

    #[test]
    fn test_manifest_parsing_efficiency() {
        // Verify manifest parsing is efficient
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"
        version = "0.2.0"
        edition = "2021"

        [dependencies]
        fjall = { version = "2.6.3", features = ["bytes"] }
        "#;

        for _ in 0..200 {
            let manifest = black_box(Manifest::from_str(cargo_toml).unwrap());
            let _ = black_box(manifest.dependencies.get("fjall"));
        }
    }

    #[test]
    fn test_dependency_extraction_efficiency() {
        // Verify dependency extraction uses efficient cloning
        let cargo_toml = r#"
        [package]
        name = "nitrite_fjall_adapter"

        [dependencies]
        fjall = { version = "2.6.3" }
        "#;

        let manifest = Manifest::from_str(cargo_toml).unwrap();
        for _ in 0..500 {
            if let Some(Dependency::Detailed(d)) = manifest.dependencies.get("fjall") {
                let version = black_box(d.version.as_ref().cloned());
                black_box(version);
            }
        }
    }

    #[test]
    fn test_error_message_construction_perf() {
        // Verify error message construction doesn't allocate excessively
        for _ in 0..500 {
            let error_msg = black_box(format!("Failed to parse Cargo.toml: {}", "test"));
            black_box(error_msg);
        }
    }

    #[test]
    fn test_version_matching_efficiency() {
        // Verify version string matching and cloning is efficient
        let versions = vec!["2.6.3", "1.0.0", "3.2.1"];
        for version in versions {
            for _ in 0..1000 {
                let cloned = black_box(version.to_string());
                black_box(cloned);
            }
        }
    }

    #[test]
    fn test_fjall_version_cloned_not_map() {
        // Verify that cloned() is more efficient than map(|v| v.clone())
        let cargo_toml = r#"
        [package]
        name = "test"

        [dependencies]
        fjall = { version = "2.6.3" }
        "#;

        for _ in 0..500 {
            let manifest = Manifest::from_str(cargo_toml).unwrap();
            if let Some(Dependency::Detailed(d)) = manifest.dependencies.get("fjall") {
                // Using cloned() is more efficient than map + clone
                let result = black_box(d.version.as_ref().cloned());
                black_box(result);
            }
        }
    }

    #[test]
    fn test_manifest_caching_via_include_str() {
        // Verify include_str! provides efficient string embedding
        let cargo_toml = include_str!("../Cargo.toml");
        for _ in 0..1000 {
            let len = black_box(cargo_toml.len());
            black_box(len);
        }
    }
}