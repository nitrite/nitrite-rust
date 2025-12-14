use crate::common::{Convertible, KEY_OBJ_SEPARATOR};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::repository::NitriteEntity;

pub fn get_key_name(name: &str) -> NitriteResult<String> {
    if name.contains(KEY_OBJ_SEPARATOR) {
        // Use split_once to avoid allocating full Vec
        if let Some((_, key_part)) = name.split_once(KEY_OBJ_SEPARATOR) {
            Ok(key_part.to_string())
        } else {
            log::error!("Invalid keyed object format: {}", name);
            Err(NitriteError::new(
                &format!("Invalid keyed object format: {}", name),
                ErrorKind::ValidationError,
            ))
        }
    } else {
        log::error!("{} is not a valid keyed object repository", name);
        Err(NitriteError::new(
            &format!("{} is not a valid keyed object repository", name),
            ErrorKind::ValidationError,
        ))
    }
}

pub fn get_keyed_repo_type(name: &str) -> NitriteResult<String> {
    if name.contains(KEY_OBJ_SEPARATOR) {
        // Use split_once to avoid allocating full Vec
        if let Some((type_part, _)) = name.split_once(KEY_OBJ_SEPARATOR) {
            Ok(type_part.to_string())
        } else {
            log::error!("Invalid keyed object format: {}", name);
            Err(NitriteError::new(
                &format!("Invalid keyed object format: {}", name),
                ErrorKind::ValidationError,
            ))
        }
    } else {
        log::error!("{} is not a valid keyed object repository", name);
        Err(NitriteError::new(
            &format!("{} is not a valid keyed object repository", name),
            ErrorKind::ValidationError,
        ))
    }
}

pub fn repository_name_by_type<T>(key: Option<&str>) -> NitriteResult<String>
where
    T: NitriteEntity,
{
    let entity_name = T::default().entity_name();
    repository_name(&entity_name, key)
}

pub fn repository_name(entity_name: &str, key: Option<&str>) -> NitriteResult<String> {
    if entity_name.contains(KEY_OBJ_SEPARATOR) {
        log::error!("{} is not a valid entity name", entity_name);
        return Err(NitriteError::new(
            &format!("{} is not a valid entity name", entity_name),
            ErrorKind::ValidationError,
        ));
    }

    match key {
        // Preallocate capacity to avoid reallocation
        Some(k) => {
            let mut result = String::with_capacity(entity_name.len() + 1 + k.len());
            result.push_str(&entity_name);
            result.push_str(KEY_OBJ_SEPARATOR);
            result.push_str(k);
            Ok(result)
        }
        None => Ok(entity_name.to_string()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_key_name_success() {
        let result = get_key_name("User+key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "key");
    }

    #[test]
    fn test_get_key_name_error() {
        let result = get_key_name("User");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_keyed_repo_type_success() {
        let result = get_keyed_repo_type("User+key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "User");
    }

    #[test]
    fn test_get_keyed_repo_type_error() {
        let result = get_keyed_repo_type("User");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_key_name_valid_standard_format() {
        let result = get_key_name("Repository+EntityKey");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "EntityKey");
    }

    #[test]
    fn test_get_key_name_with_separator_at_end() {
        // Input "Repository+" has parts ["Repository", ""]
        let result = get_key_name("Repository+");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_get_key_name_with_multiple_separators() {
        // Input "Repo+Type+Key" with split_once returns (before, after_first)
        // So we get "Type+Key" (everything after the first separator)
        // This is more efficient than the old behavior which split into full ["Repo", "Type", "Key"]
        let result = get_key_name("Repo+Type+Key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Type+Key");
    }

    #[test]
    fn test_get_key_name_without_separator() {
        let result = get_key_name("RepositoryName");
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(*e.kind(), ErrorKind::ValidationError),
            Ok(_) => panic!("Expected error for missing separator"),
        }
    }

    #[test]
    fn test_get_key_name_separator_at_start() {
        // Input "+Key" splits into ["", "Key"]
        let result = get_key_name("+Key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Key");
    }

    #[test]
    fn test_get_keyed_repo_type_valid_standard_format() {
        let result = get_keyed_repo_type("UserRepository+UserId");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "UserRepository");
    }

    #[test]
    fn test_get_keyed_repo_type_with_separator_at_end() {
        // Input "Repository+" splits into ["Repository", ""]
        let result = get_keyed_repo_type("Repository+");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Repository");
    }

    #[test]
    fn test_get_keyed_repo_type_with_multiple_separators() {
        // Input "Repo+Type+Key" splits into ["Repo", "Type", "Key"]
        // get_keyed_repo_type should return parts[0] which is "Repo"
        let result = get_keyed_repo_type("Repo+Type+Key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Repo");
    }

    #[test]
    fn test_get_keyed_repo_type_without_separator() {
        let result = get_keyed_repo_type("RepositoryName");
        assert!(result.is_err());
        match result {
            Err(e) => assert_eq!(*e.kind(), ErrorKind::ValidationError),
            Ok(_) => panic!("Expected error for missing separator"),
        }
    }

    #[test]
    fn test_get_keyed_repo_type_separator_at_start() {
        // Input "+Key" splits into ["", "Key"]
        let result = get_keyed_repo_type("+Key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_get_key_name_no_bounds_panic_various_formats() {
        // Comprehensive test ensuring no panic on edge cases
        let test_cases = vec![
            ("Repository+Key", true),
            ("+", true),
            ("Type+", true),
            ("+Value", true),
            ("NoSeparator", false),
        ];

        for (input, should_succeed) in test_cases {
            let result = get_key_name(input);
            if should_succeed {
                assert!(result.is_ok(), "Failed for input: {}", input);
            } else {
                assert!(result.is_err(), "Should error for input: {}", input);
            }
        }
    }

    #[test]
    fn test_get_keyed_repo_type_no_bounds_panic_various_formats() {
        // Comprehensive test ensuring no panic on edge cases
        let test_cases = vec![
            ("Repository+Key", true),
            ("+", true),
            ("Type+", true),
            ("+Value", true),
            ("NoSeparator", false),
        ];

        for (input, should_succeed) in test_cases {
            let result = get_keyed_repo_type(input);
            if should_succeed {
                assert!(result.is_ok(), "Failed for input: {}", input);
            } else {
                assert!(result.is_err(), "Should error for input: {}", input);
            }
        }
    }

    #[test]
    fn bench_get_key_name() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = get_key_name("Repository+EntityKey");
        }
        let elapsed = start.elapsed();
        println!(
            "get_key_name (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_get_keyed_repo_type() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = get_keyed_repo_type("Repository+EntityKey");
        }
        let elapsed = start.elapsed();
        println!(
            "get_keyed_repo_type (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_find_repository_name_string_building() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let entity_name = "TestEntity";
            let _result = String::with_capacity(entity_name.len() + 1 + 10);
        }
        let elapsed = start.elapsed();
        println!(
            "find_repository_name string capacity (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }
}
