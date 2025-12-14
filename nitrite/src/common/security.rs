use argon2::{password_hash::SaltString, Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use rand::rngs::OsRng;
use std::sync::Arc;

use crate::{
    doc,
    errors::{ErrorKind, NitriteError, NitriteResult},
    store::{NitriteMapProvider, NitriteStore, NitriteStoreProvider},
};

use super::{Value, USER_MAP};

/// Manages user authentication and password operations.
///
/// This struct provides user authentication, password creation, validation,
/// and password update functionality using Argon2 password hashing. It manages
/// user credentials stored in the database and enforces security constraints.
///
/// # Responsibilities
///
/// * **User Authentication**: Creates new users or validates existing ones during authentication
/// * **Password Hashing**: Uses Argon2 to securely hash and verify passwords
/// * **Credential Storage**: Manages user credentials in the database
/// * **Password Updates**: Allows users to change their passwords with verification
/// * **Validation**: Ensures both username and password are provided when required
/// * **Error Handling**: Returns detailed security errors for various failure scenarios
#[derive(Clone)]
pub(crate) struct AuthService {
    inner: Arc<AuthServiceInner>,
}

impl AuthService {
    /// Creates a new authentication service instance.
    pub(crate) fn new(store: NitriteStore) -> Self {
        AuthService {
            inner: Arc::new(AuthServiceInner::new(store.clone())),
        }
    }

    /// Authenticates a user with username and password.
    ///
    /// If both username and password are provided and no users exist, creates a new user.
    /// If users exist, validates the provided credentials against stored user.
    /// If neither username nor password are provided, succeeds only if no users exist.
    pub(crate) fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
    ) -> NitriteResult<()> {
        self.inner.authenticate(username, password)
    }

    /// Updates a user's password with verification of the old password.
    pub(crate) fn update_password(
        &self,
        username: &str,
        old_password: &str,
        new_password: &str,
    ) -> NitriteResult<()> {
        self.inner.update_password(username, old_password, new_password)
    }

    /// Adds a new user or updates an existing user's password.
    pub(crate) fn add_update_password(
        &self,
        username: &str,
        old_password: &str,
        new_password: &str,
        update: bool,
    ) -> NitriteResult<()> {
        self.inner.add_update_password(username, old_password, new_password, update)
    }
}

/// Inner implementation of the authentication service.
pub(crate) struct AuthServiceInner {
    store: NitriteStore,
}

impl AuthServiceInner {
    #[inline]
    pub(crate) fn new(store: NitriteStore) -> Self {
        AuthServiceInner { store }
    }

    #[inline]
    pub(crate) fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
    ) -> NitriteResult<()> {
        let existing_user = self.store.has_map(USER_MAP)?;

        match (username, password) {
            (Some(u), Some(p)) => {
                if !existing_user {
                    self.create_user(u, p)
                } else {
                    self.validate_user(u, p)
                }
            }
            (None, None) => {
                if existing_user {
                    log::error!("Username or password is invalid");
                    Err(NitriteError::new(
                        "Username or password is invalid",
                        ErrorKind::SecurityError,
                    ))
                } else {
                    Ok(())
                }
            }
            _ => {
                log::error!("Username or password is invalid");
                Err(NitriteError::new(
                    "Username or password is invalid",
                    ErrorKind::SecurityError,
                ))
            }
        }
    }

    pub(crate) fn add_update_password(
        &self,
        username: &str,
        old_password: &str,
        new_password: &str,
        update: bool,
    ) -> NitriteResult<()> {
        if update {
            return self.update_password(username, old_password, new_password);
        }
        self.create_user(username, new_password)
    }

    #[inline]
    fn create_user(&self, username: &str, password: &str) -> NitriteResult<()> {
        let user_map = self.store.open_map(USER_MAP)?;
        let password = password.as_bytes();

        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2.hash_password(password, &salt);

        match hash {
            Ok(hash) => {
                let doc = doc!{
                    "hash": (hash.to_string()),
                };

                user_map.put(Value::from(username), Value::from(doc))?;
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to create user: {:?}", e);
                Err(NitriteError::new(
                    "Username or password is invalid",
                    ErrorKind::SecurityError,
                ))
            },
        }
    }

    #[inline]
    fn validate_user(&self, username: &str, password: &str) -> NitriteResult<()> {
        let user_map = self.store.open_map(USER_MAP)?;
        let credential_doc = user_map.get(&Value::from(username))?;

        if credential_doc.is_none() {
            log::error!("Username or password is invalid");
            return Err(NitriteError::new(
                "Username or password is invalid",
                ErrorKind::SecurityError,
            ));
        }

        // Validate credential is a Document before accessing
        let credential_doc = match credential_doc.unwrap().as_document() {
            Some(doc) => doc.clone(),
            None => {
                log::error!("User credential is not a valid document: {}", username);
                return Err(NitriteError::new(
                    "Invalid user credential format",
                    ErrorKind::SecurityError,
                ));
            }
        };

        let binding = credential_doc.get("hash")?;
        let expected_hash = binding.as_string().map_or("", |v| v);

        let password = password.as_bytes();
        let parsed_hash = PasswordHash::new(expected_hash);

        match parsed_hash {
            Ok(parsed_hash) => {
                let result = Argon2::default().verify_password(password, &parsed_hash);
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        log::error!("Username or password is invalid: {:?}", e);
                        Err(NitriteError::new(
                            "Username or password is invalid",
                            ErrorKind::SecurityError,
                        ))
                    },
                }
            }
            Err(e) => {
                log::error!("Username or password is invalid: {:?}", e);
                Err(NitriteError::new(
                    "Username or password is invalid",
                    ErrorKind::SecurityError,
                ))
            }
        }
    }

    #[inline]
    pub(crate) fn update_password(
        &self,
        username: &str,
        old_password: &str,
        new_password: &str,
    ) -> NitriteResult<()> {
        let user_map = self.store.open_map(USER_MAP)?;
        let credential_doc = user_map.get(&Value::from(username))?;

        if credential_doc.is_none() {
            log::error!("Username or password is invalid");
            return Err(NitriteError::new(
                "Username or password is invalid",
                ErrorKind::SecurityError,
            ));
        }

        // Validate credential is a Document before accessing
        let credential_doc = match credential_doc.unwrap().as_document() {
            Some(doc) => doc.clone(),
            None => {
                log::error!("User credential is not a valid document: {}", username);
                return Err(NitriteError::new(
                    "Invalid user credential format",
                    ErrorKind::SecurityError,
                ));
            }
        };

        let binding = credential_doc.get("hash")?;
        let expected_hash = binding.as_string().map_or("", |v| v);

        let old_password = old_password.as_bytes();
        let new_password = new_password.as_bytes();
        let parsed_hash = PasswordHash::new(expected_hash);

        match parsed_hash {
            Ok(parsed_hash) => {
                let result = Argon2::default().verify_password(old_password, &parsed_hash);
                match result {
                    Ok(_) => {
                        let salt = SaltString::generate(&mut OsRng);
                        let argon2 = Argon2::default();
                        let hash = argon2.hash_password(new_password, &salt);

                        match hash {
                            Ok(hash) => {
                                let doc = doc!{
                                    "hash": (hash.to_string()),
                                };

                                user_map.put(Value::from(username), Value::from(doc))?;
                                Ok(())
                            }
                            Err(e) => {
                                log::error!("Failed to update password: {:?}", e);
                                Err(NitriteError::new(
                                    "Username or password is invalid",
                                    ErrorKind::SecurityError,
                                ))
                            },
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to update password: {:?}", e);
                        Err(NitriteError::new(
                            "Username or password is invalid",
                            ErrorKind::SecurityError,
                        ))
                    },
                }
            }
            Err(e) => {
                log::error!("Failed to update password: {:?}", e);
                Err(NitriteError::new(
                    "Username or password is invalid",
                    ErrorKind::SecurityError,
                ))
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::nitrite_config::NitriteConfig;

    fn setup_nitrite_store() -> NitriteStore {
        let nitrite_config = NitriteConfig::default();
        nitrite_config.auto_configure().expect("Failed to auto configure nitrite");
        nitrite_config.initialize().expect("Failed to initialize nitrite");
        nitrite_config.nitrite_store().expect("Failed to get nitrite store")
    }
    
    fn setup_auth_service() -> AuthService {
        AuthService::new(setup_nitrite_store())
    }

    #[test]
    fn test_authenticate_create_user() {
        let auth_service = setup_auth_service();
        let result = auth_service.authenticate(Some("user1"), Some("password1"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_validate_user() {
        let auth_service = setup_auth_service();
        auth_service.authenticate(Some("user1"), Some("password1")).unwrap();
        let result = auth_service.authenticate(Some("user1"), Some("password1"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_invalid_user() {
        let auth_service = setup_auth_service();
        let result = auth_service.authenticate(Some("user1"), Some("password1"));
        assert!(result.is_ok());
        let result = auth_service.authenticate(Some("user1"), Some("wrongpassword"));
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_missing_username() {
        let auth_service = setup_auth_service();
        let result = auth_service.authenticate(None, Some("password1"));
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_missing_password() {
        let auth_service = setup_auth_service();
        let result = auth_service.authenticate(Some("user1"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_password_success() {
        let auth_service = setup_auth_service();
        auth_service.authenticate(Some("user1"), Some("password1")).unwrap();
        let result = auth_service.update_password("user1", "password1", "newpassword1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_password_invalid_old_password() {
        let auth_service = setup_auth_service();
        auth_service.authenticate(Some("user1"), Some("password1")).unwrap();
        let result = auth_service.update_password("user1", "wrongpassword", "newpassword1");
        assert!(result.is_err());
    }

    #[test]
    fn test_update_password_nonexistent_user() {
        let auth_service = setup_auth_service();
        let result = auth_service.update_password("nonexistent", "password1", "newpassword1");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_user() {
        let auth_service_inner = AuthServiceInner::new(setup_nitrite_store());
        let result = auth_service_inner.create_user("user1", "password1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_user_success() {
        let auth_service_inner = AuthServiceInner::new(setup_nitrite_store());
        auth_service_inner.create_user("user1", "password1").unwrap();
        let result = auth_service_inner.validate_user("user1", "password1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_user_invalid_password() {
        let auth_service_inner = AuthServiceInner::new(setup_nitrite_store());
        auth_service_inner.create_user("user1", "password1").unwrap();
        let result = auth_service_inner.validate_user("user1", "wrongpassword");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_user_nonexistent_user() {
        let auth_service_inner = AuthServiceInner::new(setup_nitrite_store());
        let result = auth_service_inner.validate_user("nonexistent", "password1");
        assert!(result.is_err());
    }

    #[test]
    fn test_update_password_with_corrupted_credential_string() {
        // Test that update_password gracefully handles non-Document credential values
        let store = setup_nitrite_store();
        let user_map = store.open_map(USER_MAP).unwrap();
        
        // Store corrupted credential (a string instead of a document)
        user_map.put(Value::from("user1"), Value::String("corrupted".to_string())).unwrap();
        
        let auth_service_inner = AuthServiceInner::new(store);
        let result = auth_service_inner.update_password("user1", "oldpass", "newpass");
        
        // Should error gracefully instead of panicking
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::SecurityError);
        }
    }

    #[test]
    fn test_update_password_with_corrupted_credential_number() {
        // Test that update_password handles numeric credential values
        let store = setup_nitrite_store();
        let user_map = store.open_map(USER_MAP).unwrap();
        
        // Store corrupted credential (a number instead of a document)
        user_map.put(Value::from("user2"), Value::I32(12345)).unwrap();
        
        let auth_service_inner = AuthServiceInner::new(store);
        let result = auth_service_inner.update_password("user2", "oldpass", "newpass");
        
        // Should error gracefully instead of panicking
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::SecurityError);
        }
    }

    #[test]
    fn test_validate_user_with_corrupted_credential_string() {
        // Test that validate_user gracefully handles non-Document credential values
        let store = setup_nitrite_store();
        let user_map = store.open_map(USER_MAP).unwrap();
        
        // Store corrupted credential (a string instead of a document)
        user_map.put(Value::from("user3"), Value::String("corrupted_hash".to_string())).unwrap();
        
        let auth_service_inner = AuthServiceInner::new(store);
        let result = auth_service_inner.validate_user("user3", "password1");
        
        // Should error gracefully instead of panicking
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::SecurityError);
        }
    }

    #[test]
    fn test_validate_user_with_corrupted_credential_null() {
        // Test that validate_user handles null credential values
        let store = setup_nitrite_store();
        let user_map = store.open_map(USER_MAP).unwrap();
        
        // Store null credential instead of a document
        user_map.put(Value::from("user4"), Value::Null).unwrap();
        
        let auth_service_inner = AuthServiceInner::new(store);
        let result = auth_service_inner.validate_user("user4", "password1");
        
        // Should error gracefully instead of panicking
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::SecurityError);
        }
    }

    #[test]
    fn test_update_password_after_successful_creation() {
        // Integration test: create user, then update password successfully
        let auth_service = setup_auth_service();
        auth_service.authenticate(Some("user5"), Some("initial_password")).unwrap();
        
        // Update password from initial to new
        let result = auth_service.update_password("user5", "initial_password", "updated_password");
        assert!(result.is_ok());
        
        // Verify new password works
        let result = auth_service.authenticate(Some("user5"), Some("updated_password"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_user_after_update_password() {
        // Integration test: update password then validate with new password
        let store = setup_nitrite_store();
        let auth_service = AuthService::new(store.clone());
        
        // Create initial user
        auth_service.authenticate(Some("user6"), Some("password1")).unwrap();
        
        // Update password
        auth_service.update_password("user6", "password1", "password2").unwrap();
        
        // Validate should work with new password
        let auth_service_inner = AuthServiceInner::new(store);
        let result = auth_service_inner.validate_user("user6", "password2");
        assert!(result.is_ok());
        
        // Old password should fail
        let result = auth_service_inner.validate_user("user6", "password1");
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_with_both_credentials_provided() {
        // Test match pattern handles (Some, Some) case correctly
        let auth_service = setup_auth_service();
        
        // Should create user successfully with both credentials
        let result = auth_service.authenticate(Some("test_user"), Some("test_pass"));
        assert!(result.is_ok());
        
        // Verify user was created by authenticating again with same credentials
        let result = auth_service.authenticate(Some("test_user"), Some("test_pass"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_with_no_credentials_no_existing_users() {
        // Test match pattern handles (None, None) case with no existing users
        let store = setup_nitrite_store();
        let auth_service = AuthService::new(store);
        
        // Should succeed when no users exist and no credentials provided
        let result = auth_service.authenticate(None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_with_no_credentials_existing_users() {
        // Test match pattern handles (None, None) case with existing users
        let auth_service = setup_auth_service();
        
        // Create a user first
        auth_service.authenticate(Some("existing_user"), Some("password")).unwrap();
        
        // Should fail when users exist but no credentials provided
        let result = auth_service.authenticate(None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_with_only_username() {
        // Test match pattern handles (Some, None) case - should reject
        let auth_service = setup_auth_service();
        
        let result = auth_service.authenticate(Some("user"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate_with_only_password() {
        // Test match pattern handles (None, Some) case - should reject
        let auth_service = setup_auth_service();
        
        let result = auth_service.authenticate(None, Some("password"));
        assert!(result.is_err());
    }

    #[test]
    fn bench_auth_service_creation() {
        for _ in 0..100 {
            let _ = setup_auth_service();
        }
    }

    #[test]
    fn bench_user_creation_and_validation() {
        let auth_service = setup_auth_service();
        
        for i in 0..50 {
            let username = format!("bench_user_{}", i);
            let password = format!("bench_pass_{}", i);
            let _ = auth_service.authenticate(Some(&username), Some(&password));
            let _ = auth_service.authenticate(Some(&username), Some(&password));
        }
    }

    #[test]
    fn bench_password_update() {
        let auth_service = setup_auth_service();
        auth_service.authenticate(Some("bench_user"), Some("initial")).unwrap();
        
        for i in 0..20 {
            let new_pass = format!("updated_{}", i);
            let old_pass = if i == 0 { "initial" } else { &format!("updated_{}", i-1) };
            let _ = auth_service.update_password("bench_user", old_pass, &new_pass);
        }
    }
}