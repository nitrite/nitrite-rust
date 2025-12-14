// Based on Java RepositoryFactoryTest.java
use nitrite::repository::ObjectRepository;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

#[derive(Clone, Debug, Default, Convertible, NitriteEntity)]
pub struct TestEntity {
    id: Option<String>,
    name: Option<String>,
    value: Option<i32>,
}

#[test]
fn test_repository_creation() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<TestEntity> = ctx.db().repository()?;

            // Repository should be created successfully
            let _ = repo.size()?;

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_repository_with_key() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo1: ObjectRepository<TestEntity> = ctx.db().keyed_repository("test1")?;
            let repo2: ObjectRepository<TestEntity> = ctx.db().keyed_repository("test2")?;

            // Both repositories should be created independently
            assert_ne!(
                repo1.document_collection().name(),
                repo2.document_collection().name()
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_multiple_repository_instances() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo1: ObjectRepository<TestEntity> = ctx.db().repository()?;
            let repo2: ObjectRepository<TestEntity> = ctx.db().repository()?;

            // Both should refer to the same repository
            assert_eq!(
                repo1.document_collection().name(),
                repo2.document_collection().name()
            );

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_repository_name() {
    run_test(
        || create_test_context(),
        |ctx| {
            let repo: ObjectRepository<TestEntity> = ctx.db().repository()?;

            // Repository name should be set
            let name = repo.document_collection().name();
            assert!(!name.is_empty());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}
