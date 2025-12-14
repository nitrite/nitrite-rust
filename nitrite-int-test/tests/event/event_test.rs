use nitrite::collection::{insert_if_absent, CollectionEventInfo, CollectionEventListener, CollectionEvents};
use nitrite::doc;
use nitrite::filter::field;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn wait_for_event<F: Fn() -> bool>(timeout_ms: u64, check: F) {
    awaitility::at_most(Duration::from_millis(timeout_ms)).until(check);
}

#[derive(Clone, Debug, Default, Convertible, NitriteEntity)]
struct Employee {
    emp_id: i64,
    address: Option<String>,
}

#[test]
fn test_insert_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let action = Arc::new(Mutex::new(None));
            let item = Arc::new(Mutex::new(None));

            let action_clone = action.clone();
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *action_clone.lock().unwrap() = Some(event.event_type());
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            let emp = Employee { emp_id: 1, address: None };
            collection.insert(emp)?;

            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Insert));

            assert_eq!(*action.lock().unwrap(), Some(CollectionEvents::Insert));
            assert!(item.lock().unwrap().is_some());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_update_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let action = Arc::new(Mutex::new(None));
            let item = Arc::new(Mutex::new(None));

            let action_clone = action.clone();
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *action_clone.lock().unwrap() = Some(event.event_type());
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            let mut emp = Employee { emp_id: 1, address: Some("abcd".to_string()) };
            collection.insert(emp.clone())?;
            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Insert));
            assert!(item.lock().unwrap().is_some());

            emp.address = Some("xyz".to_string());
            collection.update(field("emp_id").eq(1), emp)?;
            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Update));
            assert!(item.lock().unwrap().is_some());

            let doc = collection.find(field("emp_id").eq(1))?.first().unwrap();
            assert_eq!(doc.unwrap().address, Some("xyz".to_string()));
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_upsert_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let action = Arc::new(Mutex::new(None));
            let item = Arc::new(Mutex::new(None));

            let action_clone = action.clone();
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *action_clone.lock().unwrap() = Some(event.event_type());
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            let emp = Employee { emp_id: 1, address: Some("abcd".to_string()) };
            collection.update_with_options(field("emp_id").eq(1), emp, &insert_if_absent())?;

            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Insert));
            assert!(item.lock().unwrap().is_some());
            Ok(())
        },
        |ctx| cleanup(ctx),
    );
}

#[test]
fn test_delete_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let action = Arc::new(Mutex::new(None));
            let item = Arc::new(Mutex::new(None));

            let action_clone = action.clone();
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *action_clone.lock().unwrap() = Some(event.event_type());
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            let emp = Employee { emp_id: 1, address: Some("abcd".to_string()) };
            collection.insert(emp)?;
            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Insert));
            
            collection.remove(field("emp_id").eq(1), false)?;
            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Remove));
            
            assert_eq!(*action.lock().unwrap(), Some(CollectionEvents::Remove));
            assert!(item.lock().unwrap().is_some());

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_drop_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let item = Arc::new(Mutex::new(None));
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            collection.dispose()?;
            assert!(item.lock().unwrap().is_none());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_close_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("employee")?;
            let item = Arc::new(Mutex::new(None));
            let item_clone = item.clone();
            collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            collection.close()?;
            
            assert!(item.lock().unwrap().is_none());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_deregister_event() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let action = Arc::new(Mutex::new(None));
            let item = Arc::new(Mutex::new(None));

            let action_clone = action.clone();
            let item_clone = item.clone();
            let sub_id = collection.subscribe(CollectionEventListener::new(move |event: CollectionEventInfo| {
                *action_clone.lock().unwrap() = Some(event.event_type());
                *item_clone.lock().unwrap() = event.item().clone();
                Ok(())
            }))?;

            let emp = Employee { emp_id: 1, address: Some("abcd".to_string()) };
            collection.insert(emp.clone())?;
            wait_for_event(1000, || *action.lock().unwrap() == Some(CollectionEvents::Insert));
            assert!(item.lock().unwrap().is_some());
            // reset the action and item
            *action.lock().unwrap() = None;
            *item.lock().unwrap() = None;

            collection.unsubscribe(sub_id.unwrap())?;

            let emp = Employee { emp_id: 2, address: Some("xyz".to_string()) };
            collection.insert(emp)?;
            thread::sleep(Duration::from_millis(100));
            assert!(action.lock().unwrap().is_none());
            assert!(item.lock().unwrap().is_none());
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_multiple_listeners() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().repository::<Employee>()?;
            let count = Arc::new(Mutex::new(0));
            let count_clone = count.clone();

            collection.subscribe(CollectionEventListener::new(move |_| {
                let mut count_lock = count_clone.lock().unwrap();
                *count_lock += 1;
                Ok(())
            }))?;
            
            let count_clone = count.clone();
            collection.subscribe(CollectionEventListener::new(move |_| {
                let mut count_lock = count_clone.lock().unwrap();
                *count_lock += 1;
                Ok(())
            }))?;

            let emp = Employee { emp_id: 1, address: Some("abcd".to_string()) };
            collection.insert(emp)?;
            wait_for_event(1000, || *count.lock().unwrap() == 2);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_single_event_listener() {
    run_test(
        || create_test_context(),
        |ctx| {
            let collection = ctx.db().collection("employee")?;
            let count = Arc::new(Mutex::new(0));
            let count1 = count.clone();

            collection.subscribe(CollectionEventListener::new(move |_| {
                *count1.lock().unwrap() += 1;
                Ok(())
            }))?;

            // Get a new handle to the collection
            let collection = ctx.db().collection("employee")?;
            let emp = doc!{
                "emp_id": 1,
                "address": "abcd"
            };
            collection.insert(emp)?;
            
            wait_for_event(1000, || *count.lock().unwrap() == 1);
            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}