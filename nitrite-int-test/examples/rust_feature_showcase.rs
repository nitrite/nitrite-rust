use nitrite::collection::order_by;
use nitrite::common::SortOrder;
use nitrite::doc;
use nitrite::filter::{all, and, field};
use nitrite::nitrite::Nitrite;
use nitrite::repository::ObjectRepository;
use nitrite_fjall_adapter::FjallModule;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_spatial::{spatial_field, spatial_index, Geometry, SpatialModule};
use nitrite_tantivy_fts::{fts_field, fts_index, TantivyFtsModule};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Convertible, NitriteEntity, Default)]
#[entity(
    name = "tasks",
    id(field = "id"),
    index(type = "unique", fields = "slug"),
    index(type = "non-unique", fields = "status"),
    index(type = "non-unique", fields = "owner")
)]
struct Task {
    pub id: i64,
    pub slug: String,
    pub title: String,
    pub owner: String,
    pub status: String,
    pub priority: i64,
    pub estimate_hours: i64,
    pub tags: Vec<String>,
}

impl Task {
    fn new(
        id: i64,
        slug: &str,
        title: &str,
        owner: &str,
        status: &str,
        priority: i64,
        estimate_hours: i64,
        tags: &[&str],
    ) -> Self {
        Self {
            id,
            slug: slug.to_string(),
            title: title.to_string(),
            owner: owner.to_string(),
            status: status.to_string(),
            priority,
            estimate_hours,
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        }
    }
}

#[derive(Debug, Clone, Convertible, NitriteEntity, Default)]
struct TaskSummary {
    pub title: Option<String>,
    pub status: Option<String>,
    pub priority: Option<i64>,
    pub owner: Option<String>,
}

fn main() -> ExampleResult<()> {
    let db_path = create_temp_db_path();
    let db = open_database(&db_path)?;

    seed_indexes(&db)?;
    seed_data(&db)?;

    let tasks: ObjectRepository<Task> = db.repository()?;
    let activity = db.collection("task_activity")?;

    let open_high_priority = tasks.find(and(vec![
        field("status").eq("open"),
        field("priority").gte(2i64),
    ]))?;
    println!("Open high-priority tasks: {}", open_high_priority.count());

    println!("Tasks sorted by priority:");
    let ordered = tasks.find_with_options(all(), &order_by("priority", SortOrder::Descending))?;
    for entry in ordered {
        let task = entry?;
        println!(
            "  - [{}] {} ({}, {}h)",
            task.priority, task.title, task.owner, task.estimate_hours
        );
    }

    println!("Alice's projected task summaries:");
    let mut owner_cursor = tasks.find(field("owner").eq("alice"))?;
    let summaries = owner_cursor.project::<TaskSummary>()?;
    for summary in summaries {
        let summary = summary?;
        println!(
            "  - {} [{}] priority={} owner={}",
            summary.title.unwrap_or_default(),
            summary.status.unwrap_or_default(),
            summary.priority.unwrap_or_default(),
            summary.owner.unwrap_or_default()
        );
    }

    let mut sync_task = tasks
        .get_by_id(&2)?
        .expect("seeded task with id=2 should exist");
    sync_task.status = "done".to_string();
    sync_task.priority = 1;
    tasks.update_one(sync_task, false)?;

    let done_tasks = tasks.find(field("status").eq("done"))?;
    println!("Completed tasks after update: {}", done_tasks.count());

    let text_matches = activity.find(fts_field("summary").matches("offline sync"))?;
    println!("FTS matches for 'offline sync': {}", text_matches.count());

    let seattle = Geometry::envelope(-122.5, 47.5, -122.2, 47.7);
    let nearby = activity.find(spatial_field("location").within(seattle))?;
    println!("Spatial matches inside Seattle bounding box: {}", nearby.count());

    drop(tasks);
    drop(activity);
    db.close()?;
    let _ = fs::remove_dir_all(&db_path);
    Ok(())
}

fn create_temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("nitrite_rust_feature_showcase_{}", Uuid::new_v4()))
}

fn open_database(db_path: &Path) -> ExampleResult<Nitrite> {
    let _ = fs::remove_dir_all(db_path);
    let db_path = db_path.to_string_lossy().into_owned();

    let fjall_module = FjallModule::with_config().db_path(&db_path).build();

    let db = Nitrite::builder()
        .load_module(fjall_module)
        .load_module(TantivyFtsModule::default())
        .load_module(SpatialModule)
        .open_or_create(None, None)?;

    Ok(db)
}

fn seed_indexes(db: &Nitrite) -> ExampleResult<()> {
    let activity = db.collection("task_activity")?;
    activity.create_index(vec!["summary"], &fts_index())?;
    activity.create_index(vec!["location"], &spatial_index())?;
    Ok(())
}

fn seed_data(db: &Nitrite) -> ExampleResult<()> {
    db.with_session(|session| {
        let tx = session.begin_transaction()?;
        let tx_tasks: ObjectRepository<Task> = tx.repository()?;

        tx_tasks.insert(Task::new(
            1,
            "release-dashboard",
            "Prepare the release dashboard",
            "alice",
            "open",
            3,
            8,
            &["release", "dashboard"],
        ))?;
        tx_tasks.insert(Task::new(
            2,
            "offline-sync",
            "Ship offline sync improvements",
            "alice",
            "open",
            2,
            13,
            &["sync", "storage"],
        ))?;
        tx_tasks.insert(Task::new(
            3,
            "spatial-alerts",
            "Add region-aware activity alerts",
            "bob",
            "review",
            1,
            5,
            &["search", "spatial"],
        ))?;

        tx.commit()
    })?;

    let activity = db.collection("task_activity")?;
    activity.insert(doc! {
        "task_slug": "release-dashboard",
        "summary": "Release dashboard shipped with offline sync metrics",
        "location": {
            "x": (-122.335167f64),
            "y": 47.608013f64
        }
    })?;

    activity.insert(doc! {
        "task_slug": "offline-sync",
        "summary": "Offline sync now works for project snapshots and task queues",
        "location": {
            "x": (-122.332071f64),
            "y": 47.606209f64
        }
    })?;

    activity.insert(doc! {
        "task_slug": "spatial-alerts",
        "summary": "Spatial alerts were validated for downtown Seattle work items",
        "location": {
            "x": (-122.342056f64),
            "y": 47.609722f64
        }
    })?;

    Ok(())
}