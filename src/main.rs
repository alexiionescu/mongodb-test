use mongodb::{
    bson::{self, doc}, error::{WriteError, WriteFailure}, options::{ClientOptions, IndexOptions, ServerApi, ServerApiVersion}, Client, Collection, IndexModel
};
use tracing::{error, info, warn, Level};
use anyhow::Result;
use clap::{arg, command, Subcommand, Parser};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,

    #[arg(long)]
    upsert: bool,
}

#[derive(Subcommand)]
enum CliCommand {
    Insert {
        name: String,
        birth: String,
        location: String,
        resident_since: String,
    },
    Delete {
        name: String,
        birth: String,
    },
    NewAlarm {
        name: String,
        birth: String,
        message: String,
    },
    ClearAlarm {
        name: String,
        birth: String,
        alarm_time: String,
    },
    SimpleTest,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Alarm {
    time: bson::DateTime,
    /// if None alarm is still active
    duration_sec: Option<u64>,
    message: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Resident {
    name: String,
    birth: bson::DateTime,
    location: String,
    resident_since: bson::DateTime,
    alarms: Vec<Alarm>,
}

impl Resident {
    fn new(name: &str, birth: &str, location: &str, resident_since: &str) -> Result<Self> {
        Ok(Resident {
            name: name.to_string(),
            birth: bson::DateTime::parse_rfc3339_str(birth.to_string() + "T00:00:00Z")?,
            location: location.to_string(),
            resident_since: bson::DateTime::parse_rfc3339_str(resident_since.to_string() + "T00:00:00Z")?,
            alarms: Vec::new(),
        })
    }

    fn unique_index(&self) -> bson::Document {
        doc! {
            "name": &self.name,
            "birth": &self.birth,
        }
    }

    fn update_data(&self) -> bson::Document {
        doc! {
            "$set": {
                "location": &self.location,
                "resident_since": &self.resident_since,
            }
        }
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt::init();
    dotenv::dotenv().ok();
    let mongodb_uri = dotenv::var("MONGODB_URI").expect("MONGODB_URI must be set in .env");
    let mut client_options =
    ClientOptions::parse(mongodb_uri).await?;

    // Set the server_api field of the client_options object to set the version of the Stable API on the client
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);

    // Get a handle to the cluster
    let client = Client::with_options(client_options)?;

    // Ping the server to see if you can connect to the cluster
    client
        .database("testdb")
        .run_command(doc! {"ping": 1})
        .await?;

    let collection: Collection<Resident> = client.database("testdb").collection("test_collection");
    let unique_index = IndexModel::builder()
        .keys(doc! { "name": 1, "birth": 1 })
        .options(Some(IndexOptions::builder().unique(true).build()))
        .build();
    collection.create_index(unique_index).await?;

    match &cli.command {
        CliCommand::Insert { name, birth, location, resident_since } => {
            let resident = Resident::new(name, birth, location, resident_since)?;
            if cli.upsert {
                test_upsert(&collection, resident).await?;
            } else {
                test_insert_or_update(&collection, resident).await?;
            }
        }
        CliCommand::Delete { name, birth } => {
            test_delete(&collection, name, birth).await?;
        }
        CliCommand::SimpleTest => {
            simple_test(&collection).await?;
        }
        CliCommand::NewAlarm { name, birth, message } => {
            test_new_alarm(&collection, name, birth, message).await?;
        }
        CliCommand::ClearAlarm { name, birth, alarm_time } => {
            test_clear_alarm(&collection, name, birth, alarm_time).await?;
        }
    }

    Ok(())
}

#[tracing::instrument(name = "new_alarm", skip_all, fields(name=%name, birth=%birth), level = Level::TRACE)]
async fn test_new_alarm(collection: &Collection<Resident>, name: &str, birth: &str, message: &str) -> Result<()> {
    let birth_date = bson::DateTime::parse_rfc3339_str(birth.to_string() + "T00:00:00Z")?;
    let filter = doc! {
        "name": name,
        "birth": birth_date,
    };
    let new_alarm = Alarm {
        time: bson::DateTime::now(),
        duration_sec: None,
        message: message.to_string(),
    };
    let update = doc! {
        "$push": { "alarms": bson::to_bson(&new_alarm)? }
    };
    match collection.update_one(filter, update).await {
        Ok(update_result) => {
            if update_result.matched_count > 0 {
                info!("Alarm {} added to resident. Matched: {} Updated: {}", new_alarm.time.try_to_rfc3339_string()?, update_result.matched_count, update_result.modified_count);
            } else {
                warn!("No resident found to add alarm.");
            }
        }
        Err(e) => {
            error!("Failed to add alarm: {}", e);
        }
    }
    Ok(())
}
#[tracing::instrument(name = "clear_alarm", skip_all, fields(name=%name, birth=%birth), level = Level::TRACE)]
async fn test_clear_alarm(collection: &Collection<Resident>, name: &str, birth: &str, alarm_time: &str) -> Result<()> {
    let birth_date = bson::DateTime::parse_rfc3339_str(birth.to_string() + "T00:00:00Z")?;
    let start_time = bson::DateTime::parse_rfc3339_str(alarm_time)?;
    let filter = doc! {
        "name": name,
        "birth": birth_date,
        "alarms.time" : start_time,
    };
    let duration = bson::DateTime::now().checked_duration_since(start_time).unwrap_or_default().as_secs();
    let update = doc! {
        "$set": { "alarms.$.duration_sec": bson::to_bson(&duration)? }
    };
    match collection.update_one(filter, update).await {
        Ok(update_result) => {
            if update_result.matched_count > 0 {
                info!("Alarm cleared for resident. Matched: {} Updated: {}", update_result.matched_count, update_result.modified_count);
            } else {
                warn!("No resident found to clear alarm.");
            }
        }
        Err(e) => {
            error!("Failed to clear alarm: {}", e);
        }
    }
    Ok(())
}

// Delete a resident by name and birth date
#[tracing::instrument(name = "delete", skip_all, fields(name=%name, birth=%birth), level = Level::TRACE)]
async fn test_delete(collection: &Collection<Resident>, name: &str, birth: &str) -> Result<()> {
    let birth_date = bson::DateTime::parse_rfc3339_str(birth.to_string() + "T00:00:00Z")?;
    let filter = doc! {
        "name": name,
        "birth": birth_date,
    };
    match collection.delete_one(filter).await {
        Ok(delete_result) => {
            if delete_result.deleted_count > 0 {
                info!("Resident deleted successfully.");
            } else {
                warn!("No resident found to delete.");
            }
        }
        Err(e) => {
            error!("Failed to delete resident: {}", e);
        }
    }
    Ok(())
}

#[tracing::instrument(name = "upsert", skip_all, fields(name=%resident.name, birth=%resident.birth), level = Level::TRACE)]
async fn test_upsert(collection: &Collection<Resident>, resident: Resident) -> Result<()> {
    let filter = resident.unique_index();
    let update = resident.update_data();
    let options = mongodb::options::UpdateOptions::builder().upsert(true).build();
    match collection.update_one(filter, update).with_options(options).await {
        Ok(update_result) => {
            if update_result.matched_count > 0 {
                info!("Resident updated Matched: {} Updated: {}", update_result.matched_count, update_result.modified_count);
            } else if let Some(upserted_id) = update_result.upserted_id {
                info!("New resident inserted with id: {}", upserted_id);
            }
        }
        Err(e) => {
            error!("Failed to upsert resident: {}", e);
        }
    }
    Ok(())
}

#[tracing::instrument(name = "insert_or_update", skip_all, fields(name=%resident.name, birth=%resident.birth), level = Level::TRACE)]
async fn test_insert_or_update(collection: &Collection<Resident>, resident: Resident) -> Result<()> {
    match collection.insert_one(&resident).await {
        Ok(insert_result) => {
            info!("New resident inserted with id: {}", insert_result.inserted_id);
        }
        Err(e) => {
            match e.kind.as_ref() {
                mongodb::error::ErrorKind::Write(write_failure) => {
                    match write_failure {
                        WriteFailure::WriteError(WriteError {code: 11000, ..}) => {
                            warn!("Duplicate key error: A resident with the same name and birth date already exists. Updating...");
                            let filter = resident.unique_index();
                            let update = resident.update_data();
                            match collection.update_one(filter, update).await {
                                Ok(update_result) => {
                                    info!("Resident updated Matched: {} Updated: {}", update_result.matched_count, update_result.modified_count);
                                }
                                Err(e) => {
                                    error!("Failed to update resident: {}", e);
                                }
                            }
                        }
                        _ => error!("Failed to insert new resident: {}", e),
                    }
                }
                _ => error!("Failed to insert new resident: {}", e),
            }
        }
    }
    Ok(())
}

async fn simple_test(collection: &Collection<Resident>) -> Result<()> {
    let new_resident = Resident::new("John Doe", "1990-01-01", "Room 101", "2020-01-01")?;
    test_insert_or_update(collection, new_resident).await?;
    let updated_resident = Resident::new("John Doe", "1990-01-01", "Room 102", "2021-01-01")?;
    test_insert_or_update(collection, updated_resident).await?;
    let another_resident = Resident::new("Jane Smith", "1985-05-15", "Room 105", "2019-06-01")?;
    test_upsert(collection, another_resident).await?;
    let upserted_resident = Resident::new("Jane Smith", "1985-05-15", "Room 106", "2022-07-01")?;
    test_upsert(collection, upserted_resident).await?;

    test_delete(collection, "John Doe", "1990-01-01").await?;
    test_delete(collection, "Jane Smith", "1985-05-15").await?;
    Ok(())
}