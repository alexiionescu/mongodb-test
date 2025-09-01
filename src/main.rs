use std::fmt;

use anyhow::Result;
use clap::{Parser, Subcommand, arg, command};
use csv::ReaderBuilder;
use futures::TryStreamExt as _;
use mongodb::{
    Client, Collection, IndexModel,
    bson::{self, doc},
    error::{WriteError, WriteFailure},
    options::{ClientOptions, IndexOptions, ServerApi, ServerApiVersion},
};
use tracing::{Level, error, info, warn};
use utils::{DateTimeStr, serde_helpers};

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
    NewAlarmCsv {
        file_path: String,
        count: usize,
        #[clap(long)]
        no_clear: bool,
    },
    InsertCsv {
        file_path: String,
    },
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
    Query {
        from_date: String,
        to_date: String,
        #[clap(short, long, help = "Optional Regexp pattern to match resident names")]
        name: Option<String>,
        #[clap(
            short,
            long,
            help = "Optional Regexp pattern to match resident locations"
        )]
        location: Option<String>,
        #[clap(long, help = "CSV File to Save Results")]
        csv: Option<String>,
    },
    SimpleTest,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Alarm {
    time: bson::DateTime,
    duration_sec: u64,
    message: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ActiveAlarm {
    time: bson::DateTime,
    message: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Resident {
    name: String,
    #[serde(deserialize_with = "serde_helpers::bson_datetime_as_rfc3339_string_date::deserialize")]
    birth: bson::DateTime,
    location: String,
    #[serde(deserialize_with = "serde_helpers::bson_datetime_as_rfc3339_string_date::deserialize")]
    resident_since: bson::DateTime,
    #[serde(default)]
    alarms: Vec<Alarm>,
    #[serde(default)]
    active_alarms: Vec<ActiveAlarm>,
}

impl Resident {
    fn new(name: &str, birth: &str, location: &str, resident_since: &str) -> Result<Self> {
        Ok(Resident {
            name: name.to_string(),
            birth: DateTimeStr::Str(birth).into(),
            location: location.to_string(),
            resident_since: DateTimeStr::Str(resident_since).into(),
            alarms: Vec::new(),
            active_alarms: Vec::new(),
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

impl fmt::Display for Resident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Resident {{ name: {}, birth: {}, location: {}, resident_since: {} }}",
            self.name, self.birth, self.location, self.resident_since,
        )?;
        for active_alarm in &self.active_alarms {
            write!(
                f,
                "\n  ActiveAlarm {{ time: {}, message: {} }}",
                active_alarm.time, active_alarm.message,
            )?;
        }
        for alarm in &self.alarms {
            write!(
                f,
                "\n  HistoryAlarm {{ time: {}, duration_sec: {}, message: {} }}",
                alarm.time, alarm.duration_sec, alarm.message,
            )?;
        }
        Ok(())
    }
}

mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let mut cli = Cli::parse();
    tracing_subscriber::fmt::init();
    dotenv::dotenv().ok();
    let mongodb_uri = dotenv::var("MONGODB_URI").expect("MONGODB_URI must be set in .env");
    let mut client_options = ClientOptions::parse(mongodb_uri).await?;

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

    match &mut cli.command {
        CliCommand::Insert {
            name,
            birth,
            location,
            resident_since,
        } => {
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
        CliCommand::NewAlarm {
            name,
            birth,
            message,
        } => {
            test_new_alarm(&collection, name, birth, message).await?;
        }
        CliCommand::ClearAlarm {
            name,
            birth,
            alarm_time,
        } => {
            test_clear_alarm(&collection, name, birth, DateTimeStr::Str(alarm_time), None).await?;
        }
        CliCommand::Query {
            from_date,
            to_date,
            name,
            location,
            csv,
        } => {
            test_query(
                &collection,
                from_date,
                to_date,
                name.as_deref(),
                location.as_deref(),
                csv.as_deref(),
            )
            .await?;
        }
        CliCommand::InsertCsv { file_path } => {
            test_insert_csv(&collection, file_path, cli.upsert).await?;
        }
        CliCommand::NewAlarmCsv {
            file_path,
            count,
            no_clear,
        } => {
            let mut reader = ReaderBuilder::new()
                .has_headers(true)
                .from_path(file_path)?;
            let mut alarms = Vec::new();
            while *count > 0
                && let Some(Ok(record)) = reader.deserialize::<Resident>().next()
            {
                if rand::random::<f32>() > (0.02 + *count as f32 * 0.02) {
                    continue;
                }
                let name = record.name.clone();
                let birth = record.birth.try_to_rfc3339_string()?[..10].to_string();
                alarms.push((
                    name,
                    birth.clone(),
                    test_new_alarm(&collection, &record.name, &birth, "test csv alarm").await?,
                ));
                *count -= 1;
            }
            if !*no_clear {
                for (name, birth, alarm_time) in alarms {
                    test_clear_alarm(
                        &collection,
                        &name,
                        &birth,
                        DateTimeStr::DateTime(alarm_time),
                        Some(rand::random::<u64>() % 600),
                    )
                    .await?;
                }
            }
        }
    }

    Ok(())
}

async fn test_insert_csv(
    collection: &Collection<Resident>,
    file_path: &str,
    upsert: bool,
) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    for result in reader.deserialize() {
        let record: Resident = result?;
        println!("Importing {}", record);
        if upsert {
            test_upsert(collection, record).await?;
        } else {
            test_insert_or_update(collection, record).await?;
        }
    }
    Ok(())
}

#[tracing::instrument(name = "query", skip(collection), level = Level::TRACE)]
async fn test_query(
    collection: &Collection<Resident>,
    from_date: &str,
    to_date: &str,
    name: Option<&str>,
    location: Option<&str>,
    csv: Option<&str>,
) -> Result<()> {
    let mut filter = if let Some(name_pattern) = name
        && let Some(location_pattern) = location
    {
        doc! {
            "$or": [
                { "name": { "$regex": name_pattern, "$options": "i" } },
                { "location": { "$regex": location_pattern, "$options": "i" } }
            ]
        }
    } else if let Some(name_pattern) = name {
        doc! { "name": { "$regex": name_pattern, "$options": "i" } }
    } else if let Some(location_pattern) = location {
        doc! { "location": { "$regex": location_pattern, "$options": "i" } }
    } else {
        doc! {}
    };
    filter.extend(doc! {
        "$or": [
            { "active_alarms.0": { "$exists": true } },
            { "$expr": { "$gt": [ { "$size": "$filteredAlarms" }, 0 ] } }
        ]
    });
    let pipeline = vec![
        doc! { "$addFields": {
            "filteredAlarms": {
                "$filter": {
                    "input": "$alarms",
                    "as": "alarm",
                    "cond": {
                        "$and": [
                            { "$gte": [ "$$alarm.time", bson::DateTime::parse_rfc3339_str(from_date.to_string() + "T00:00:00Z")? ] },
                            { "$lte": [ "$$alarm.time", bson::DateTime::parse_rfc3339_str(to_date.to_string() + "T23:59:59.999Z")? ] }
                        ]
                    }
                }
            }
        } },
        doc! { "$match": filter },
        doc! { "$project": {
            "name": 1, "location": 1,
            "alarms_count": { "$size": { "$ifNull": ["$filteredAlarms", []] } },
            "alarms_avg_duration": { "$avg": "$filteredAlarms.duration_sec" },
            "alarms_min_time": { "$min": "$filteredAlarms.time" },
            "alarms_max_time": { "$max": "$filteredAlarms.time" },
            "active_alarms_count": { "$size": { "$ifNull": ["$active_alarms", []] } }
        } },
        doc! { "$sort": { "location": 1 } },
    ];
    match collection.aggregate(pipeline).await {
        Ok(mut cursor) => {
            if let Some(csv) = csv {
                utils::bson_to_csv(cursor, csv).await?;
            } else {
                while let Some(resident) = cursor.try_next().await? {
                    println!("{}", resident);
                }
            }
        }
        Err(e) => {
            error!("Failed to query residents: {}", e);
        }
    }
    Ok(())
}

#[tracing::instrument(name = "new_alarm", skip_all, fields(name=%name, birth=%birth), level = Level::TRACE)]
async fn test_new_alarm(
    collection: &Collection<Resident>,
    name: &str,
    birth: &str,
    message: &str,
) -> Result<bson::DateTime> {
    let birth_date: bson::DateTime = DateTimeStr::Str(birth).into();
    let filter = doc! {
        "name": name,
        "birth": birth_date,
    };
    let new_alarm = ActiveAlarm {
        time: bson::DateTime::now(),
        message: message.to_string(),
    };
    let update = doc! {
        "$push": { "active_alarms": bson::to_bson(&new_alarm)? }
    };
    match collection.update_one(filter, update).await {
        Ok(update_result) => {
            if update_result.matched_count > 0 {
                info!(
                    "Alarm {} added to resident. Matched: {} Updated: {}",
                    new_alarm.time.try_to_rfc3339_string()?,
                    update_result.matched_count,
                    update_result.modified_count
                );
                println!(
                    "To clear: mongodb-test clear-alarm '{}' '{}' '{}'",
                    name,
                    birth,
                    new_alarm.time.try_to_rfc3339_string()?
                );
                Ok(new_alarm.time)
            } else {
                anyhow::bail!("No resident found to add alarm.");
            }
        }
        Err(e) => {
            anyhow::bail!(format!("Failed to add alarm: {}", e));
        }
    }
}
#[tracing::instrument(name = "clear_alarm", skip_all, fields(name=%name, birth=%birth, alarm=%alarm_time), level = Level::TRACE)]
async fn test_clear_alarm(
    collection: &Collection<Resident>,
    name: &str,
    birth: &str,
    alarm_time: DateTimeStr<'_>,
    duration: Option<u64>,
) -> Result<()> {
    let birth_date: bson::DateTime = DateTimeStr::Str(birth).into();
    let start_time: bson::DateTime = alarm_time.into();
    let filter = doc! {
        "name": name,
        "birth": birth_date,
    };
    let mut resident_id_and_alarm = collection.aggregate(vec![
        doc! { "$match": filter },
        doc! { "$project": { "id": "$_id", "alarm": { "$filter": { "input": "$active_alarms", "as": "alarm", "cond": { "$eq": [ "$$alarm.time", start_time ] } } } } } 
    ]).await?;
    if let Some(resident) = resident_id_and_alarm.try_next().await? {
        let resident_id = resident.get("_id");

        let alarm_array = resident.get_array("alarm")?;
        if alarm_array.is_empty() {
            warn!("No active alarm found with the specified time to clear.");
            return Ok(());
        }
        let alarm_doc = alarm_array[0].as_document().unwrap();
        let message = alarm_doc.get_str("message").unwrap_or("");
        let alarm_time = alarm_doc.get_datetime("time").unwrap();
        let duration = duration.unwrap_or(
            bson::DateTime::now()
                .checked_duration_since(*alarm_time)
                .unwrap_or_default()
                .as_secs(),
        );
        info!(
            "Clearing alarm for resident id: {:?}, message: {}, start_time: {}, duration_sec: {}",
            resident_id,
            message,
            alarm_time.try_to_rfc3339_string()?,
            duration
        );

        // remove alarm from active
        let filter = doc! {
            "_id": resident_id
        };
        let update = doc! {
            "$pull": {
                "active_alarms": {
                    "time": alarm_time
                }
            }
        };
        match collection.update_one(filter.clone(), update).await {
            Ok(update_result) => {
                if update_result.matched_count > 0 {
                    info!(
                        "Alarm cleared from active for resident. Matched: {} Updated: {}",
                        update_result.matched_count, update_result.modified_count
                    );
                } else {
                    warn!("No resident found to clear alarm.");
                }
            }
            Err(e) => {
                error!("Failed to clear alarm: {}", e);
            }
        };

        // add alarm to history
        let history_update = doc! {
            "$push": {
                "alarms": {
                    "time": alarm_time,
                    "message": message,
                    "duration_sec": bson::to_bson(&duration)?
                }
            }
        };
        match collection.update_one(filter, history_update).await {
            Ok(update_result) => {
                if update_result.matched_count > 0 {
                    info!(
                        "Alarm added to history for resident. Matched: {} Updated: {}",
                        update_result.matched_count, update_result.modified_count
                    );
                } else {
                    warn!("No resident found to add alarm to history.");
                }
            }
            Err(e) => {
                error!("Failed to add alarm to history: {}", e);
            }
        };
    } else {
        warn!("No resident found to clear alarm.");
        return Ok(());
    }

    // let duration = bson::DateTime::now().checked_duration_since(start_time).unwrap_or_default().as_secs();
    // let update = doc! {
    //     "$set": {
    //         "active_alarms.duration_sec": bson::to_bson(&duration)?
    //     }
    // };
    // let mut set_filter = filter.clone();
    // set_filter.extend(doc! { "active_alarms.time": start_time });
    // match collection.update_one(set_filter, update).await {
    //     Ok(update_result) => {
    //         if update_result.matched_count > 0 {
    //             info!("Alarm cleared updated for resident. Matched: {} Updated: {}", update_result.matched_count, update_result.modified_count);
    //         } else {
    //             warn!("No resident found to clear alarm.");
    //         }
    //     }
    //     Err(e) => {
    //         error!("Failed to clear alarm: {}", e);
    //     }
    // }
    // let update = doc! {
    //     "$set": {
    //         "active_alarms" : {
    //             "$filter": {
    //                 "input": "$active_alarms",
    //                 "as": "alarm",
    //                 "cond": { "$ne": [ "$$alarm.time", start_time ] }
    //             }
    //         },
    //         "alarms" : {
    //             "$concatArrays": [
    //                 "$alarms",
    //                 {
    //                     "$filter": {
    //                         "input": "$active_alarms",
    //                         "as": "alarm",
    //                         "cond": { "$eq": [ "$$alarm.time", start_time ] }
    //                     }
    //                 }
    //             ]
    //         }
    //     }
    // };
    // match collection.update_one(filter, update).await {
    //     Ok(update_result) => {
    //         if update_result.matched_count > 0 {
    //             info!("Alarm cleared moved to history for resident. Matched: {} Updated: {}", update_result.matched_count, update_result.modified_count);
    //         } else {
    //             warn!("No resident found to clear alarm.");
    //         }
    //     }
    //     Err(e) => {
    //         error!("Failed to clear alarm: {}", e);
    //     }
    // }
    Ok(())
}

// Delete a resident by name and birth date
#[tracing::instrument(name = "delete", skip_all, fields(name=%name, birth=%birth), level = Level::TRACE)]
async fn test_delete(collection: &Collection<Resident>, name: &str, birth: &str) -> Result<()> {
    let birth_date: bson::DateTime = DateTimeStr::Str(birth).into();
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
    let options = mongodb::options::UpdateOptions::builder()
        .upsert(true)
        .build();
    match collection
        .update_one(filter, update)
        .with_options(options)
        .await
    {
        Ok(update_result) => {
            if update_result.matched_count > 0 {
                info!(
                    "Resident updated Matched: {} Updated: {}",
                    update_result.matched_count, update_result.modified_count
                );
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
async fn test_insert_or_update(
    collection: &Collection<Resident>,
    resident: Resident,
) -> Result<()> {
    match collection.insert_one(&resident).await {
        Ok(insert_result) => {
            info!(
                "New resident inserted with id: {}",
                insert_result.inserted_id
            );
        }
        Err(e) => match e.kind.as_ref() {
            mongodb::error::ErrorKind::Write(write_failure) => match write_failure {
                WriteFailure::WriteError(WriteError { code: 11000, .. }) => {
                    warn!(
                        "Duplicate key error: A resident with the same name and birth date already exists. Updating..."
                    );
                    let filter = resident.unique_index();
                    let update = resident.update_data();
                    match collection.update_one(filter, update).await {
                        Ok(update_result) => {
                            info!(
                                "Resident updated Matched: {} Updated: {}",
                                update_result.matched_count, update_result.modified_count
                            );
                        }
                        Err(e) => {
                            error!("Failed to update resident: {}", e);
                        }
                    }
                }
                _ => error!("Failed to insert new resident: {}", e),
            },
            _ => error!("Failed to insert new resident: {}", e),
        },
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
