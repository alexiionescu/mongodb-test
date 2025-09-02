use anyhow::Result;
use chrono::Local;
use futures::TryStreamExt as _;
use mongodb::bson;

pub enum DateTimeStr<'a> {
    Str(&'a str),
    String(String),
    DateTime(bson::DateTime),
}

impl From<DateTimeStr<'_>> for bson::DateTime {
    fn from(val: DateTimeStr<'_>) -> Self {
        match val {
            DateTimeStr::Str(s) => {
                if !s.contains('T') {
                    // assume date only
                    bson::DateTime::parse_rfc3339_str(s.to_string() + "T00:00:00Z")
                        .unwrap_or_else(|_| bson::DateTime::now())
                } else if !s.ends_with('Z') && !s.contains('+') && !s.contains('-') {
                    // assume UTC if no timezone provided
                    bson::DateTime::parse_rfc3339_str(s.to_string() + "Z")
                        .unwrap_or_else(|_| bson::DateTime::now())
                } else {
                    bson::DateTime::parse_rfc3339_str(s).unwrap_or_else(|_| bson::DateTime::now())
                }
            }
            DateTimeStr::String(s) => {
                if !s.contains('T') {
                    // assume date only
                    bson::DateTime::parse_rfc3339_str(s + "T00:00:00Z")
                        .unwrap_or_else(|_| bson::DateTime::now())
                } else if !s.ends_with('Z') && !s.contains('+') && !s.contains('-') {
                    // assume UTC if no timezone provided
                    bson::DateTime::parse_rfc3339_str(s + "Z")
                        .unwrap_or_else(|_| bson::DateTime::now())
                } else {
                    bson::DateTime::parse_rfc3339_str(&s).unwrap_or_else(|_| bson::DateTime::now())
                }
            }
            DateTimeStr::DateTime(dt) => dt,
        }
    }
}

impl std::fmt::Display for DateTimeStr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimeStr::Str(s) => write!(f, "{}", s),
            DateTimeStr::String(s) => write!(f, "{}", s),
            DateTimeStr::DateTime(dt) => write!(f, "{}", dt),
        }
    }
}

pub mod serde_helpers {
    pub mod bson_datetime_as_rfc3339_string_date {
        use crate::utils::DateTimeStr;
        use mongodb::bson;
        use serde::{Deserialize, Deserializer, Serializer, ser};
        use std::result::Result;

        /// Deserializes a [`bson::DateTime`] from an RFC 3339 formatted string.
        pub fn deserialize<'de, D>(deserializer: D) -> Result<bson::DateTime, D::Error>
        where
            D: Deserializer<'de>,
        {
            Ok(DateTimeStr::String(String::deserialize(deserializer)?).into())
        }

        #[allow(unused)]
        /// Serializes a [`bson::DateTime`] as an RFC 3339 (ISO 8601) formatted string.
        pub fn serialize<S: Serializer>(
            val: &bson::DateTime,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let formatted = val.try_to_rfc3339_string().map_err(|e| {
                ser::Error::custom(format!("cannot format {} as RFC 3339: {}", val, e))
            })?;
            serializer.serialize_str(&formatted)
        }
    }
}

/// print query cursor to tty nice table view
pub async fn bson_table_print(cursor: mongodb::Cursor<bson::Document>) -> Result<()> {
    let mut table = comfy_table::Table::new();
    let mut first = true;
    let mut cursor = cursor;
    while let Some(doc) = cursor.try_next().await? {
        if first {
            // Write the header row (keys of the BSON document)
            let headers: Vec<&str> = doc.keys().map(|k| k.as_str()).collect();
            table.set_header(headers);
            first = false;
        }
        // Write the values row (values of the BSON document)
        let values: Vec<String> = doc
            .values()
            .enumerate()
            .map(|(i, v)| bson_value_to_str(v, doc.keys().nth(i).unwrap()))
            .collect();
        table.add_row(values);
    }
    println!("{table}");
    Ok(())
}

/// Converts a BSON Documents Cursor to a CSV file
pub async fn bson_to_csv(
    mut cursor: mongodb::Cursor<bson::Document>,
    file_path: &str,
) -> Result<()> {
    // Create a CSV writer
    let mut writer = csv::WriterBuilder::new().from_path(file_path)?;

    let mut first = true;
    while let Some(doc) = cursor.try_next().await? {
        if first {
            // Write the header row (keys of the BSON document)
            let headers: Vec<&str> = doc.keys().map(|k| k.as_str()).collect();
            writer.write_record(&headers)?;
            first = false;
        }
        // Write the values row (values of the BSON document)
        let values: Vec<String> = doc
            .values()
            .enumerate()
            .map(|(i, v)| bson_value_to_str(v, doc.keys().nth(i).unwrap()))
            .collect();
        writer.write_record(&values)?;
    }

    // Flush and close the writer
    writer.flush()?;
    Ok(())
}

pub fn format_timedelta(duration: &f64) -> String {
    let total_minutes = (duration / 60.0).round() as i64;
    let days = total_minutes / 1440;
    let hours = (total_minutes % 1440) / 60;
    let minutes = total_minutes % 60;
    if days > 0 {
        format!("{days:02} days {hours:02}h {minutes:02}m")
    } else if hours > 0 {
        format!("{hours:02}h {minutes:02}m")
    } else {
        format!("{:4}{minutes:02}m", "")
    }
}

fn bson_value_to_str(value: &bson::Bson, key: &str) -> String {
    match value {
        bson::Bson::String(s) => s.clone(),
        bson::Bson::Boolean(b) => {
            if *b {
                "yes".into()
            } else {
                "no".into()
            }
        }
        bson::Bson::ObjectId(oid) => oid.to_string(),
        bson::Bson::DateTime(dt) => dt
            .to_chrono()
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M")
            .to_string(),
        bson::Bson::Double(d) => {
            println!("Formatting BSON Double: {} for {}", d, key);
            if key.contains("duration") {
                format_timedelta(d)
            } else {
                format!("{:.2}", d)
            }
        }
        bson::Bson::Int64(i) => {
            if key.contains("duration") {
                format_timedelta(&(*i as f64))
            } else {
                i.to_string()
            }
        }
        _ => value.to_string(),
    }
}
