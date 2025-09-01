use anyhow::Result;
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
        let values: Vec<String> = doc.values().map(|v| v.to_string()).collect();
        writer.write_record(&values)?;
    }

    // Flush and close the writer
    writer.flush()?;
    Ok(())
}
