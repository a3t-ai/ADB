//! Lance 12 writes and reads a table built from the workspace's Arrow types (K5).

use std::sync::Arc;

use adb_store::lance::dataset::{Dataset, WriteParams};
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

#[tokio::test]
async fn write_and_reopen_a_lance_table() {
    let dir = std::env::temp_dir().join(format!("adb-store-{}", uuid::Uuid::new_v4()));
    let uri = dir.to_str().unwrap().to_string();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("log_offset", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    let reader = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(reader, &uri, Some(WriteParams::default())).await.unwrap();

    let ds = Dataset::open(&uri).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 3);
    assert_eq!(ds.schema().fields.len(), 2);

    std::fs::remove_dir_all(dir).unwrap();
}
