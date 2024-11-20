use std::{io::Cursor, sync::Arc};

use arrow2::io::{
    ipc::{
        read::{StreamMetadata, StreamReader, StreamState},
        IpcSchema,
    },
    json_integration::read::deserialize_schema,
};
use arrow_format::ipc::MetadataVersion;
use daft_core::series::Series;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_schema::python::schema::PySchema;
use daft_table::Table;
use eyre::{bail, ensure, WrapErr};
use itertools::Itertools;
use pyo3::{types::PyAnyMethods, Python};

pub fn local_relation(plan: spark_connect::LocalRelation) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::LocalRelation { data, schema } = plan;

    let Some(data) = data else {
        return bail!("Data is required");
    };

    let Some(schema) = schema else {
        return bail!("Schema is required");
    };

    let schema: serde_json::Value = serde_json::from_str(&schema)
        .wrap_err_with(|| format!("Failed to deserialize schema: {schema}"))?;

    let (schema, ipc_fields) = deserialize_schema(&schema)?;

    let daft_schema = daft_schema::schema::Schema::try_from(&schema)
        .wrap_err_with(|| format!("Failed to convert schema to daft schema: {schema:?}"))?;

    let daft_schema = Arc::new(daft_schema);

    let metadata = StreamMetadata {
        schema,
        version: MetadataVersion::V1,
        ipc_schema: IpcSchema {
            fields: ipc_fields,
            is_little_endian: false,
        }, // todo(corectness): unsure about endianness
    };

    let reader = Cursor::new(&data);
    let reader = StreamReader::new(reader, metadata, None); // todo(corectness): unsure about projection

    let tables = Vec::new();

    let mut chunks = reader.map(|value| match value {
        Ok(StreamState::Some(chunk)) => Ok(chunk.arrays()),
        Ok(StreamState::Waiting) => eyre::bail!("Expected some chunk"),
        Err(e) => eyre::bail!("Failed to read chunk: {e}"),
    });

    let Some(first_chunk) = chunks.next() else {
        bail!("Expected at least one chunk");
    };

    ensure!(chunks.next().is_none(), "Expected only one chunk");

    let first_chunk = first_chunk.wrap_err("Failed to read first chunk")?;

    let mut columns = Vec::with_capacity(daft_schema.fields.len());
    let mut num_rows = Vec::with_capacity(daft_schema.fields.len());

    for (array, (_, daft_field)) in itertools::zip_eq(first_chunk, &daft_schema.fields) {
        // todo(perf): is there a way to avoid cloning field and array?
        let field = daft_field.clone();
        let array = array.clone();

        let field_ref = Arc::new(field);
        let series =
            Series::from_arrow(field_ref, array).wrap_err("Failed to create series from array")?;

        columns.push(series);
        num_rows.push(series.len());
    }

    ensure!(
        num_rows.iter().all_equal(),
        "All columns must have the same number of rows"
    );

    let Some(&num_rows) = num_rows.first() else {
        bail!("Expected at least one column");
    };

    let table = Table::new_with_size(daft_schema.clone(), columns, num_rows)
        .wrap_err("Failed to create table")?;

    // todo: is daft_schema here the same as the table or no
    let micro_partition = MicroPartition::new_loaded(daft_schema, Arc::new(vec![table]), None);

    let py_mp = PyMicroPartition::from(micro_partition);

    Python::with_gil(|py| {
        //   daft. dataframe. dataframe. DataFrame
        let py_micropartition = py
            .import_bound(pyo3::intern!(py, "daft.dataframe.dataframe"))?
            .getattr(pyo3::intern!(py, "DataFrame"))?
            .getattr(pyo3::intern!(py, "_from_tables"))? // really is from micropartition
            .call1(py_mp)?; // todo: how to fix

        let plan = LogicalPlanBuilder::in_memory_scan(
            "test",
            // todo;
            py_micropartition,
            daft_schema.clone(),
            1,
            0, // todo
            num_rows,
        );

        Ok(())
    });

    // Python::with_gil(|py| {
    //     // Convert schema to PySchema
    //     let py_schema = PySchema {
    //         schema: daft_schema,
    //     };
    //
    //     // Create MicroPartition from record batches
    //     let mp = daft_micropartition::python::PyMicroPartition::from_arrow_record_batches(
    //         py,
    //         record_batches,
    //         &py_schema,
    //     )?;
    //
    //     // Convert to table and build logical plan
    //     let table = mp.to_table(py)?;
    //
    //     Ok(LogicalPlanBuilder::sc
    // });
    //
    // // let arrow_writer = daft_writers::pyarrow::PyArrowWriter::new
    // let micro_pariti

    todo!()
}
