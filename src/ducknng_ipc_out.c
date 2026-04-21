#include "ducknng_ipc_out.h"
#include "ducknng_util.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_build_metadata_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 3) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "rows_changed") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "statement_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[2], "result_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[2], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_set_arrow_schema_type(duckdb_logical_type logical_type, struct ArrowSchema *schema,
    char **errmsg) {
    duckdb_type type_id;
    if (!logical_type || !schema) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing logical type for Arrow schema mapping");
        return -1;
    }
    type_id = duckdb_get_type_id(logical_type);
    switch (type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TINYINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT8) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_SMALLINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_INTEGER:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BIGINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UTINYINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT8) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_USMALLINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT16) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UINTEGER:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT32) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UBIGINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT64) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_FLOAT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DOUBLE:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_VARCHAR:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BLOB:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY) == NANOARROW_OK ? 0 : -1;
        default:
            if (errmsg) {
                *errmsg = ducknng_strdup(
                    "ducknng: unary exec row replies currently support BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB only");
            }
            return -1;
    }
}

static int ducknng_build_result_schema(duckdb_result result, struct ArrowSchema *schema, char **errmsg) {
    idx_t ncols;
    idx_t col;
    ArrowSchemaInit(schema);
    ncols = duckdb_column_count(&result);
    if (ArrowSchemaSetTypeStruct(schema, (int64_t)ncols) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result schema");
        return -1;
    }
    for (col = 0; col < ncols; col++) {
        duckdb_logical_type logical_type = duckdb_column_logical_type(&result, col);
        const char *name = duckdb_column_name(&result, col);
        if (ArrowSchemaSetName(schema->children[col], name ? name : "") != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to set Arrow result column name");
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            return -1;
        }
        if (ducknng_set_arrow_schema_type(logical_type, schema->children[col], errmsg) != 0) {
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            return -1;
        }
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
    }
    return 0;
}

static int ducknng_is_valid(uint64_t *validity, idx_t row) {
    return validity ? duckdb_validity_row_is_valid(validity, row) : 1;
}

static int ducknng_append_vector_value(duckdb_type type_id, duckdb_vector vector,
    struct ArrowArray *array, idx_t row, char **errmsg) {
    uint64_t *validity = duckdb_vector_get_validity(vector);
    void *data = duckdb_vector_get_data(vector);
    if (!ducknng_is_valid(validity, row)) {
        return ArrowArrayAppendNull(array, 1) == NANOARROW_OK ? 0 : -1;
    }
    switch (type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            return ArrowArrayAppendInt(array, ((bool *)data)[row] ? 1 : 0) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TINYINT:
            return ArrowArrayAppendInt(array, ((int8_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_SMALLINT:
            return ArrowArrayAppendInt(array, ((int16_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_INTEGER:
            return ArrowArrayAppendInt(array, ((int32_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BIGINT:
            return ArrowArrayAppendInt(array, ((int64_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UTINYINT:
            return ArrowArrayAppendUInt(array, ((uint8_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_USMALLINT:
            return ArrowArrayAppendUInt(array, ((uint16_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UINTEGER:
            return ArrowArrayAppendUInt(array, ((uint32_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UBIGINT:
            return ArrowArrayAppendUInt(array, ((uint64_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_FLOAT:
            return ArrowArrayAppendDouble(array, ((float *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DOUBLE:
            return ArrowArrayAppendDouble(array, ((double *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_VARCHAR: {
            duckdb_string_t *strings = (duckdb_string_t *)data;
            struct ArrowStringView value;
            value.data = duckdb_string_t_data(&strings[row]);
            value.size_bytes = (int64_t)duckdb_string_t_length(strings[row]);
            return ArrowArrayAppendString(array, value) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_BLOB: {
            duckdb_string_t *strings = (duckdb_string_t *)data;
            struct ArrowBufferView value;
            value.data.data = (const uint8_t *)duckdb_string_t_data(&strings[row]);
            value.size_bytes = (int64_t)duckdb_string_t_length(strings[row]);
            return ArrowArrayAppendBytes(array, value) == NANOARROW_OK ? 0 : -1;
        }
        default:
            if (errmsg) {
                *errmsg = ducknng_strdup(
                    "ducknng: encountered unsupported DuckDB type while encoding unary row reply");
            }
            return -1;
    }
}

static int ducknng_append_chunk_to_arrow(duckdb_data_chunk chunk, struct ArrowArray *root,
    char **errmsg) {
    idx_t ncols;
    idx_t nrows;
    idx_t col;
    idx_t row;
    ncols = duckdb_data_chunk_get_column_count(chunk);
    nrows = duckdb_data_chunk_get_size(chunk);
    for (col = 0; col < ncols; col++) {
        duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, col);
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
        duckdb_type type_id = logical_type ? duckdb_get_type_id(logical_type) : DUCKDB_TYPE_INVALID;
        for (row = 0; row < nrows; row++) {
            if (ducknng_append_vector_value(type_id, vector, root->children[col], row, errmsg) != 0) {
                if (logical_type) duckdb_destroy_logical_type(&logical_type);
                if ((!errmsg || !*errmsg) && errmsg) {
                    *errmsg = ducknng_strdup("ducknng: failed to append DuckDB result value to Arrow array");
                }
                return -1;
            }
        }
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
    }
    return 0;
}

static int ducknng_arrow_stream_to_bytes(struct ArrowSchema *schema, struct ArrowArray *array,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int rc = -1;

    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (ArrowBasicArrayStreamInit(&stream, schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result stream");
        return -1;
    }
    memset(schema, 0, sizeof(*schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, array);
    memset(array, 0, sizeof(*array));

    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying Arrow result payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array->release) ArrowArrayRelease(array);
    if (schema->release) ArrowSchemaRelease(schema);
    return rc;
}

int ducknng_result_to_ipc_stream(duckdb_prepared_statement stmt, duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowError error;
    duckdb_data_chunk chunk = NULL;
    (void)stmt;

    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&error, 0, sizeof(error));
    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid result output pointers");
        return -1;
    }
    if (ducknng_build_result_schema(result, &schema, errmsg) != 0) goto cleanup;
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start Arrow result append");
        goto cleanup;
    }
    for (;;) {
        chunk = duckdb_fetch_chunk(result);
        if (!chunk) break;
        if (ducknng_append_chunk_to_arrow(chunk, &array, errmsg) != 0) {
            duckdb_destroy_data_chunk(&chunk);
            goto cleanup;
        }
        duckdb_destroy_data_chunk(&chunk);
    }
    if (ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to finalize Arrow result array");
        goto cleanup;
    }
    return ducknng_arrow_stream_to_bytes(&schema, &array, out_bytes, out_len, errmsg);

cleanup:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return -1;
}

int ducknng_exec_metadata_to_ipc(uint64_t rows_changed,
    uint32_t statement_type, uint32_t result_type, uint8_t **out_bytes,
    size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int rc = -1;

    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid metadata output pointers");
        return -1;
    }
    if (ducknng_build_metadata_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build metadata Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start metadata Arrow array append");
        goto cleanup;
    }
    if (ArrowArrayAppendUInt(array.children[0], rows_changed) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[1], (int64_t)statement_type) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[2], (int64_t)result_type) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build metadata Arrow array");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying metadata Arrow IPC payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
