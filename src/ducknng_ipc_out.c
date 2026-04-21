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

int ducknng_result_to_ipc_stream(duckdb_prepared_statement stmt, duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    (void)stmt;
    (void)result;
    (void)out_bytes;
    (void)out_len;
    if (errmsg) *errmsg = ducknng_strdup(
        "ducknng: Arrow row-result encoding is not yet implemented on the stable extension API path; use want_result = false for now");
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
