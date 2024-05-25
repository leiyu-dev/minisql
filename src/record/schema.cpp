#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    char *start = buf;
    MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
    buf += sizeof(uint32_t);

    uint32_t column_count = columns_.size();
    MACH_WRITE_UINT32(buf, column_count);
    buf += sizeof(uint32_t);

    for (const auto &column : columns_) {
        buf += column->SerializeTo(buf);
    }

    return buf - start;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    if (schema != nullptr) {
        LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
    }

    char *start = buf;
    uint32_t magic_num = MACH_READ_UINT32(buf);
    ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Magic number mismatch in Schema::DeserializeFrom");
    buf += sizeof(uint32_t);

    uint32_t column_count = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    std::vector<Column *> columns;
    for (uint32_t i = 0; i < column_count; ++i) {
        Column *column = nullptr;
        buf += Column::DeserializeFrom(buf, column);
        columns.push_back(column);
    }

    schema = new Schema(columns);
    return buf - start;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t size = sizeof(uint32_t) * 2;  // magic_num + column_count
    for (const auto &column : columns_) {
        size += column->GetSerializedSize();
    }
    return size;
}