#include "record/row.h"

/**
 * TODO: Student Implement
 */

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

    char *start = buf;
    MACH_WRITE_UINT32(buf, Row::ROW_MAGIC_NUM);
    buf += sizeof(uint32_t);

    uint32_t field_count = fields_.size();
    MACH_WRITE_UINT32(buf, field_count);
    buf += sizeof(uint32_t);

    uint32_t null_bitmap = 0;
    for (uint32_t i = 0; i < field_count; ++i) {
        if (fields_[i]->IsNull()) {
            null_bitmap |= (1 << i);
        }
    }
    MACH_WRITE_UINT32(buf, null_bitmap);
    buf += sizeof(uint32_t);

    for (uint32_t i = 0; i < field_count; i++) {
        buf += fields_[i]->SerializeTo(buf);
    }

    return buf - start;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before deserialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");

    char *start = buf;
    uint32_t magic_num = MACH_READ_UINT32(buf);
    ASSERT(magic_num == Row::ROW_MAGIC_NUM, "Magic number mismatch in Row::DeserializeFrom");
    buf += sizeof(uint32_t);

    uint32_t field_count = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    uint32_t null_bitmap = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    fields_.resize(field_count);
    for (uint32_t i = 0; i < field_count; ++i) {
        fields_[i] = nullptr;
        buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &fields_[i], (null_bitmap & (1 << i)) != 0);
    }

    return buf - start;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before calculate size.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

    uint32_t size = sizeof(uint32_t) * 3;  // magic_num + field_count + null_bitmap
    for (const auto &field : fields_) {
        size += field->GetSerializedSize();
    }
    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column : columns) {
        schema->GetColumnIndex(column->GetName(), idx);
        fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}
