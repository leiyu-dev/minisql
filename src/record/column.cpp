#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
    ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
    switch (type) {
        case TypeId::kTypeInt:
            len_ = sizeof(int32_t);
            break;
        case TypeId::kTypeFloat:
            len_ = sizeof(float_t);
            break;
        default:
            ASSERT(false, "Unsupported column type.");
    }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)),
          type_(type),
          len_(length),
          table_ind_(index),
          nullable_(nullable),
          unique_(unique) {
    ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
        : name_(other->name_),
          type_(other->type_),
          len_(other->len_),
          table_ind_(other->table_ind_),
          nullable_(other->nullable_),
          unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
    char *start = buf;
    MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
    buf += sizeof(uint32_t);

    uint32_t name_len = name_.length();
    MACH_WRITE_UINT32(buf, name_len);
    buf += sizeof(uint32_t);
    MACH_WRITE_STRING(buf, name_);
    buf += name_len;

    MACH_WRITE_UINT32(buf, static_cast<uint32_t>(type_));
    buf += sizeof(uint32_t);

    MACH_WRITE_UINT32(buf, len_);
    buf += sizeof(uint32_t);

    MACH_WRITE_UINT32(buf, table_ind_);
    buf += sizeof(uint32_t);

    MACH_WRITE_UINT32(buf, nullable_);
    buf += sizeof(uint32_t);

    MACH_WRITE_UINT32(buf, unique_);
    buf += sizeof(uint32_t);

    return buf - start;
}


/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
    return sizeof(uint32_t) * 7 + name_.length();
}


/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    if (column != nullptr) {
        LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
    }

    char *start = buf;
    uint32_t magic_num = MACH_READ_UINT32(buf);
    ASSERT(magic_num == COLUMN_MAGIC_NUM, "Magic number mismatch in Column::DeserializeFrom");
    buf += sizeof(uint32_t);

    uint32_t name_len = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    std::string column_name(buf, name_len);
    buf += name_len;

    TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf));
    buf += sizeof(uint32_t);

    uint32_t col_len = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    uint32_t col_ind = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    bool nullable = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    bool unique = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);

    /* allocate object */
    if (type == TypeId::kTypeChar) {
        column = new Column(column_name, type, col_len, col_ind, nullable, unique);
    } else {
        column = new Column(column_name, type, col_ind, nullable, unique);
    }

    return buf - start;
}
