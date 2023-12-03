def get_format_str(schema, max_length=10):
    format_str = ""
    for field in schema:
        format_str += f"{{:<{max_length}}}"
    return format_str

def print_table_header(schema, format_str):
    print("=" * len(format_str.format(*schema)))
    print(format_str.format(*schema))
    print("=" * len(format_str.format(*schema)))

# max_length must be >= 6
def print_row(row_dict, schema, format_str, max_length):
    row_list = []
    for field in schema:
        field_value = row_dict[field]
        field_value += "   "
        if len(field_value) > max_length:
            field_value = field_value[:max_length - 6] + "...   "
            row_list.append(field_value)
        else:
            row_list.append(field_value)
    print(format_str.format(*row_list))