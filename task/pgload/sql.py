class SQL:
    def __init__(self, table: str, fields: list):
        self.table = table
        self.fields = fields
        self.non_key_fields = [field for field in self.fields if field != self.table]

    def clone_table(self):
        return f"CREATE TABLE {self.table}__new (LIKE {self.table} INCLUDING ALL);"

    def copy(self):
        return f"""
            COPY {self.table}__new (
                {",".join(self.fields)}
            ) FROM STDIN WITH (
                FORMAT CSV,
                HEADER,
                DELIMITER '|',
                FORCE_NULL(
                    {",".join(self.non_key_fields)}
                )
            );
        """

    def rename_tables(self):
        return f"""
            ALTER TABLE {self.table} RENAME TO {self.table}__old;
            ALTER TABLE {self.table}__new RENAME TO {self.table};
        """

    def drop_clone_table(self):
        return f"DROP TABLE {self.table}__old;"
