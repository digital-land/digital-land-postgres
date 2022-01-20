class SQL:

    def __init__(self, table:str, fields:list):
        self.table = table
        self.fields = fields
        self.non_key_fields = [field for field in self.fields if field != self.table]

    def clone_table(self):
        sql = (
            f"CREATE TEMPORARY TABLE __temp_table AS (SELECT * FROM {self.table} LIMIT 0);"
        )
        return sql

    def copy(self):

        sql = f"""
            COPY __temp_table (
                {",".join(self.fields)}
            ) FROM STDIN WITH (
                FORMAT CSV, 
                HEADER, 
                DELIMITER '|', 
                FORCE_NULL(
                    {",".join(self.non_key_fields)}
                )
            )
        """
        return sql

    def upsert(self):
        excluded = [f"{field}=EXCLUDED.{field}" for field in self.non_key_fields]
        sql = f"""
            INSERT INTO {self.table}
            SELECT *
            FROM __temp_table
            ON CONFLICT ({self.table}) DO UPDATE
            SET {",".join(excluded)};
        """
        return sql

    def drop_clone_table(self):
        sql = (
            f"DROP TABLE __temp_table;"
        )
        return sql

