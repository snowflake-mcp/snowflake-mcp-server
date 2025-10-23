class DropDatabase:
    def drop_database(self, database_name: str) -> str:
        query = f"DROP DATABASE {database_name}"
        return self.process_request(query)