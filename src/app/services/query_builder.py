from pydantic.dataclasses import dataclass


class Config:
    arbitrary_types_allowed = True


@dataclass(config=Config)
class QueryBuilder:
    metadata_file_path: str

    def build(self) -> None:
        """ Method to trigger the build
        """

        # parsed_meta_file: Any = MetadataParser(self.metadata_file_path).read_config()  --Todo
        print(f"building query from meta_file -> {self.metadata_file_path}")
