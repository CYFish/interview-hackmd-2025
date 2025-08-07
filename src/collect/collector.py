import json
import logging

from sickle import Sickle

import boto3
import pendulum
import requests


logging.basicConfig(level=logging.INFO)


class ArxivCollector:
    """
    A collector for ArXiv metadata.

    Collects data from the arXiv API and saves it as raw JSON to S3.
    """
    def __init__(self, from_str: str, to_str: str):
        self.s3 = boto3.client("s3", region_name="ap-northeast-1")
        self.bucket = "hackmd-project-2025"
        self.data = {}
        try:
            self.from_date = pendulum.parse(from_str)
            self.to_date = pendulum.parse(to_str)
        except Exception as e:
            logging.error(f"Error parsing from_date or to_date: {e}")
            raise

    def collect(self) -> dict:
        """Main collection process"""
        sickle = Sickle("https://oaipmh.arxiv.org/oai")

        params = {
            "from": self.from_date.to_date_string(),
            "until": self.to_date.to_date_string(),
        }

        print("==================================================")

        metadata_formats = ["arXiv", "arXivRaw"]
        for format in metadata_formats:
            params["metadataPrefix"] = format
            records = sickle.ListRecords(**params)

            for i, record in enumerate(records):
                metadata = record.metadata
                print(metadata)
                break
            print("==================================================")


    def get_latest_data(metadata):
        return {
            "id": metadata["id"][0],
            "title": metadata["title"][0],
            "doi": metadata["doi"][0],
            "categories": metadata["categories"][0],
            "journal-ref": metadata["journal-ref"][0],
            "abstract": metadata["abstract"][0],
            "update_date": metadata["updated"][0],
            "authors_parsed": [[pair[0], pair[1], ""] for pair in zip(metadata["keyname"], metadata["forenames"])],
        }

    def get_internal_data(metadata):
        return {
            "id": metadata["id"][0],
            "submitter": metadata["submitter"][0],
            "authors": metadata["authors"][0],
            "versions": sorted([{"version": pair[0], "created": pair[1]} for pair in zip(metadata["version"], metadata["date"])], key=lambda x: x["version"])
        }

def main(from_date: str, to_date: str):
    collector = ArxivCollector(
        from_str=from_date,
        to_str=to_date
    )
    collector.collect()


if __name__ == "__main__":
    main(
        from_date="2025-08-01",
        to_date="2025-08-02"
    )
