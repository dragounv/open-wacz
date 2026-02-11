from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime
from zipfile import ZipFile

HARVEST_NAME_PREFIX = "Linkra"

SCRIPT_NAME = "extract_wacz.py"
SCRIPT_VERSION = "1.0.0"


class Arguments:
    def __init__(self, namespace: argparse.Namespace):
        self.wacz_path: str = namespace.file
        self.target_path: str = namespace.target_directory


def main():
    parser = argparse.ArgumentParser(
        description="Convert WACZ into directory structure."
    )
    parser.add_argument("file", help="Path to WACZ file.")
    parser.add_argument(
        "target_directory", help="Directory into which will be the result stored."
    )
    ns = parser.parse_args()
    args = Arguments(ns)
    prepare_and_run(args)


def prepare_and_run(args: Arguments):
    with ZipFile(args.wacz_path) as wacz_zip:
        harvest_metadata = get_harvest_metadata(wacz_zip)
        # First extract name of harvest that will be used as name of root dir
        harvest_name = get_harvest_name(wacz_zip, harvest_metadata)
        harvest_path = os.path.join(args.target_path, harvest_name)
        create_directory_structure(harvest_path)
        extract_warcs(wacz_zip, harvest_path, harvest_name)
        # Skipped for now. Because we rename the archive file, the index will be invalid.
        # TODO: Implement rewriting of the index files.
        # extract_indexes(wacz_zip, harvest_path)
        create_info_file(
            harvest_metadata,
            harvest_path,
            os.path.basename(args.wacz_path),
            harvest_name,
        )


def create_info_file(
    harvest_metadata: HarvestMetadata,
    harvest_path: str,
    wacz_file_name: str,
    harvest_name: str,
):
    info_file_path = os.path.join(harvest_path, "logs/crawl/info.txt")
    # TODO: Get rid of datetime completely.
    today = datetime.now().isoformat()
    with open(info_file_path, "w", encoding="utf-8") as info_file:
        print("info: this harvest was extracted from WACZ file", file=info_file)
        print(f"original_file: {wacz_file_name}", file=info_file)
        print(f"converted_with: {SCRIPT_NAME} {SCRIPT_VERSION}", file=info_file)
        print(f"conversion_date: {today}", file=info_file)
        print(f"harvest_name: {harvest_name}", file=info_file)
        print(
            f"wacz_created: {harvest_metadata.required.full_date}",
            file=info_file,
        )
        if harvest_metadata.optional.title:
            print(f"wacz_title: {harvest_metadata.optional.title}", file=info_file)
        if harvest_metadata.optional.software:
            print(
                f"wacz_software: {harvest_metadata.optional.software}", file=info_file
            )
        if harvest_metadata.optional.main_page_url:
            print(
                f"wacz_main_page_url: {harvest_metadata.optional.main_page_url}",
                file=info_file,
            )
        if harvest_metadata.optional.main_page_date:
            print(
                f"wacz_main_page_date: {harvest_metadata.optional.main_page_date}",
                file=info_file,
            )


# Unused for now
def extract_indexes(wacz_zip: ZipFile, harvest_path: str):
    ZIP_PATH = "indexes/"
    path = os.path.join(harvest_path, "logs/cdxj")
    extract_from_to(wacz_zip, ZIP_PATH, path)


def extract_warcs(wacz_zip: ZipFile, harvest_path: str, harvest_name: str):
    ZIP_PATH = "archive/"
    extract_from_to(wacz_zip, ZIP_PATH, harvest_path)
    # Rename "data.warc.gz" to unique name.
    data_warc_path = os.path.join(harvest_path, "data.warc.gz")
    correct_warc_path = os.path.join(harvest_path, harvest_name + ".warc.gz")
    if os.path.exists(data_warc_path):
        os.rename(data_warc_path, correct_warc_path)


def extract_from_to(wacz_zip: ZipFile, from_path: str, to_path: str):
    """Extract files from zip directory 'from_path' into fs directory 'to_path'"""
    for zip_info in wacz_zip.filelist:
        if zip_info.filename.startswith(from_path):
            wacz_zip.extract(zip_info, to_path)
            # Extracting zip files creates unnecessary directory so we need to deal with that
            _, filename_without_path = os.path.split(zip_info.filename)
            shutil.move(
                os.path.join(to_path, zip_info.filename),
                os.path.join(to_path, filename_without_path),
            )
            os.rmdir(os.path.join(to_path, from_path))


def create_directory_structure(harvest_path: str):
    # Root directory for harvest warc files will be extracted here.
    os.mkdir(harvest_path)

    # Top level logs dir.
    logs_path = os.path.join(harvest_path, "logs")
    os.mkdir(logs_path)

    # For old index format. Possibly unused but expected for now.
    cdx_path = os.path.join(logs_path, "cdx")
    os.mkdir(cdx_path)

    # Skipped for now. Because we rename the archive file, the index will be invalid.
    # TODO: Implement rewriting of the index files.
    # For cdxj indexes. Will be extracted from wacz.
    # cdxj_path = os.path.join(logs_path, "cdxj")
    # os.mkdir(cdxj_path)

    # Normally a gzipped logs from heritrix would be here. Instead create a small info file.
    crawl_path = os.path.join(logs_path, "crawl")
    os.mkdir(crawl_path)


def get_harvest_name(wacz_zip: ZipFile, harvest_metadata: HarvestMetadata) -> str:
    prefix = HARVEST_NAME_PREFIX
    date = harvest_metadata.required.date
    filename: str = os.path.basename(wacz_zip.filename)
    filename = filename.split(".")[0]
    return f"{prefix}-{date}-{filename}"


def get_harvest_metadata(wacz_zip: ZipFile) -> HarvestMetadata:
    datapackage_data = wacz_zip.read("datapackage.json")
    datapackage_object = json.loads(datapackage_data)
    return HarvestMetadata(datapackage_object)


class HarvestMetadata:
    def __init__(self, datapackage_json_object):
        self.required = RequiredHarvestMetadata(datapackage_json_object)
        self.optional = OptionalHarvestMetadata(datapackage_json_object)


class RequiredHarvestMetadata:
    def __init__(self, datapackage_json_object: dict):
        if "created" in datapackage_json_object:
            # IMPORTANT: Avoid using datetime package for parsing iso format.
            # Regardless of the method name 'fromisoformat', it can't parse iso format.
            # There is no way to do that in the datetime package (or python standart library it seems)!
            #
            # self.full_date = datetime.fromisoformat(datapackage_json_object["created"])
            # self.date = self.full_date.strftime("%Y-%m")

            # Keep the string, it should be valid ISO format anyway
            self.full_date: str = datapackage_json_object["created"]
            # Time formatting via string manipulation (i really don't want to import external library)
            # At least we know it should be an ISO formatted. So we can keep the year and month like this:
            self.date = self.full_date[:7]
            # 4 chars year + 1 char '-' + 2 chars moth = 7 chars
        else:
            raise ValueError(
                "This script requires that the datapackage.json has a 'created' property, but it was not found!"
            )


class OptionalHarvestMetadata:
    def __init__(self, datapackage_json_object: dict):
        self.title = None
        if "title" in datapackage_json_object:
            self.title = datapackage_json_object["title"]

        self.software = None
        if "software" in datapackage_json_object:
            self.software = datapackage_json_object["software"]

        self.main_page_url = None
        if "mainPageUrl" in datapackage_json_object:
            self.main_page_url = datapackage_json_object["mainPageUrl"]

        self.main_page_date = None
        if "mainPageDate" in datapackage_json_object:
            self.main_page_date = datapackage_json_object["mainPageDate"]


if __name__ == "__main__":
    main()
