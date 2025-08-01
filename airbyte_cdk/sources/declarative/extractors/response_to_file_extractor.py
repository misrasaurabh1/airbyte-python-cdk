#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import os
import uuid
import zlib
from contextlib import closing
from dataclasses import InitVar, dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

import pandas as pd
import requests
from numpy import nan

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor

DEFAULT_ENCODING: str = "utf-8"
DOWNLOAD_CHUNK_SIZE: int = 1024 * 10

DEFAULT_ENCODING: str = "utf-8"
DOWNLOAD_CHUNK_SIZE: int = 1024 * 10


@dataclass
class ResponseToFileExtractor(RecordExtractor):
    """
    This class is used when having very big HTTP responses (usually streamed) which would require too much memory so we use disk space as
    a tradeoff.

    Eventually, we want to support multiple file type by re-using the file based CDK parsers if possible. However, the lift is too high for
    a first iteration so we will only support CSV parsing using pandas as salesforce and sendgrid were doing.
    """

    parameters: InitVar[Mapping[str, Any]]

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.logger = logging.getLogger("airbyte")

    def _get_response_encoding(self, headers: Dict[str, Any]) -> str:
        """
        Get the encoding of the response based on the provided headers. This method is heavily inspired by the requests library
        implementation.

        Args:
            headers (Dict[str, Any]): The headers of the response.
        Returns:
            str: The encoding of the response.
        """
        content_type = headers.get("content-type")
        if not content_type:
            return DEFAULT_ENCODING

        # Parse like requests: look for 'charset=' in the header
        parts = content_type.split(";")
        for part in parts[1:]:
            if "charset=" in part:
                return part.split("charset=", 1)[1].strip().strip("'\"")
        return DEFAULT_ENCODING

    def _filter_null_bytes(self, b: bytes) -> bytes:
        """
        Filter out null bytes from a bytes object.

        Args:
            b (bytes): The input bytes object.
        Returns:
            bytes: The filtered bytes object with null bytes removed.

        Referenced Issue:
            https://github.com/airbytehq/airbyte/issues/8300
        """

        res = b.replace(b"\x00", b"")
        if len(res) < len(b):
            self.logger.warning(
                "Filter 'null' bytes from string, size reduced %d -> %d chars", len(b), len(res)
            )
        return res

    def _save_to_file(self, response: requests.Response) -> Tuple[str, str]:
        """
        Saves the binary data from the given response to a temporary file and returns the filepath and response encoding.

        Args:
            response (Optional[requests.Response]): The response object containing the binary data. Defaults to None.

        Returns:
            Tuple[str, str]: A tuple containing the filepath of the temporary file and the response encoding.

        Raises:
            ValueError: If the temporary file does not exist after saving the binary data.
        """
        decompressor = zlib.decompressobj(zlib.MAX_WBITS | 32)
        needs_decompression = True
        tmp_file = str(uuid.uuid4())
        null_bytes_filtered = False

        with closing(response) as response, open(tmp_file, "wb") as data_file:
            response_encoding = self._get_response_encoding(response.headers)

            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                out = None
                try:
                    if needs_decompression:
                        out = decompressor.decompress(chunk)
                        data_file.write(out)
                    else:
                        # Only filter if necessary
                        if chunk.find(b"\x00") != -1:
                            null_bytes_filtered = True
                            out = chunk.replace(b"\x00", b"")
                        else:
                            out = chunk
                        data_file.write(out)
                except zlib.error:
                    # fallback: not compressed, treat as normal bytes
                    needs_decompression = False
                    if chunk.find(b"\x00") != -1:
                        null_bytes_filtered = True
                        out = chunk.replace(b"\x00", b"")
                    else:
                        out = chunk
                    data_file.write(out)

            # After all chunks, warn *once* if any nulls were filtered
            if null_bytes_filtered:
                if not hasattr(self, "logger"):
                    self.logger = logging.getLogger("airbyte")
                self.logger.warning(
                    "Filtered 'null' bytes from at least one chunk in the response file '%s'.",
                    tmp_file,
                )

        if os.path.isfile(tmp_file):
            return tmp_file, response_encoding
        else:
            raise ValueError(
                f"The IO/Error occured while verifying binary data. Tmp file {tmp_file} doesn't exist."
            )

    def _read_with_chunks(
        self, path: str, file_encoding: str, chunk_size: int = 100
    ) -> Iterable[Mapping[str, Any]]:
        """
        Reads data from a file in chunks and yields each row as a dictionary.

        Args:
            path (str): The path to the file to be read.
            file_encoding (str): The encoding of the file.
            chunk_size (int, optional): The size of each chunk to be read. Defaults to 100.

        Yields:
            Mapping[str, Any]: A dictionary representing each row of data.

        Raises:
            ValueError: If an IO/Error occurs while reading the temporary data.
        """

        try:
            with open(path, "r", encoding=file_encoding) as data:
                chunks = pd.read_csv(
                    data, chunksize=chunk_size, iterator=True, dialect="unix", dtype=object
                )
                for chunk in chunks:
                    chunk = chunk.replace({nan: None}).to_dict(orient="records")
                    for row in chunk:
                        yield row
        except pd.errors.EmptyDataError as e:
            self.logger.info(f"Empty data received. {e}")
            yield from []
        except IOError as ioe:
            raise ValueError(f"The IO/Error occured while reading tmp data. Called: {path}", ioe)
        finally:
            # remove binary tmp file, after data is read
            os.remove(path)

    def extract_records(
        self, response: Optional[requests.Response] = None
    ) -> Iterable[Mapping[str, Any]]:
        """
        Extracts records from the given response by:
            1) Saving the result to a tmp file
            2) Reading from saved file by chunks to avoid OOM

        Args:
            response (Optional[requests.Response]): The response object containing the data. Defaults to None.

        Yields:
            Iterable[Mapping[str, Any]]: An iterable of mappings representing the extracted records.

        Returns:
            None
        """
        if response:
            file_path, encoding = self._save_to_file(response)
            yield from self._read_with_chunks(file_path, encoding)
        else:
            yield from []
