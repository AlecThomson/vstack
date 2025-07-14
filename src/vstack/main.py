from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Awaitable
from pathlib import Path

from astropy.table import Table, vstack

logger = logging.getLogger(__name__)
logging.captureWarnings(True)
ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.INFO)


async def read_table(input_path: Path) -> Awaitable[Table]:
    msg = f"Reading {input_path}"
    logger.info(msg)
    return Table.read(input_path)


async def vstack_tables(
    input_paths: list[Path], output_path: Path, overwrite: bool = False
) -> Awaitable[None]:
    if output_path.exists() and not overwrite:
        msg = f"{output_path} already exists. Use --overwrite to overwrite the file."
        raise FileExistsError(msg)

    coros = []
    for input_path in input_paths:
        coro = read_table(input_path)
        coros.append(coro)

    table_list = await asyncio.gather(*coros)

    logger.info("Stacking tables...")
    stacked_table = vstack(table_list)

    msg = f"Writing stacked table to {output_path}"
    logger.info(msg)
    stacked_table.write(output_path, overwrite=overwrite)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_filtes", type=Path, nargs="+", help="List of paths to input files"
    )
    parser.add_argument("output_files", type=Path, help="Path to output file")
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite output file if it exists"
    )
    args = parser.parse_args()

    asyncio.run(vstack_tables(args.input_filtes, args.output_files, args.overwrite))


if __name__ == "__main__":
    main()
