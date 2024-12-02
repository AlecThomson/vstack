from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Awaitable

from astropy.table import Table, vstack
from tqdm.asyncio import tqdm


async def vstack_tables(
        input_paths: list[Path], 
        output_path: Path,
        overwrite: bool = False
) -> Awaitable[None]:

    if output_path.exists() and not overwrite:
        msg = f"{output_path} already exists. Use --overwrite to overwrite the file."
        raise FileExistsError(msg)

    tasks = []
    for input_path in input_paths:
        task = asyncio.create_task(
            asyncio.to_thread(Table.read, input_path)
        )
        tasks.append(task)

    table_list = await tqdm.gather(*tasks)
    stacked_table = vstack(table_list)

    stacked_table.write(output_path, overwrite=overwrite)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_paths", type=Path, nargs="+")
    parser.add_argument("output_path", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    asyncio.run(
        vstack_tables(
            args.input_paths,
            args.output_path,
            args.overwrite
        )
    )


if __name__ == "__main__":
    main()
