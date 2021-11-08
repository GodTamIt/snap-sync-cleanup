#! /usr/bin/env python3

import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import PosixPath
from typing import List, Optional, Tuple, Union

logger = logging.getLogger("snap-sync-cleanup")
LOG_FORMAT = "%(name)s:%(lineno)d::%(levelname)s: %(message)s"


def set_up_logging(log_level, use_color: bool):
    use_colorlog = False
    if use_color:
        try:
            import colorlog

            use_colorlog = True
        except ImportError:
            pass

    if use_colorlog:
        handler_module = colorlog
        formatter = colorlog.ColoredFormatter(
            f"%(log_color)s{LOG_FORMAT}",
            log_colors={
                "DEBUG": "white",
                "INFO": "blue",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    else:
        handler_module = logging
        formatter = logging.Formatter(LOG_FORMAT)

    logger.setLevel(log_level)

    # Send all logs <= INFO to stdout
    stdout_handler = handler_module.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    # Send all logs >= WARN to stderr
    stderr_handler = handler_module.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    # Now that logger is set up, notify user that colorlog could not be imported.
    if not use_colorlog and use_color:
        logger.warn("Unable to import colorlog dependency for colored logging.")


def log_external_output(
    stdout: Union[str, bytes], stderr: Union[str, bytes], log_level=logging.DEBUG
):
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8")
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8")

    stdout = stdout.strip()
    stderr = stderr.strip()

    if stderr:
        logger.log(log_level, stderr)

    if stdout:
        logger.log(log_level, stdout)


def get_latest_snapshot_num(config: str) -> Optional[int]:
    """Grabs the number of the latest snap-sync snapshot.

    This relies on snap-sync marking the latest snapshots with the string
    "latest incremental backup".
    """
    snapper = subprocess.run(
        ["snapper", "-c", config, "list", "--columns", "number,description"],
        capture_output=True,
    )

    snapper_stdout = snapper.stdout.decode("utf-8").strip()

    if snapper.returncode != 0:
        snapper_stderr = snapper.stderr.decode("utf-8").strip()

        if "No permissions" in snapper_stderr:
            logger.error(
                f"Insufficient permissions to get config '{config}' info from snapper. Try running as sudo?"
            )
        else:
            logger.error(f"Failed to get info on config '{config}' from snapper")

        log_external_output(snapper_stdout, snapper_stderr)

        exit(1)

    for line in reversed(snapper_stdout.splitlines()):
        if "latest incremental backup" not in line:
            continue

        num = line.split(" ")[0]
        try:
            num = int(num)
            return num
        except:
            pass

    return None


def get_snapshot_root_path(remote: str, config: str) -> PosixPath:
    path = PosixPath(remote, config).absolute()

    if not path.exists() or not path.is_dir():
        logger.error(f"Could not find snapshot path '{path}'")
        exit(1)

    return path


def get_snapshots(path: PosixPath) -> List[Tuple[int, PosixPath]]:
    """Retrieves a list of all snapshots in a directory.

    Returns a list of tuples containing each snapshot's number and PosixPath.
    """
    snapshots = []

    for child in path.iterdir():
        if not child.is_dir():
            logger.debug(f"Ignoring non-directory snapshot candidate '{child}'")
            continue

        try:
            num = int(child.name)
        except ValueError:
            logger.debug(f"Ignoring non-numerical snapshot candidate: '{child}'")
            continue

        snapshots.append((num, child))

    return snapshots


def delete_snapshot(path: PosixPath) -> bool:
    assert path.is_absolute()

    subvolume_path = path.joinpath("subvolume").absolute()
    delete_result = subprocess.run(["btrfs subvolume delete", subvolume_path])

    if delete_result.returncode != 0:
        logger.error(f"Failed to delete subvolume '{subvolume_path}'")
        log_external_output(delete_result.stdout, delete_result.stderr)
        return False

    try:
        shutil.rmtree(str(path))
    except Exception as e:
        logging.warning(f"Failed to delete outer snapshot directory '{path}'")
        logger.debug(e)
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "snap-sync-cleanup", description="Cleans up old remote snap-sync backups."
    )
    parser.add_argument(
        "-c", "--config", required=True, type=str, help="the snapper config to use."
    )
    parser.add_argument(
        "-r", "--remote", required=True, type=str, help="the remote volume path."
    )
    parser.add_argument(
        "-m",
        "--max-keep",
        required=True,
        type=int,
        help="the max number of backups to keep.",
    )
    parser.add_argument(
        "--no-color", action="store_true", help="emit logs without colors"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="verbose output."
    )

    args = parser.parse_args()

    if args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARN

    set_up_logging(log_level, not args.no_color)

    max_keep: int = args.max_keep
    assert max_keep >= 0
    if max_keep == 0:
        logging.warning("The --max-keep flag was set to 0. Deleting all snapshots.")

    # Find the latest backup number
    latest_backup_num = get_latest_snapshot_num(args.config)
    logger.info(f"Latest backup number: {latest_backup_num}")

    # Get the base directory containing all of the snapshots.
    snapshot_path = get_snapshot_root_path(args.remote, args.config)

    # Find the snapshots.
    snapshots = get_snapshots(snapshot_path)

    # Sort snapshots numerically
    snapshots.sort(key=lambda x: x[0])

    logger.debug(
        f"List of {len(snapshots)} snapshots: {','.join(str(x[0]) for x in snapshots)}"
    )

    delete_count = 0
    delete_attempts = 0
    for (num, path) in snapshots:
        if len(snapshots) - delete_attempts <= max_keep:
            break

        if num == latest_backup_num and max_keep > 0:
            logger.debug(f"Skipping latest backup number ({num})")
            continue

        if path == PosixPath("/"):
            logging.warning(f"Skipping '{path}' since it resolves to the root path")
            continue

        # If code has reached here, the snapshot is delete-eligible.
        delete_attempts = 0

        if delete_snapshot(path):
            logger.info(f"Successfully deleted snapshot {num}")
            delete_count += 1

    logger.info(f"Successful deletes: {delete_count}")
    logger.info(f"Delete attempts: {delete_attempts}")
    logger.info(f"Total snapshots discovered: {len(snapshots)}")

    # Exit with error code if there were failed to delete attempts.
    if delete_attempts > delete_count:
        exit(1)
