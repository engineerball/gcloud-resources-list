"""Command-line interface for gcp-inventory."""

from __future__ import annotations

import argparse
import logging
import sys

from google.auth.exceptions import GoogleAuthError

from gcp_inventory.config import ConfigError
from gcp_inventory.inventory import EXIT_FATAL, run_inventory, setup_logging
from gcp_inventory.registry import CollectorSelectionError, all_collectors
from gcp_inventory.runner import DEFAULT_WORKERS

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="debug logging"
    )

    parser = argparse.ArgumentParser(
        prog="gcp-inventory",
        description="Inventory GCP resources across projects into CSV files.",
        parents=[common],
    )
    # SUPPRESS on the flag (in both root and subparsers, via the shared
    # `common` parent) keeps a later subparser from clobbering a root-level
    # -v. Note: parser.set_defaults(verbose=...) must NOT be used here - it
    # mutates the shared action's default in place, undoing the SUPPRESS on
    # every parser that shares this parent. Callers should read the flag via
    # getattr(args, "verbose", False).
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run the inventory and write CSVs", parents=[common])
    p_run.add_argument("--config", default="config.yaml", help="config file path")
    p_run.add_argument("--output", default="output", help="output root directory")
    p_run.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS, help="parallel API calls"
    )
    p_run.add_argument(
        "--collector",
        action="append",
        dest="collectors",
        help="collector name to run; repeatable (default: all)",
    )
    p_run.add_argument(
        "--list-collectors",
        action="store_true",
        help="list available collector names and exit",
    )

    p_cfg = sub.add_parser(
        "config",
        help="generate the config file from a GCP folder (overwrites)",
        parents=[common],
    )
    p_cfg.add_argument("--folder-id", required=True, help="GCP folder ID")
    p_cfg.add_argument(
        "--location",
        action="append",
        dest="locations",
        help="location/region to inventory; repeatable (default: asia-southeast1)",
    )
    p_cfg.add_argument("--output", default="config.yaml", help="config file to write")

    p_sa = sub.add_parser(
        "sa", help="service-account tools (single project)", parents=[common]
    )
    sa_sub = p_sa.add_subparsers(dest="sa_command", required=True)

    p_roles = sa_sub.add_parser(
        "roles", help="dump SA project-role bindings to CSV", parents=[common]
    )
    p_roles.add_argument("--source-project", required=True)
    p_roles.add_argument("--output-csv", default="sa_roles.csv")

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    setup_logging(getattr(args, "verbose", False))
    try:
        sys.exit(_dispatch(args))
    except (CollectorSelectionError, ConfigError, GoogleAuthError) as exc:
        logger.error("%s", exc)
        sys.exit(EXIT_FATAL)


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "run":
        if args.list_collectors:
            for spec in all_collectors():
                print(f"{spec.name}\t{spec.scope.name}")
            return 0
        return run_inventory(
            args.config,
            args.output,
            max_workers=args.workers,
            collector_names=args.collectors,
        )

    if args.command == "config":
        from gcp_inventory.folder_config import (
            DEFAULT_LOCATIONS,
            generate_config,
            write_config,
        )

        locations = tuple(args.locations) if args.locations else DEFAULT_LOCATIONS
        write_config(generate_config(args.folder_id, locations), args.output)
        return 0

    if args.command == "sa":
        return _dispatch_sa(args)

    raise AssertionError(f"unhandled command: {args.command}")


def _dispatch_sa(args: argparse.Namespace) -> int:
    from gcp_inventory.auth import get_credentials
    from gcp_inventory.sa.roles import get_service_accounts_with_roles, save_roles_csv

    service_accounts = get_service_accounts_with_roles(
        args.source_project, get_credentials()
    )
    if not service_accounts:
        logger.error("no service accounts found in project %s", args.source_project)
        return EXIT_FATAL

    save_roles_csv(service_accounts, args.output_csv)
    return 0
