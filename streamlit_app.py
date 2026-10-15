import sys
from importlib import import_module, util
from pathlib import Path


def main() -> None:
    if "marketguard" not in sys.modules:
        source_path = Path(__file__).resolve().parent / "src"
        specification = util.spec_from_file_location(
            "marketguard", source_path / "__init__.py",
            submodule_search_locations=[str(source_path)],
        )
        if specification is None or specification.loader is None:
            raise RuntimeError("Unable to locate the MarketGuard dashboard package")
        package = util.module_from_spec(specification)
        sys.modules["marketguard"] = package
        specification.loader.exec_module(package)
    import_module("marketguard.dashboard.app").main(public_demo=True)


if __name__ == "__main__":
    main()
