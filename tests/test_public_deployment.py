import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("entrypoint", ["streamlit_app.py", "deploy/streamlit_app.py"])
def test_public_entrypoint_runs_without_pipeline_imports(tmp_path, entrypoint):
    program = """
import importlib.abc
import sys

class DashboardOnlyImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyspark', 'delta', 'confluent_kafka', 'azure'}:
            raise ImportError(f'Pipeline library is not allowed in the public app: {fullname}')

sys.meta_path.insert(0, DashboardOnlyImports())
from streamlit.testing.v1 import AppTest

page = AppTest.from_file(sys.argv[1], default_timeout=30).run()
assert not page.exception, page.exception
assert not page.error, page.error
assert not page.sidebar.text_input
assert page.tabs[0].metric[0].value == '100'
assert page.tabs[1].metric[0].value == '2'
for product in ['BTC-USD', 'ETH-USD']:
    page.selectbox[0].select(product).run()
    assert not page.exception, page.exception
    assert page.tabs[0].metric[0].value == '50'
    assert page.tabs[1].metric[0].value == '1'
print('Public dashboard verified without pipeline imports')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(PROJECT_ROOT / entrypoint)],
        cwd=tmp_path, capture_output=True, text=True, timeout=90, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Public dashboard verified" in result.stdout
