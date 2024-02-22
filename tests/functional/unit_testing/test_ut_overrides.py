import pytest
from dbt.tests.util import run_dbt


my_model_with_macros = """
SELECT
{{ current_timestamp() }} as global_dbt_macro,
{{ dbt.current_timestamp() }} as dbt_macro,
{{ my_macro() }} as global_user_defined_macro,
{{ dbt_utils.generate_surrogate_key() }} as package_macro
"""

# TODO: add tests for global override, namespaced usage

test_my_model_with_macros = """
unit_tests:
  - name: test_macro_overrides
    model: my_model_with_macros
    overrides:
      macros:
        current_timestamp: "'global_dbt_macro_override'"
        dbt.current_timestamp: "'dbt_macro_override'"
        my_macro: "'global_user_defined_macro_override'"
        dbt_utils.generate_surrogate_key: "'package_macro_override'"
    given: []
    expect:
      rows:
        - global_dbt_macro: "global_dbt_macro_override"
          dbt_macro: "dbt_macro_override"
          global_user_defined_macro: "global_user_defined_macro_override"
          package_macro: "package_macro_override"
"""

MY_MACRO_SQL = """
{% macro my_macro() -%}
  {{ test }}
{%- endmacro %}
"""


class TestUnitTestingMacroOverrides:
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": "1.1.1",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_with_macros.sql": my_model_with_macros,
            "test_my_model_with_macros.yml": test_my_model_with_macros,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"my_macro.sql": MY_MACRO_SQL}

    def test_macro_overrides(self, project):
        run_dbt(["deps"])

        # Select by model name
        results = run_dbt(["test", "--select", "my_model_with_macros"], expect_pass=True)
        assert len(results) == 1
