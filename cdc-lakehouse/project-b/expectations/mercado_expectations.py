# Great Expectations Suite for Projeto B - Mercado

This suite validates the key data quality indicators that feed the SLOs defined in `slo_catalog.yaml` for the `stg_coingecko__mercado` table.

## Expectations definitions

### expectations/mercado_expectations.py
```python
import great_expectations as gx
from great_expectations.core import ExpectationSuite

def create_mercado_suite():
    suite = ExpectationSuite(name="mercado_expectations")

    # 1. Preço deve ser positivo
    suite.add_expectation(
        gx.expectation.expect_column_values_to_be_between(
            column="preço",
            min_value=0.01,
            max_value=None
        )
    )

    # 2. Volume negociado deve ser não-negativo
    suite.add_expectation(
        gx.expectation.expect_column_values_to_be_between(
            column="volume",
            min_value=0,
            max_value=None
        )
    )

    # 3. Moeda must be a known ISO code (ex.: BTC, ETH, DOGE)
    suite.add_expectation(
        gx.expectation.expect_column_values_to_be_in_set(
            column="moeda",
            value_set=["BTC", "ETH", "DOGE", "BNB", "ADA"]
        )
    )

    # 4. Timestamp dentro de janela esperada (ultimas 2h)
    suite.add_expectation(
        gx.expectation.expect_column_values_to_not_be_greater_than(
            column="timestamp",
            value="2025-12-31T23:59:59Z"
        )
    )

    return suite
```

## Como usar

```bash
# No ambiente Spark/Databricks
python -m great_expectations suite validate expectations/mercado_expectations.py
```

## Integração com SLOs

- Os resultados desses expectations são consumidos pelos verificadores de frescor e volume do `slo_catalog.yaml`.
- Falhas acima do threshold definido nos SLOs acionam alertas no painel de saúde.
```