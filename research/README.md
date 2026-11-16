# research

Experimentos puntuales; no forman parte del pipeline diario.

## revision_model_experiment.py (2026-11-16, tenant tsp)

¿Un modelo entrenado (LightGBM, regresión por cuantiles) predice la fecha de
emisión de una revisión mejor que el método actual de `src/risk.mjs` (mediana
histórica por grupo)? Mismas filas y misma información para ambos, con corte
temporal: se entrena con checkpoints anteriores al corte cuyo resultado ya se
conocía y se evalúa en los posteriores.

Resultado: **no lo supera**. Error medio a nivel revisión 16–25 días (modelo)
contra 16–21 (método actual) según el corte; a nivel fin de proyecto es algo
mejor en los cortes más recientes (17–24 contra 22–26 días) y peor en los
primeros, y su estimación pesimista queda mal calibrada (cubre 37–86 % de los
casos contra 85–93 % del método actual). La causa principal es la censura: al
entrenar solo se conocen los resultados de las revisiones que ya se emitieron,
que son las más rápidas. Repetir con un margen de 90 o 150 días no lo corrige.

Volver a probar cuando el lake acumule meses de snapshots: ahí habrá historia
real de cómo se movieron los forecasts, que hoy no existe.

Cómo correrlo: copiar a `data/` los parquet del último snapshot del tenant
(`list_document_forecast`, `documents`, `projects`, `origen_type`, `hours`) y

```bash
python -m venv venv && ./venv/bin/pip install duckdb pandas pyarrow scikit-learn lightgbm
./venv/bin/python revision_model_experiment.py [margen_en_dias]
```
