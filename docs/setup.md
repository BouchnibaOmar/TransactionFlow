# Installation et environnement local

[← README](../README.md) · [Synthétique](synthetic_pipeline.md) · [NYC](nyc_pipeline.md) · [Mesures](benchmarks.md)

## Depuis un clone neuf

Après avoir cloné le dépôt, ouvrir sa racine dans WSL Ubuntu. Pré-requis : Python
3.13 et Java 21 disponibles dans ce shell (`python3.13 --version`, `java -version`).
Les données et sorties sont ignorées par Git : un clone ne contient pas les démos.

```bash
python3.13 -m venv ~/.venvs/transactionflow
source ~/.venvs/transactionflow/bin/activate
python -m pip install -r requirements.txt
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
python generate_data.py --rows 10000 --seed 42 --output data/raw/quickstart
python run_silver.py --input data/raw/quickstart --output data/silver/quickstart
python run_gold.py --silver data/silver/quickstart/transactions_clean --references data/raw/quickstart --output data/gold/quickstart
```

La création du venv concerne un environnement neuf ; si le venv existe, l'activer
sans le recréer. Les noms `quickstart` supposent des dossiers absents ou vides.
Lors d'une répétition, choisir de nouveaux noms pour préserver les anciennes sorties.
Ces commandes sont une procédure proposée : les arguments et chemins sont vérifiés
contre les CLI existantes, mais ce démarrage complet n'a pas été relancé pendant
la réorganisation documentaire. Aucune variable globale Windows n'est modifiée.

## Tests et portée du résultat local

**89 tests réussis localement, aucune CI configurée.** Ce nombre réunit deux
vérifications locales distinctes : la suite de 80 tests Spark (305,48 s), puis les
9 tests du mesureur (1,44 s). Il ne désigne pas une nouvelle exécution complète
unique de 89 tests. Le badge statique du README ne s'actualise pas automatiquement.
Aucun test ni pipeline n'a été relancé lors de cette mise à jour documentaire.

Commande disponible pour une future vérification de la suite :

```bash
python -m pytest -q --basetemp artifacts/pytest-temp
```

Les fixtures utilisent des fichiers temporaires, indépendants des données demo.
Le dossier `--basetemp` est réservé aux tests et peut être nettoyé par pytest.
Les sections suivantes conservent les commandes et résultats locaux antérieurs.

## Organisation

| Fichier | Responsabilité |
|---|---|
| `generate_data.py` | Générer les CSV et leur manifeste avec la bibliothèque standard |
| `silver_io.py` | Lire les CSV, vérifier les chemins et écrire les sorties |
| `silver_rules.py` | Valider les références et transactions, isoler les conflits, dédupliquer |
| `run_silver.py` | Configurer Spark local et lancer le pipeline par CLI |
| `gold_io.py` | Lire Silver et les références, protéger les chemins et écrire Gold |
| `gold_rules.py` | Joindre les références, agréger par mois et réconcilier par devise |
| `run_gold.py` | Lancer Gold par CLI avec la session Spark locale existante |
| `nyc_io.py` | Lire les sources NYC, tracer leurs empreintes et vérifier les sorties relues |
| `nyc_rules.py` | Contrôler le schéma, joindre les zones et classifier les trajets NYC |
| `run_nyc_silver.py` | Lancer uniquement Silver NYC, séparément du pipeline synthétique |
| `nyc_gold_io.py` | Lire Silver NYC, protéger les chemins et publier Gold après relecture |
| `nyc_gold_rules.py` | Contrôler les libellés et agréger les montants enregistrés par jour/zone/paiement |
| `run_nyc_gold.py` | Lancer Gold NYC depuis le dossier Parquet `trips_clean` |
| `measure_cli.py` | Mesurer une CLI Python et la somme RSS de son arbre de processus |
| `run_nyc_measurements.py` | Mesurer trois paires NYC séquentielles et vérifier leurs rapports |
| `check_parquet.py` | Vérifier séparément une petite écriture et relecture Parquet |
| `project_python.cmd` | Configurer Java et Python pour un processus Windows |
| `tests/` | Vérifier la génération, Silver et Gold sur des données temporaires |
| `pytest.ini` | Limiter la collecte aux tests du projet, hors copies de vérification |

`requirements.txt` fixe PySpark 4.0.2, pytest 9.1.1 et psutil 7.2.2. Les données, sorties,
environnements et caches sont exclus de Git. Aucun script n’installe de dépendance
ni ne change les variables globales.

## WSL Ubuntu

Environnement Linux : Python 3.13.16, Java 21, PySpark 4.0.2 et pytest 9.1.1.
L’environnement existant est `~/.venvs/transactionflow`, distinct de `.venv` Windows.
Commandes pour lancer les tests depuis Ubuntu :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow
source ~/.venvs/transactionflow/bin/activate
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
python -m pytest -q --basetemp artifacts/pytest-temp
```

`artifacts/pytest-temp` est réservé aux tests et nettoyé par pytest. Les tests génèrent
leurs données déterministes et ne nécessitent pas `data/raw/demo`. Les variables
exportées concernent seulement le shell courant et ses processus enfants.

Exemple de lancement Silver dans une nouvelle sortie :

```bash
python run_silver.py --input data/raw/demo --output data/silver/demo_wsl_verification
```

Un dossier non vide est refusé sans `--overwrite`. Cette option remplace seulement
les deux dossiers Parquet et le rapport. Les sources et sorties ne peuvent être
identiques ni imbriquées. Le générateur possède aussi une option `--overwrite`,
qui remplace ses quatre fichiers. Les autres fichiers sont conservés.

## Windows natif / CMD

Le lanceur configure Java 21 et le Python de `.venv` avec `setlocal`.
Commandes exécutées lors du contrôle Windows :

```bat
project_python.cmd check_parquet.py
project_python.cmd -m pytest -q -s --tb=short --basetemp data/test-temp
project_python.cmd run_silver.py --input data/raw/demo --output data/silver/demo
```

Résultat observé : **16 tests réussis et 2 échecs**. L’écriture Parquet a échoué via
`Shell.getWinUtilsPath` et `RawLocalFileSystem.setPermission` avec :

```text
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

Aucun binaire winutils n’a été téléchargé. La limite Windows reste distincte des
résultats Linux. `data/test-temp` est réservé à pytest. Le diagnostic Parquet utilise
et remplace uniquement `data/parquet_check`.

## Points à comprendre

Pour les mesures, relire `sample_tree` et `measure_command` : somme RSS instantanée,
identification des processus et limites de l’échantillonnage. Relire `verify_report`
et `run_series` pour l’ordre des six CLI, les nouveaux chemins et l’arrêt sur erreur.

Pour Gold NYC, relire `consistent_labels`, `attach_labels` et `aggregate_daily`
pour les libellés cohérents, les clés NULL et les sommes décimales signées.
Relire `reconcile_daily` et `write_daily` pour les contrôles avant/après écriture.

Pour NYC, relire `validate_trip_schema`, `enrich_zones` et `classify_trips` pour
comprendre la préservation des valeurs, les deux left joins et la distinction entre
quarantaine et flags. Relire `quality_report` et `write_nyc_outputs` pour les compteurs
qui se chevauchent, la conservation des multiplicités et le rapport après relecture.

Relire `generate_data` et `write_transactions` pour la graine, l’ordre des tirages et
l’écriture progressive ; `write_anomalies` pour le lien au manifeste. Relire
`validate_reference`, `find_conflicting_ids` et `prepare_silver` pour les jointures,
la quarantaine et les compteurs. Enfin, relire `validate_transactions`,
`deduplicate_transactions` et les tests Parquet pour distinguer valeurs brutes,
valeurs converties et égalité logique des sorties.

Pour Gold, relire `join_reference` : la jointure anti détecte les références absentes,
puis la jointure interne ajoute leurs attributs. Relire `aggregate_monthly` pour
les clés année/mois/entité/devise, et `reconcile_monthly` pour la conservation des
comptes et montants. Les résultats calculés à la main sont dans `tests/test_gold.py`.

Les conversions sont décrites dans la
[documentation Spark 4.0.2](https://spark.apache.org/docs/4.0.2/api/sql/index.html).
