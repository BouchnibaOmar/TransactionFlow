# TransactionFlow

Projet pédagogique PySpark : génération de transactions financières synthétiques,
puis lecture, validation et déduplication dans une couche Silver, et agrégations
mensuelles dans une couche Gold. Des mesures locales des CLI sont documentées.
Aucun dashboard, traitement de fraude ou modèle ML n’est inclus.

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

## Génération

```bash
python generate_data.py --rows 10000 --seed 42 --output data/raw/demo
```

Le générateur produit `customers.csv` (identifiant, pays, segment), `merchants.csv`
(identifiant, catégorie), `transactions.csv` (identifiants, date UTC, montant en
centimes, devise EUR) et `manifest.json` (graine, volumes et anomalies).
Les valeurs par défaut sont 100 clients et 20 commerçants, ajustables avec
`--customers` et `--merchants`.

`--rows` désigne les transactions valides de base. Quatre anomalies distinctes sont
ajoutées : une copie exacte, une date impossible, un client absent et un client
inconnu. Avec 10 000 lignes de base, le CSV contient 10 004 transactions, plus
l’en-tête. Les lignes du manifeste sont physiques : l’en-tête est la ligne 1.

Les dates sont réparties dans la période de janvier à juin 2025 ; les montants sont des entiers de 1 à
100 000 centimes. Les transactions sont écrites une par une. Un générateur aléatoire
local est partagé dans un ordre fixe : référentiels, puis transactions. Les mêmes
paramètres et la même graine donnent les mêmes fichiers dans le même environnement Python.

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

## Règles Silver

Spark utilise `local[2]`, UTC et ses fonctions natives, sans UDF Python. Les CSV sont
lus avec un schéma explicite de chaînes. Les rejets conservent les valeurs décodées,
pas les guillemets de la ligne CSV ; les champs vides deviennent `null`. Le lecteur
contrôle l’en-tête et utilise le mode `FAILFAST`.

1. Refuser les identifiants absents ou dupliqués des référentiels avant les jointures,
   pour empêcher la multiplication des transactions.
2. Mettre en quarantaine toutes les lignes d’un identifiant de transaction non vide
   associé à plusieurs contenus bruts, avec `conflicting_transaction_id`.
3. Valider les autres lignes : identifiants obligatoires, date UTC au format
   `yyyy-MM-ddTHH:mm:ssZ`, montant composé de chiffres représentant un `long`
   strictement positif, devise EUR et références client/commerçant existantes.
   Les conversions invalides retournent `null` et alimentent les motifs de rejet.
4. Supprimer les copies exactes parmi les lignes valides en comparant les six colonnes
   brutes. `1` et `01` sont des contenus différents ; les répétitions invalides restent rejetées.

Chaque ligne appartient à une seule catégorie de comptage. Plusieurs motifs ne
multiplient pas les rejets. Les conflits passent avant les autres validations.
Le pipeline ne lit jamais le manifeste, réservé aux tests.

```text
input_transactions = rejected_rows + exact_duplicates_removed + final_transactions
rejected_rows = validation_rejected_rows + conflict_rejected_rows
```

La réconciliation est contrôlée par une exception explicite. Les sorties sont :

- `transactions_clean/` : Parquet, timestamp UTC, montant `long`, partitions `year/month`.
- `transactions_rejected/` : valeurs brutes et tableau `rejection_reasons`.
- `quality_report.json` : volumes et compteurs, écrit après les deux Parquet.

Les deux écritures ne sont pas atomiques : un échec peut laisser une sortie partielle.
Lors d’un remplacement, l’ancien rapport est retiré avant l’écriture pour éviter
un rapport de succès périmé.

## Phase Gold

Gold lit le Parquet Silver `transactions_clean` et les CSV clients/commerçants avec
les schémas explicites existants. Il réutilise les contrôles de référence de Silver
sans modifier ses règles. Les identifiants de référence doivent être uniques et
non vides. Une transaction sans référence fait échouer Gold ; les jointures doivent
conserver exactement le nombre de lignes Silver.

Les tables produites sont partitionnées par `year/month` :

- `merchant_monthly` : année, mois, commerçant, catégorie, devise, nombre de
  transactions et montant total en centimes.
- `customer_monthly` : année, mois, client, pays, segment, devise, nombre de
  transactions et montant total en centimes.

La devise fait partie des clés d’agrégation : EUR et USD ne sont jamais additionnés.
Les montants des tables restent des `long`. Le mode ANSI fait échouer les sommes
dépassant ce type, plutôt que de laisser un entier déborder silencieusement.
Les contrôles monétaires utilisent des décimaux de précision 38 et d’échelle 0,
puis des entiers pour le rapport, sans conversion float. Pour chaque table et
chaque devise, les comptes et montants sont comparés aux totaux Silver.

Une entrée Silver vide est refusée explicitement, sans créer de sortie. Le dossier
Gold ne peut chevaucher ni Silver ni les références. Une sortie non vide est refusée
sans `--overwrite`. Cette option remplace uniquement `merchant_monthly`,
`customer_monthly` et `quality_report.json`.

Le rapport indique les volumes d’entrée, les comptes après jointure, les nombres
de groupes et les totaux réconciliés par devise. Il est écrit après les deux sorties
Parquet. Ces écritures ne sont pas atomiques : un échec peut laisser une sortie
partielle. Lors d’un remplacement, l’ancien rapport est supprimé avant les écritures
pour éviter un ancien rapport de succès attaché à des fichiers partiellement remplacés.

Les tests Gold sont autonomes. Leur jeu de sept transactions donne à la main
4 transactions et 800 centimes EUR, et 3 transactions et 1 700 centimes USD.
Ils contrôlent aussi les références dupliquées/absentes, les erreurs de réconciliation,
un montant au-delà de la précision exacte d’un float, les sorties Parquet, l’entrée
vide et la protection des chemins. `pytest.ini` limite la collecte à `tests/`, pour
exclure les anciennes copies sous `artifacts/`.

### Exécution Gold vérifiée sous WSL

Les commandes suivantes ont été exécutées depuis le projet après activation de
`~/.venvs/transactionflow` :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow
source ~/.venvs/transactionflow/bin/activate
python -m pytest -q --basetemp artifacts/gold-validation/pytest-temp
python run_gold.py --silver data/silver/demo_wsl/transactions_clean --references data/raw/demo --output data/gold/demo_wsl
python artifacts/gold-validation/read_back_demo.py
```

La suite complète donne **32 tests réussis en 134,47 s** : les 18 tests existants
et 14 tests Gold. Le premier essai de collecte rencontrait des copies de tests
sous `artifacts/` ; la configuration `testpaths = tests` a corrigé cette collecte.

Le CLI a créé une nouvelle sortie, sans `--overwrite` : **120 lignes dans
`merchant_monthly` et 600 lignes dans `customer_monthly`**. Chaque table réconcilie
**10 000 transactions et 498 531 403 centimes EUR** avec Silver.
La relecture des Parquet a confirmé ces nombres, les montants de type `long`,
l’unicité des clés mensuelles et les six partitions `year=2025/month=1` à `month=6`.
Le script de relecture est un fichier local de vérification sous `artifacts/`, exclu
de Git ; il ne fait pas partie du pipeline. Le rapport Gold est
`data/gold/demo_wsl/quality_report.json`.

Les empreintes SHA-256 des 31 fichiers préexistants contrôlés sont inchangées :
générateur, modules Silver, anciens tests et fichiers de `data/raw` et `data/silver`.
Gold n’a pas été exécuté sous Windows natif ; la limite Hadoop documentée plus bas
reste celle observée lors des essais Silver.

## Silver NYC

Sources officielles : [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
et [dictionnaire Yellow Taxi](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
Les fichiers locaux sont `data/raw/nyc/yellow_tripdata_2025-01.parquet` et
`data/raw/nyc/taxi_zone_lookup.csv`. La référence de zones est publiée sur la page TLC.
La TLC indique que les données sont fournies par les prestataires et ne garantit
pas leur exactitude. Silver et Gold NYC restent séparés ; aucun modèle ML ou benchmark NYC n’est inclus.

Le schéma Parquet est inspecté avant toute conversion. Les dates requises doivent
être `timestamp_ntz`, les montants et distances `double`, et les identifiants de
zone et codes de paiement des entiers. Toutes les colonnes source et leur multiplicité
sont conservées ; aucun identifiant source n’est inventé et aucun doublon n’est supprimé.
Les noms qui entrent en conflit avec les colonnes dérivées sont refusés.

`--month YYYY-MM` définit une fenêtre sur le pickup : premier jour inclus, premier
jour du mois suivant exclu. Les dates restent des heures civiles `timestamp_ntz`,
sans conversion UTC. La session NYC utilise `America/New_York` et `TIMESTAMP_NTZ` ;
cela n’ajoute pas de fuseau aux dates source.

La quarantaine conserve les valeurs source avec une liste de motifs : dates absentes,
fin strictement avant début, pickup hors mois, montant ou distance manquant/non fini
(NaN ou infinis) et conversion du montant normalisé impossible. **Hors mois est une
exclusion du périmètre analytique, pas une preuve d’erreur source.** Plusieurs motifs
peuvent s’appliquer à une seule ligne ; les compteurs par motif se chevauchent.

Les totaux négatifs ou zéro et les distances négatives ou zéro restent conservés
avec des `quality_flags`. Un montant négatif n’est pas interprété comme un remboursement.
Les paiements manquants, le code 5 (Unknown) et les codes non documentés sont signalés.
`payment_type` est conservé brut ; `payment_label` vaut : 0 Flex Fare, 1 Credit card,
2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided trip ; tout autre code, y compris
une valeur manquante, donne Unmapped.

`total_amount_decimal` est de type `decimal(18,2)` : `round(total_amount, 2)` utilise
l’arrondi Spark **HALF_UP**, avec les demis arrondis en s’éloignant de zéro, puis
`try_cast` effectue la conversion décimale. Les tests contrôlent notamment 1,005 →
1,01 et −1,005 → −1,01. La valeur `total_amount` en double n’est jamais remplacée.
Cette normalisation n’annule pas l’imprécision de la représentation double source.
Les calculs financiers futurs devront utiliser le montant décimal, sans somme en double.

Les zones sont lues avec un schéma CSV explicite. Les `LocationID` manquants,
dupliqués, non entiers ou non positifs sont refusés, y compris les doublons après
normalisation d’identifiants comme `1` et `01`. Deux left joins ajoutent
`pickup_borough/zone/service_zone` et `dropoff_borough/zone/service_zone`, avec
conservation contrôlée des lignes. Une zone absente conserve la ligne, laisse les
attributs à `null` et ajoute un flag. Les libellés spéciaux du CSV sont conservés.

Sorties : `trips_clean/` partitionné par année/mois du pickup et `trips_quarantined/`.
Les sorties vides sont écrites sans partitions pour conserver un fichier Parquet
lisible avec son schéma. Les sorties sont relues et contrôlées par schéma, compte
et comparaison `exceptAll` des valeurs et multiplicités, sans collecte des trajets.
Le rapport `quality_report.json` est écrit après cette relecture. Il contient volumes,
motifs, flags sur les lignes propres, contrôles, schémas, mois, configuration Spark
effective, chemins et SHA-256 des deux sources. Seuls de petits agrégats sont collectés.

La sortie ne peut chevaucher les sources et un dossier non vide est refusé sans
`--overwrite`. Les écritures des deux Parquet ne sont pas atomiques : une erreur
peut laisser une sortie partielle. L’ancien rapport est supprimé avant un remplacement
et aucun nouveau rapport de succès n’est écrit si les contrôles échouent.

### Observations manuelles fournies sur janvier 2025

Ces valeurs ont été fournies avant l’implémentation ; elles ne sont pas des règles
de nettoyage : 3 475 226 lignes, dates `timestamp_ntz`, montants/distances `double`,
aucun NULL dans les sept colonnes inspectées, 124 fins avant début, 63 037 totaux
négatifs, 559 totaux zéro et 90 893 distances zéro. La plage pickup observée allait
du 31 décembre 2024 à 20:47:55 au 1er février 2025 à 00:00:44. Ces compteurs peuvent
se chevaucher. SHA-256 fourni du Parquet :
`9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`.

### Exécutions et relecture effectuées pour Silver NYC

Commandes exécutées sous WSL dans l’environnement Linux existant :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow
source ~/.venvs/transactionflow/bin/activate
python artifacts/nyc-validation/inspect_source.py
python -m pytest -q --basetemp artifacts/nyc-validation/pytest-temp
python run_nyc_silver.py --input data/raw/nyc/yellow_tripdata_2025-01.parquet --zones data/raw/nyc/taxi_zone_lookup.csv --month 2025-01 --output data/silver/nyc_2025_01_run1
python artifacts/nyc-validation/read_back.py
```

Les scripts sous `artifacts/` sont des vérifications locales exclues de Git, pas des
composants du pipeline. La suite finale donne **62 tests réussis en 193,00 s** :
32 tests synthétiques existants et 30 tests NYC. Deux attentes des nouveaux tests
ont été corrigées avant ce résultat : type explicitement `timestamp_ltz` pour tester
un mauvais schéma, et refus d’un fichier comme dossier de sortie.

La CLI sans `--overwrite` et la relecture indépendante confirment :

| Compteur | Résultat |
|---|---:|
| Entrées | 3 475 226 |
| Lignes conservées | 3 475 080 |
| Lignes en quarantaine | 146 |
| Fin avant début | 124 |
| Pickup hors janvier | 22 |
| Dates, montants ou distances manquants | 0 |
| Montants ou distances non finis | 0 |
| Échecs de conversion décimale | 0 |

Sur ce fichier, les deux motifs non nuls totalisent 146 lignes. Le contrôle général
reste `input_rows = clean_rows + quarantined_rows`, sans supposer que les motifs
sont disjoints sur d’autres fichiers. Flags sur les **lignes conservées uniquement** :
63 037 totaux négatifs, 550 totaux zéro, 90 893 distances zéro et 1 paiement inconnu/
non documenté. Les autres flags sont à zéro. Ce périmètre diffère des observations
manuelles sur l’entrée complète.

Les dates `timestamp_ntz`, le montant `decimal(18,2)`, les schémas, volumes, motifs
et flags sont confirmés après relecture. Le pipeline a aussi comparé les valeurs et
multiplicités relues avec les DataFrames avant écriture, sans collecte des trajets.
Le SHA-256 du Parquet correspond à celui fourni manuellement ; celui du CSV de zones
est `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`.
Le rapport est `data/silver/nyc_2025_01_run1/quality_report.json`, avec chemins,
empreintes, schémas et configuration effective : Spark 4.0.2, `local[2]`, session
`America/New_York`, `TIMESTAMP_NTZ`, 2 partitions de shuffle, ANSI et AQE activés.

Les 448 fichiers préexistants contrôlés par SHA-256 sont inchangés : code et tests
synthétiques, benchmarks, données et sorties. Spark a stocké sur disque des blocs
de cache ne tenant pas en mémoire ; la mémoire totale Python + JVM n’a pas été
mesurée. Les comparaisons `exceptAll` utilisent des shuffles et du stockage temporaire.
Ce résultat local n’est pas un benchmark ni une extrapolation à un cluster.
Silver NYC n’a pas été exécuté sous Windows natif ; la limite Hadoop précédemment
observée reste documentée.

## Gold NYC : montants enregistrés

Gold NYC lit directement le dossier Silver `trips_clean`. L’entrée rapportée pour
janvier 2025 contient 3 475 080 lignes conservées. L’inspection effectuée avant
l’implémentation confirme `tpep_pickup_datetime` en `timestamp_ntz`,
`total_amount_decimal` en `decimal(18,2)`, les libellés `pickup_borough`,
`pickup_zone`, `pickup_service_zone`, `payment_label` et le tableau `quality_flags`.
Les observations Silver fournies restent distinctes des contrôles Gold exécutés.

La table `daily_zone_payment` regroupe par date civile du pickup, `PULocationID`
et `payment_type` brut. Aucune conversion UTC n’est appliquée. Les clés NULL sont
conservées, ainsi que tous les doublons, flags, montants négatifs et montants zéro.
Les libellés distincts sont contrôlés pour chaque clé avant une jointure null-safe ;
une incohérence, y compris entre NULL et un libellé présent, est refusée.
Aucun libellé n’est choisi avec `first()`.
Les seuls appels `first()` du code métier lisent des totaux globaux d’une ligne.

Mesures : `trip_count`, `positive_amount_total`, `negative_amount_total` avec son
signe négatif, `net_amount_total`, `zero_amount_trip_count` et `flagged_trip_count`.
Une ligne avec plusieurs flags compte une seule fois dans `flagged_trip_count`.
Les montants proviennent exclusivement de `total_amount_decimal` ; les branches
positives et négatives utilisent des zéros décimaux explicites. Les sommes par groupe
sont `decimal(28,2)` et les contrôles globaux restent décimaux, sans calcul financier
en double. Un montant normalisé NULL est refusé pour empêcher une somme incomplète.

Ces résultats sont des **montants enregistrés**, pas des revenus certifiés, des
remboursements ou une qualification de fraude. USD est une métadonnée du dataset
TLC dans le rapport, pas une colonne source ni une devise inférée depuis chaque ligne.
Voir les [tarifs TLC](https://www.nyc.gov/site/tlc/passengers/taxi-fare.page) et le
[dictionnaire Yellow Taxi](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

Les contrôles vérifient le compte de trajets et le montant net par rapport à Silver,
`positif + négatif = net` pour chaque groupe, et les bornes des comptes zéro/flagged
entre 0 et `trip_count`. Les clés doivent être uniques. Les exceptions sont explicites.
La relecture contrôle aussi le schéma et le contenu par comparaison `exceptAll`.
Le rapport JSON est écrit après la relecture ; tous les montants y sont des chaînes
décimales, avec volumes, contrôles et configuration Spark effective.

Le Parquet est partitionné par `year/month` dérivés de `pickup_date` : ce choix
permet de sélectionner les mois utiles sur une entrée couvrant plusieurs mois ou
années, tout en gardant la granularité journalière dans la table. Une entrée vide
produit un Parquet vide lisible et un rapport avec comptes zéro et montants `0.00`.
Les types de partition sont explicités à la relecture pour les entrées dont toutes
les dates sont NULL ; ces groupes restent conservés.

La sortie non vide est refusée sans `--overwrite`, ainsi que tout chevauchement
entrée/sortie. Le remplacement concerne seulement la table et le rapport. Les
écritures ne sont pas atomiques ; un échec peut laisser une sortie partielle sans
nouveau rapport de succès. L’ancien rapport est supprimé avant un remplacement.

### Gold NYC : commandes et résultats exécutés

Commandes exécutées sous WSL, dans l’environnement Linux existant :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow
source ~/.venvs/transactionflow/bin/activate
python artifacts/nyc-gold-validation/inspect_source.py
python -m pytest -q --basetemp artifacts/nyc-gold-validation/pytest-temp
python run_nyc_gold.py --silver data/silver/nyc_2025_01_run1/trips_clean --output data/gold/nyc_2025_01_run1
python artifacts/nyc-gold-validation/read_back.py
```

Les scripts sous `artifacts/` sont des vérifications locales exclues de Git.
La suite finale donne **80 tests réussis en 305,48 s** : les 62 tests existants
et 18 tests Gold NYC autonomes. Une ambiguïté Spark entre les deux branches issues
du même DataFrame a été corrigée en qualifiant les colonnes des alias `daily` et
`labels`, avant cette vérification finale.

La CLI a créé une nouvelle sortie sans `--overwrite`. Le rapport puis la relecture
indépendante confirment les **montants enregistrés**, exprimés en USD :

| Contrôle | Résultat |
|---|---:|
| Trajets Silver représentés | 3 475 080 |
| Groupes journaliers zone/paiement | 22 979 |
| Montants positifs | 90 658 795,44 |
| Montants négatifs, signe conservé | −1 657 489,57 |
| Montant net | 89 001 305,87 |
| Trajets à montant normalisé zéro | 550 |
| Trajets avec au moins un flag | 149 692 |

Les trois totaux monétaires du rapport sont respectivement les chaînes
`"90658795.44"`, `"-1657489.57"` et `"89001305.87"`. La relecture a confirmé les
types `decimal(28,2)`, les comptes `long`, la date civile `date`, les clés uniques,
le contenu des agrégats, les bornes des compteurs et les identités monétaires.
La partition observée est `year=2025/month=1`.
Rapport : `data/gold/nyc_2025_01_run1/quality_report.json`.

Les 465 fichiers préexistants contrôlés par SHA-256 sont inchangés : Silver NYC,
pipeline synthétique, anciens tests, benchmarks, données et sorties existantes.
Gold NYC n’a pas été exécuté sous Windows natif. Les limites restantes sont les
écritures non atomiques, la capacité des décimaux et le coût des contrôles distribués.
La mémoire totale Python + JVM n’a pas été mesurée ; aucun nouveau benchmark n’a
été réalisé et aucune performance cluster n’est déduite de cette exécution locale.

## Résultats vérifiés

### Contrôle manuel demo

Observations manuelles fournies sous WSL pour la version avant la revue de
maintenabilité : **18 tests réussis en 39,67 s**. La commande exécutée était :

```bash
python run_silver.py --input data/raw/demo --output data/silver/demo_wsl
```

Le rapport indique **10 004 entrées, 3 rejets, 1 doublon exact supprimé, 0 conflit et
10 000 transactions finales**. La relecture a confirmé 10 000 transactions propres,
10 000 identifiants distincts, 3 rejets et les partitions `year/month`.
Ces sorties existantes ne sont pas régénérées par les tests.

### Tests automatisés

La suite contrôle les règles, les références, les conflits, les doublons, les compteurs,
les montants, les partitions, la relecture des rejets vides avec leur schéma et la
reproductibilité logique de deux exécutions.

Après la revue de maintenabilité, la suite exécutée sous WSL Ubuntu dans une copie
des sources sans `data/` a donné **18 tests réussis en 43,86 s**. Après activation
de `~/.venvs/transactionflow`, la commande exécutée était :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow/artifacts/project-review/tested-project
python -m pytest -q --basetemp pytest-temp
```

Une comparaison avant/après du générateur a également été exécutée pour 1, 24 et
10 000 transactions de base : les quatre fichiers produits sont identiques octet
par octet dans chaque configuration. Les empreintes des 25 fichiers existants
de `data/raw` et `data/silver` sont restées identiques. Ces vérifications n’ont pas
réexécuté le CLI sur les sorties de démonstration existantes.

## Mesures manuelles locales

Les chiffres ci-dessous sont des **mesures manuelles fournies**, relevées le
12 octobre 2026. Les commandes chronométrées n’ont pas été exécutées pendant cette
mise à jour documentaire. Les valeurs sont consignées dans
[`benchmarks/results.csv`](benchmarks/results.csv).

Les données sont synthétiques, avec **seed 42, 1 000 clients, 100 commerçants**,
devise EUR et une période de janvier à juin 2025. Les volumes désignent les
transactions valides de base ; les quatre anomalies du générateur s’ajoutent à
l’entrée Silver. Environnement déclaré : WSL Ubuntu, Python 3.13, Java 21,
PySpark 4.0.2, Spark local `local[2]`. Le projet, les données et les sorties sont
sous `/mnt/c/Users/omarb/Projects/TransactionFlow`.

Le protocole comporte **trois exécutions par volume et par phase**. Les temps de
l’exécution 1 ont été fournis manuellement en complément de l’extrait des exécutions
2 et 3. Les quatre séries contiennent maintenant **trois mesures** ; le CSV indique
`planned_runs=3` et `provided_runs=3`.

Le chronométrage `/usr/bin/time -f 'ELAPSED_SECONDS=%e'` mesure le **temps total de
la CLI**, incluant le démarrage Spark, les contrôles, les écritures et l’arrêt du
processus. La génération des données ne fait pas partie de ce temps.

| Transactions de base | Phase | Exécution 1 (s) | Exécution 2 (s) | Exécution 3 (s) | Médiane (s) | Minimum (s) | Maximum (s) |
|---:|---|---:|---:|---:|---:|---:|---:|
| 100 000 | Silver | 28,44 | 27,97 | 36,40 | 28,44 | 27,97 | 36,40 |
| 100 000 | Gold | 30,51 | 35,39 | 35,26 | 35,26 | 30,51 | 35,39 |
| 1 000 000 | Silver | 54,27 | 61,89 | 72,88 | 61,89 | 54,27 | 72,88 |
| 1 000 000 | Gold | 38,26 | 36,57 | 41,93 | 38,26 | 36,57 | 41,93 |

Pour trois mesures, la médiane est la valeur centrale après tri. Les temps bruts
et les statistiques sont exprimés en secondes, avec deux décimales.

Les rapports fournis sont identiques entre les exécutions **2 et 3** d’un même
volume, pour les compteurs Silver et les contrôles de réconciliation Gold :

| Transactions de base | Entrées Silver | Rejets | Doublons supprimés | Conflits | Transactions finales | Lignes commerçants Gold | Lignes clients Gold | Total EUR par table Gold (centimes) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 000 | 100 004 | 3 | 1 | 0 | 100 000 | 600 | 6 000 | 5 002 817 912 |
| 1 000 000 | 1 000 004 | 3 | 1 | 0 | 1 000 000 | 600 | 6 000 | 49 943 269 884 |

Chaque table Gold retrouve le compte de transactions Silver de son volume et le
même total en centimes EUR. Les comptes après jointure sont également conservés.
L’identité des rapports sur les trois exécutions ne peut pas être vérifiée à partir
de cet extrait, qui ne contient pas les rapports de l’exécution 1.

La **mémoire totale Python + JVM n’a pas été mesurée**. Ces résultats sont locaux,
avec stockage sous `/mnt/c` ; ils ne permettent aucune extrapolation à un cluster.
Aucun pipeline n’a été relancé, aucune règle n’a été modifiée, et aucune donnée
ou sortie existante n’a été remplacée pour cette documentation.

## Mesures automatiques des CLI NYC

`measure_cli.py` lance une CLI avec le Python de l’environnement actif et utilise
`time.perf_counter()`, une horloge monotone, pour mesurer sa durée totale. Le
chronométrage commence juste avant le lancement du processus et finit quand sa
sortie est observée : démarrage Spark, traitements, contrôles, écritures, relectures
et arrêt sont inclus. La collecte des versions et le calcul de taille sont hors
de cette durée. Le suivi ajoute un léger coût d’observation.

Toutes les 200 ms demandées, psutil additionne les RSS du Python lancé et de ses
descendants, dont la JVM. Le pic enregistré est le maximum des **sommes par
échantillon**, pas la somme des pics individuels. Les processus sont identifiés
par PID et date de création, et comptés une fois par échantillon. Les descendants
déjà observés restent suivis pendant la vie de la CLI. Le processus de mesure
lui-même est exclu. Les processus très courts peuvent échapper au suivi.

Cette somme RSS peut compter les pages partagées plusieurs fois et manquer un
pic entre deux échantillons. Les lectures des processus ne sont pas simultanées.
**Ce n’est pas une mesure exacte de mémoire physique unique.** Le CSV conserve
le nombre d’échantillons, le plus grand écart observé entre eux et la présence
de la JVM ; la cadence effective peut dépasser 200 ms. Une erreur d’accès RSS
arrête la série plutôt que de valider une mesure incomplète.

La taille logique est la somme des tailles des fichiers présents dans le dossier
de sortie après la CLI : Parquet, rapport JSON, `_SUCCESS` et fichiers annexes
éventuels. Pour cette série, elle exclut les journaux de mesure et le CSV, placés ailleurs. Elle
n’est ni l’espace physique alloué sur disque ni le volume des fichiers temporaires
Spark. Les liens symboliques dans les sorties mesurées sont refusés.

`run_nyc_measurements.py` exécute Silver 1, Gold 1, Silver 2, Gold 2, Silver 3,
Gold 3, sans parallélisme. Gold lit le Silver de la même paire. Tous les dossiers,
journaux et le CSV doivent être nouveaux ; aucune option d’écrasement n’est
transmise aux pipelines. Chaque rapport est contrôlé avant de poursuivre :
contrôles déclarés réussis, compteurs de relecture et réconciliation, montants
Gold en chaînes décimales, mêmes compteurs et totaux entre les trois exécutions
d’une phase. Les pipelines effectuent eux-mêmes leurs vérifications Parquet.
Le premier code de sortie non nul ou contrôle invalide arrête la série ; les
mesures déjà écrites et les sorties partielles sont conservées pour diagnostic.

Commandes exécutées sous WSL Ubuntu, dans `~/.venvs/transactionflow` :

```bash
cd /mnt/c/Users/omarb/Projects/TransactionFlow
source ~/.venvs/transactionflow/bin/activate
python -m pip install psutil
python -m pytest -q tests/test_measure_cli.py
python run_nyc_measurements.py --tag nyc_rss_20261012 --csv benchmarks/nyc_cli_measurements_20261012.csv
```

L’installation locale a donné psutil 7.2.2, version maintenant fixée dans
`requirements.txt`. Les tests ciblés de l’outil ont donné **9 passed in 1.44s**.
Les règles métier et les tests existants n’ont pas été modifiés. Pour une autre
série, choisir un nouveau tag et un nouveau chemin CSV. Le mesureur peut aussi
lancer une CLI Python individuellement ; son aide décrit `--output`, `--csv`,
`--log`, puis le script et ses arguments après `--`.

Le stockage des sources, des sorties et des journaux est sous `/mnt/c`.
Les caches OS **ne sont pas vidés ni contrôlés** ; aucune exécution n’est qualifiée
de démarrage à cache froid. Trois répétitions locales sur ce stockage ne permettent
aucune extrapolation à un cluster ; les autres activités de la machine ne sont
pas contrôlées. Les anciennes séries synthétiques restent des
mesures manuelles fournies, distinctes de cette instrumentation Python + JVM.
Les écritures des pipelines restent non atomiques.

Versions observées dans cette série : Python 3.13.16, PySpark 4.0.2, psutil 7.2.2,
OpenJDK 21.0.12.1, WSL2 Linux `6.18.33.2-microsoft-standard-WSL2` x86_64.
La configuration effective consignée par les rapports est Spark `local[2]`,
deux partitions de shuffle, AQE et mode ANSI activés, fuseau
`America/New_York`, `spark.sql.timestampType=TIMESTAMP_NTZ`.
Silver consigne aussi `spark.sql.parquet.inferTimestampNTZ.enabled=true`.
Le CSV conserve ces configurations par exécution dans `spark_json`, ainsi que
les versions, les commandes exactes, les chemins, les codes de sortie et
les résumés des contrôles de qualité. Il ne prétend pas inventorier toutes les
options internes de Spark.

### Résultats exécutés sous WSL le 12 octobre 2026

Ces chiffres proviennent des **six CLI exécutées avec l’outil de mesure**,
sur le fichier NYC existant de janvier 2025. Les données brutes sont dans
[`benchmarks/nyc_cli_measurements_20261012.csv`](benchmarks/nyc_cli_measurements_20261012.csv).
Les six codes de sortie sont 0, les six rapports sont validés, la JVM a été
observée à chaque fois et aucune erreur d’accès RSS n’a été détectée.

| Phase | Exécution | Durée totale (s) | Pic RSS échantillonné (octets) | Taille logique de sortie (octets) |
|---|---:|---:|---:|---:|
| Silver | 1 | 152,194222 | 1 917 190 144 | 93 577 604 |
| Gold | 1 | 44,700184 | 1 341 427 712 | 431 150 |
| Silver | 2 | 188,078677 | 1 910 042 624 | 93 679 428 |
| Gold | 2 | 38,279003 | 1 414 094 848 | 429 997 |
| Silver | 3 | 187,506790 | 1 926 762 496 | 93 662 841 |
| Gold | 3 | 53,499846 | 1 406 693 376 | 432 270 |

Statistiques sur les **trois mesures de chaque phase**, avec arrondi à deux
décimales pour les secondes et les Mio ; 1 Mio = 1 048 576 octets.
La médiane est la valeur centrale après tri, calculée séparément pour chaque
mesure : les médianes de durée et de mémoire peuvent provenir de runs différents.

| Phase | Mesure | Médiane | Minimum | Maximum |
|---|---|---:|---:|---:|
| Silver | Durée totale (s) | 187,51 | 152,19 | 188,08 |
| Silver | Pic RSS échantillonné (Mio) | 1 828,38 | 1 821,56 | 1 837,50 |
| Silver | Taille logique de sortie (octets) | 93 662 841 | 93 577 604 | 93 679 428 |
| Gold | Durée totale (s) | 44,70 | 38,28 | 53,50 |
| Gold | Pic RSS échantillonné (Mio) | 1 341,53 | 1 279,29 | 1 348,59 |
| Gold | Taille logique de sortie (octets) | 431 150 | 429 997 | 432 270 |

Les sorties sont `data/silver/nyc_rss_20261012_run1` à `run3` et
`data/gold/nyc_rss_20261012_run1` à `run3`. Les journaux sont dans
`artifacts/cli-measurements/nyc_rss_20261012/`. Le plus grand écart entre
échantillons observé dans la série est de **219,658 ms** pour une cadence demandée
de 200 ms. Les tailles logiques ont été recalculées après la série et correspondent
aux valeurs du CSV. Les rapports et configurations consignées ont aussi été relus.

Résultats de qualité identiques pour les trois exécutions de chaque phase :

- Silver : 3 475 226 entrées, 3 475 080 conservées, 146 en quarantaine
  (124 dates de dépose antérieures au départ, 22 départs hors janvier).
  Les compteurs par flag sont également identiques.
- Gold : 22 979 groupes, 3 475 080 trajets réconciliés, 550 trajets de montant
  zéro et 149 692 trajets avec au moins un flag.
- Montants enregistrés Gold, en USD selon la métadonnée TLC : positif
  `90658795.44`, négatif `-1657489.57`, net `89001305.87`.
  Le net correspond à la somme Silver et positif + négatif = net.

Le contrôle SHA-256 des **474 fichiers préexistants suivis** confirme que les
sources métier, les tests existants, les données, les anciennes sorties et
`benchmarks/results.csv` sont inchangés. Le README et les dépendances sont les
fichiers existants mis à jour. La suite Spark complète n’a pas été relancée pour
cet ajout ; les neuf tests ciblés du mesureur ont été exécutés.

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

