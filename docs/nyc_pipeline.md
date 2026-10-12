# Pipeline NYC TLC

[← README](../README.md) · [Installation](setup.md) · [Mesures](benchmarks.md)

## Préparer les fichiers depuis un clone

Les fichiers de trajets réels et de zones ne sont pas versionnés. Depuis la
[page officielle TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
récupérer Yellow Taxi janvier 2025 et Taxi Zone Lookup Table, puis les placer sous
`data/raw/nyc/` avec les noms indiqués ci-dessous. Aucun téléchargement n'est
exécuté par les pipelines. Les empreintes des fichiers vérifiés sont conservées
plus bas et dans les rapports ; un autre fichier peut donner d'autres résultats.

Commandes de reprise proposées dans des sorties nouvelles (arguments vérifiés
par lecture des CLI, pas relancés pendant cette mise à jour) :

```bash
python run_nyc_silver.py --input data/raw/nyc/yellow_tripdata_2025-01.parquet --zones data/raw/nyc/taxi_zone_lookup.csv --month 2025-01 --output data/silver/nyc_quickstart
python run_nyc_gold.py --silver data/silver/nyc_quickstart/trips_clean --output data/gold/nyc_quickstart
```

## Interprétation et limites décimales

Les flags signalent une particularité des données, pas une fraude. Un montant
négatif ne prouve pas un remboursement. Les agrégats sont des **montants enregistrés**.
USD est une métadonnée TLC, pas une colonne monétaire de la source.

`decimal(18,2)` garde au plus 16 chiffres entiers ; `decimal(28,2)` au plus 26.
Les contrôles globaux sont bornés par la précision décimale Spark, jusqu'à 38 chiffres.
Une conversion hors capacité est mise en quarantaine en Silver ; une somme hors
capacité peut faire échouer Gold avec ANSI activé. Arrondir une source `double`
ne restitue pas un montant d'origine exact. Aucun résultat certifié n'en découle.
Les écritures non atomiques peuvent laisser des sorties partielles.

## Silver NYC

Sources officielles : [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
et [dictionnaire Yellow Taxi](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
Les fichiers locaux sont `data/raw/nyc/yellow_tripdata_2025-01.parquet` et
`data/raw/nyc/taxi_zone_lookup.csv`. La référence de zones est publiée sur la page TLC.
La TLC indique que les données sont fournies par les prestataires et ne garantit
pas leur exactitude. Silver et Gold NYC ont des règles distinctes du pipeline synthétique. Les mesures locales ultérieures sont dans [les benchmarks](benchmarks.md).

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
Lors de cette exécution initiale, la mémoire totale Python + JVM n’a pas été mesurée ; aucun nouveau benchmark n’a
été réalisé et aucune performance cluster n’est déduite de cette exécution locale.
