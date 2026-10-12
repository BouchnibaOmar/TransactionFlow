# Mesures locales et protocole

[← README](../README.md) · [Installation](setup.md) · [Synthétique](synthetic_pipeline.md) · [NYC](nyc_pipeline.md)

## Lire les graphiques

![NYC : trois durées par phase et médiane en secondes](assets/nyc_cli_times.svg)

Silver et Gold réalisent des travaux différents : ce graphique ne compare pas
deux moteurs. Les trois points sont les exécutions 1, 2 et 3 ; le trait pointillé
est la médiane. Les axes utilisent la même échelle en secondes.

![Synthétique : médiane et intervalle minimum–maximum par volume et phase](assets/synthetic_cli_times.svg)

Le losange est la médiane, le segment relie minimum et maximum. Aucun temps
supplémentaire n'a été inféré. Les tableaux ci-dessous restent la référence
numérique accessible et distinguent les deux provenances.

## Génération des visuels

Les SVG sont créés avec `render_charts.py` et la bibliothèque de graphiques
ReportLab 4.4.9, disponible dans le runtime documentaire utilisé. Ce script lit
les CSV existants, vérifie les statistiques et n'importe aucun module Spark.
ReportLab est un outil documentaire facultatif, absent des dépendances applicatives.
Aucune dépendance du projet n'a été changée pour générer les visuels.
Dans un environnement de documentation où ReportLab est déjà disponible :

```bash
python docs/render_charts.py
```

Il remplace uniquement les deux SVG de temps sous `docs/assets/`.
Les SVG embarquent leur texte et leurs formes : pas de JavaScript, animation,
iframe, police téléchargée ou service externe. Le diagramme Mermaid du README
nécessite un lecteur Markdown compatible ; ses règles sont aussi expliquées en prose.

## Portée commune

Trois exécutions par série, démarrage Spark inclus, Spark local sous WSL,
stockage des entrées et sorties sous `/mnt/c`. Les caches OS ne sont pas présentés
comme vidés et les autres activités de la machine ne sont pas contrôlées.
Ces mesures ne décrivent ni un cluster ni une installation de production.
La mémoire n'a pas été mesurée pour les séries synthétiques manuelles ; seule
la série NYC ultérieure dispose d'une somme RSS échantillonnée Python + JVM.

## Mesures manuelles locales

Les chiffres ci-dessous sont des **mesures manuelles fournies**, relevées le
12 octobre 2026. Les commandes chronométrées n’ont pas été exécutées pendant cette
mise à jour documentaire. Les valeurs sont consignées dans
[`benchmarks/results.csv`](../benchmarks/results.csv).

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
[`benchmarks/nyc_cli_measurements_20261012.csv`](../benchmarks/nyc_cli_measurements_20261012.csv).
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
`benchmarks/results.csv` sont inchangés. Lors de l’ajout du mesureur, le README
et `requirements.txt` avaient été actualisés. Cette réorganisation documentaire
ne change ni code ni dépendances. La suite Spark complète n’avait pas été relancée
pour l’ajout du mesureur ; ses neuf tests ciblés avaient été exécutés.
