# Pipeline synthétique

[← README](../README.md) · [Installation](setup.md) · [Mesures](benchmarks.md)

Ce parcours travaille sur des transactions générées, pas sur des paiements réels.
Toutes les commandes ci-dessous se lancent depuis la racine du dépôt.

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
