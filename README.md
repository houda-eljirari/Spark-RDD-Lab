# Big Data – Programmation RDD avec Apache Spark

### Exercice 1 – Analyse des ventes

À partir du fichier `ventes.txt`, deux traitements sont réalisés :

- Calcul du chiffre d'affaires total par ville.
- Calcul du chiffre d'affaires par ville et par année.

**Technologies :** Java, Apache Spark RDD, Maven.

### Exercice 2 – Analyse des logs Apache

À partir du fichier `access.log`, l'application réalise :

- Extraction des informations des requêtes HTTP.
- Calcul du nombre total de requêtes et du taux d'erreur.
- Identification des 5 adresses IP les plus actives.
- Identification des 5 ressources les plus demandées.
- Distribution des codes de réponse HTTP.

**Technologies :** Java, Apache Spark RDD, expressions régulières, Maven.

## Environnement technique

- Java 17
- Apache Spark 3.5.5
- Scala 2.12
- Maven Wrapper
- Windows / PowerShell

## Structure du projet

```text
Spark-RDD-Lab/
├── exercise1-sales/
│   ├── pom.xml
│   ├── ventes.txt
│   └── src/main/java/ma/enset/
│       ├── TotalSalesByCity.java
│       └── TotalSalesByCityAndYear.java
├── exercise2-logs/
│   ├── pom.xml
│   ├── access.log
│   └── src/main/java/ma/enset/
│       └── LogAnalysis.java
├── README.md
└── .gitignore
```

## Compilation et exécution

Les deux exercices sont des projets Maven indépendants.

### Exercice 1

Depuis le dossier `exercise1-sales` :

```powershell
.\mvnw.cmd clean compile
.\mvnw.cmd exec:java "-Dexec.mainClass=ma.enset.TotalSalesByCity"
.\mvnw.cmd exec:java "-Dexec.mainClass=ma.enset.TotalSalesByCityAndYear"
```

### Exercice 2

Depuis le dossier `exercise2-logs` :

```powershell
.\mvnw.cmd clean compile
.\mvnw.cmd exec:java "-Dexec.mainClass=ma.enset.LogAnalysis"
```

Pour l'exécution locale avec Java 17, si nécessaire, configurer les options JVM suivantes dans PowerShell :

```powershell
$env:MAVEN_OPTS = "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
```

## Résultats obtenus

### Exercice 1

- Casablanca : 10 230
- Rabat : 9 850
- Marrakech : 7 760

### Exercice 2

- Nombre total de requêtes : 5
- Erreurs HTTP : 2
- Taux d'erreur : 40 %
- Codes HTTP observés : 200, 302, 404 et 500.

