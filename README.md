# 📊 Social Media Analytics Platform

Projet Big Data pour l’analyse des interactions sur les réseaux sociaux
et l’identification des influenceurs et communautés.

---

## 🎯 Objectifs
- Collecter des interactions en temps réel
- Nettoyer et enrichir les données
- Identifier les influenceurs
- Détecter les communautés
- Visualiser les résultats dans Power BI

---

## 🏗️ Architecture
Kafka → Spark → Delta Lake → Neo4j → Power BI

---

## 🛠️ Technologies utilisées
- Apache Kafka
- Apache Spark
- Delta Lake
- Neo4j (Graph Data Science)
- Power BI
- Python

  ## 📂 Structure du projet
producer/ # Kafka producer
spark/ # Spark jobs (Bronze → Silver)
neo4j/ # Graph analytics (PageRank, Louvain)
powerbi/ # Dashboard Power BI

---

## 🔍 Analyses réalisées
- Top 10 influenceurs (PageRank)
- Détection de communautés (Louvain)
- Évolution temporelle des interactions
- Répartition des actions (Like, Share, Comment, Follow)

---

## 📈 Dashboard
Le tableau de bord Power BI permet :
- Suivi des KPI
- Analyse des utilisateurs les plus actifs
- Visualisation des tendances temporelles

---

## 👩‍💻 Auteur
**Chaimae Mesbahi**  
Étudiante en Ingénierie des Systèmes Informatiques


