# Détection d'événements sociaux anormaux

Mini-projet 5 du module `Environnement Cloud pour le Big Data 2026`.

## Architecture

Le projet respecte l'architecture demandée:

`Flux synthétique de posts -> Producer Python -> Kafka -> Spark Structured Streaming -> PostgreSQL`

## Détection

- Fenêtres tumbling de `15s`
- Watermark de `30s`
- Filtrage par langue: `fr`
- Mot clé par défaut: `manifestation`
- Baseline dynamique: moyenne et écart-type des `6` fenêtres précédentes
- Seuil d'anomalie: `max(mean + 3*stddev, mean*2, 10)`

Le producteur simule un trafic normal multilingue puis injecte un pic artificiel de messages en français contenant le mot clé. Un bruit supplémentaire en anglais peut être injecté pendant le pic pour montrer que le filtrage par langue empêche un faux positif.

## Lancement

```bash
cd course_projects/social-event-anomaly-detection
docker compose up -d
docker compose logs -f producer spark
```

Le job Spark s'arrête après la détection du premier pic anormal ou après `180s`.

## Vérification

```bash
python3 -m unittest discover -s tests -v
./scripts/check_results.sh
```

Résultat attendu:

- au moins une ligne dans `window_metrics`
- au moins une ligne dans `anomalies`
- les anomalies concernent `keyword=manifestation` et `lang=fr`

## Nettoyage

```bash
docker compose down -v
```
