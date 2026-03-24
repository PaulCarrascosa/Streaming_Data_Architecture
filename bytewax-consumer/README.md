# Bytewax Kafka Consumer

Dataflow Bytewax qui consomme les messages depuis un topic Kafka, les traite et les produit dans un autre topic.

## Structure

```
bytewax-consumer/
├── app/
│   └── main.py          # Définition du dataflow Bytewax
├── pyproject.toml       # Configuration du projet
├── Dockerfile           # Image Docker pour exécuter le dataflow
└── .venv/              # Virtual environment (créé avec `uv venv`)
```

## Installation locale (Linux/Mac uniquement)

```bash
cd bytewax-consumer

# Créer un venv Python 3.13
uv venv --python 3.10

# Ajouter les dépendances
uv add bytewax[kafka] pillow

# Visualiser le dataflow
python -m bytewax.visualize app/main

# Démarrer le dataflow
python -m bytewax.run app/main
```

## Utilisation avec Docker

Le Dockerfile est configuré pour exécuter automatiquement le dataflow Bytewax :

```bash
# Depuis le répertoire racine du projet
docker-compose up -d bytewax-consumer
```

## Pipeline de traitement

Le dataflow effectue les opérations suivantes :

1. **Source** : Consomme les messages du topic `test-topic`
2. **Parse** : Décodi les messages JSON
3. **Enrich** : Ajoute des métadonnées (`processed: true`, `processor: bytewax`)
4. **Filter** : Filtre les messages contenant le champ `data`
5. **Format** : Encode la sortie en JSON
6. **Sink** : Produit les résultats dans le topic `test-topic-output`

## Configuration

Les variables d'environnement sont définies dans `docker-compose.yml` :

- `KAFKA_BROKERS` : Adresse des brokers Kafka
- `INPUT_TOPIC` : Topic Kafka à consommer
- `OUTPUT_TOPIC` : Topic Kafka pour la sortie

## Logs

Pour voir les logs du service :

```bash
docker logs bytewax-consumer -f
```

## Documentation

- [Bytewax Documentation](https://docs.bytewax.io/)
- [Kafka Connectors](https://docs.bytewax.io/stable/api/bytewax/bytewax.connectors.kafka.html)
