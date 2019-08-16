#!/bin/sh
echo "START injecting jsonld..."
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/beehive.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/beekeeper.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/door.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/observation_door.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/observation_sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/sensor.jsonld --header "Content-Type: application/ld+json"
curl -vX POST http://localhost:8080/ngsi-ld/v1/entities/neo4j -d @src/test/resources/data/smartdoor.jsonld --header "Content-Type: application/ld+json"
echo "END injection"