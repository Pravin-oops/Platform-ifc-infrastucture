"""IFC trigger connector utilities: ECS-hosted producer for the BSP Trigger Backbone.

Layout mirrors ``produce_app``: executable entry points live in ``scripts/`` and
everything they import - modules, YAML configs, Avro/JSON schemas and
``requirements.txt`` - lives flat in this package.

Nothing is re-exported here on purpose. Importers name the module they need
(``from ifc_trigger_connector.utility.kafka_publisher import Publisher``), which
keeps the import graph readable and avoids pulling boto3 and confluent-kafka
into a process that only wanted the failure catalogue.
"""

__version__ = "0.1.0"
