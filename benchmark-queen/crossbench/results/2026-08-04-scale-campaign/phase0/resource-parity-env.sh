# Campagna scale 2026-08-04 — parita di risorse fra TUTTI i sistemi.
# Il compose di queen non ha deploy.limits; pgmq/kafka/rabbit li hanno con
# default 8 cpus / 16g. Su un box 32c/62G quello sarebbe un vantaggio 4x a
# Queen: qui ognuno riceve la macchina intera, come nella fair-matrix 08-02.
export CM_CPUS=32
export CM_MEM=56g
export KAFKA_HEAP_OPTS="-Xmx16g -Xms16g"
