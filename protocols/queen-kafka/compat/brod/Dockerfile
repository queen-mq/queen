# The brod row of the M6 client matrix runs in a container because there is no
# Erlang on the host, and because "the official erlang image" is the most
# honest statement of what an Erlang shop actually has.
#
# The ONLY thing this adds to erlang:27 is cmake. That is not a nicety: brod
# pulls crc32cer, whose NIF is built by a Makefile that shells out to cmake, and
# erlang:27 ships gcc but not cmake, so a bare `rebar3 compile` dies with
#   Compiling NIF in c_build / make: cmake: No such file or directory
# before a single byte of Kafka protocol is exchanged. snappyer (the snappy
# codec) is built the same way.
FROM erlang:27
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*
