# What is this?

I often reach for sqlite (`rusqlite` specifically) when building little
proofs-of-concept in Rust.

I also often reach for tokio and a web server of some kind (usually `axum` for
http or `tonic` for gRPC).

It is "easy" to use these two things together, but the path of least resistance
is to do blocking IO on the executor thread, which is an antipattern.

This repo is a template for my own benefit, but maybe someone will see this and
be able to use it too. It spawns a single background thread (the "background
database", hence `bgdb`) and delegates all IO to that thread. The web server
itself runs on a different thread and communicates with the database exclusively
via message passing over a `tokio::sync::mpsc` channel (and receieves responses
by sending a `tokio::sync::oneshot` channel with each request).