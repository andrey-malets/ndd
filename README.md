ndd
===

The idea of the project is quickly copy files between a group of machines over fast full-duplex network.
This utility does the same job as old unix `dd`, but over the network. All the machines are connected with
the pipeline and each machine receives the data from preceding and transmits it to the following in the
pipeline, saving the data to the local disk in the meantime. To avoid slowdowns caused by disk seeks etc.
We maintain large buffer in memory and perform all the operations asynchronously.

We try to keep the codebase small and perform task such as launch, error control and statistics externally.
