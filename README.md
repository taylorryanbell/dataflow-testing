# dataflow-testing
Testing Python / Dataflow integration. This program will be used to see how to integrate Google Cloud Dataflow with Python code.

### Input
Takes a Google Cloud Storage bucket folder with given file name prefix as an input

### Process
Reads all files with given prefix, compiles them into one PCollection, then uses a custom PTransform object to strip the specified header, everywhere it appears inside the compiled PCollection. Then writes a new file to the specified bucket folder with given name and suffix, using a single header line.