# COVID functions

This is a simple project I realized for a job interview.
It's an Azure function containing 3 HTTP-Triggered and 1 Time-Triggered APIs

## HTTP-Trigggered

The 3 HTTP-Triggered APIs read data from three different collections on a CosmosDB instance
and serve it (JSON)

## Time-Triggered

The Time-Triggered API updates the three collections on CosmosDB once a day
