# Distributed-File-System

Distributed file storage system for the Distributed Systems and Networks module. It consists of a controller that orchestrates the store/retrieve operations, and N Data Stores that store the files.
The client is provided by the module team for making requests to the controller and testing the system.

Command line parameters to start up the system:
- Controller: java Controller cport R timeout rebalance_period
- A Dstore: java Dstore port cport timeout file_folder
- A client: java Client cport timeout
